"""
Async MeshCore Companion TCP client.

The plugin runs as its own process and reaches the mesh the same way a phone
app does: over the companion frame protocol on the repeater's companion port
(``identities.companions[].settings.tcp_port``). Nothing here touches repeater
internals, which is what lets this ship as a wheel instead of a fork.

Wire format (openhop_core.companion.constants):

    inbound  (plugin -> repeater)   0x3c | len(2, LE) | payload
    outbound (repeater -> plugin)   0x3e | len(2, LE) | payload

Payload byte 0 is a command code on the way in and a response/push code on the
way out. Codes >= 0x80 are unsolicited pushes; everything else answers the
command currently in flight, so pushes are routed to callbacks and responses to
a queue the command path awaits.

Only one TCP client per companion is allowed, so this connection is exclusive:
while the plugin holds it, a phone app cannot attach to the same companion.
Give the observer its own companion identity.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import struct
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

from openhop_core.companion.constants import (
    CMD_APP_START,
    CMD_GET_CHANNEL,
    CMD_GET_CONTACTS,
    CMD_SEND_CHANNEL_TXT_MSG,
    CMD_SET_CHANNEL,
    CMD_SET_DEVICE_TIME,
    CMD_SET_PATH_HASH_MODE,
    CMD_SYNC_NEXT_MESSAGE,
    FRAME_INBOUND_PREFIX,
    FRAME_OUTBOUND_PREFIX,
    PUSH_CODE_ADVERT,
    PUSH_CODE_LOG_RX_DATA,
    PUSH_CODE_MSG_WAITING,
    PUSH_CODE_NEW_ADVERT,
    RESP_CODE_CHANNEL_DATA_RECV,
    RESP_CODE_CHANNEL_INFO,
    RESP_CODE_CHANNEL_MSG_RECV,
    RESP_CODE_CHANNEL_MSG_RECV_V3,
    RESP_CODE_CONTACT,
    RESP_CODE_CONTACT_MSG_RECV,
    RESP_CODE_CONTACT_MSG_RECV_V3,
    RESP_CODE_CONTACTS_START,
    RESP_CODE_END_OF_CONTACTS,
    RESP_CODE_NO_MORE_MESSAGES,
    RESP_CODE_OK,
)

logger = logging.getLogger("txmesh.companion")

# Pushes carry no reply; they must never be handed to the command path.
_PUSH_FLOOR = 0x80

# Companion app protocol version we speak. 3 is the first that carries SNR on
# message frames.
APP_TARGET_VER = 3

# On-air text budget in BYTES. §6.2's 160 is characters; slicing the UTF-8
# encoding at a byte boundary can cut inside a codepoint, which the server
# then decodes with errors="replace" and puts U+FFFD on air (audit L3).
MAX_TEXT_BYTES = 160


def utf8_truncate(text: str, limit: int) -> bytes:
    """Encode and clamp to ``limit`` bytes without splitting a codepoint."""
    body = text.encode("utf-8")
    if len(body) <= limit:
        return body
    cut = limit
    # Back up over continuation bytes (10xxxxxx) to the start of a codepoint.
    while cut > 0 and (body[cut] & 0xC0) == 0x80:
        cut -= 1
    return body[:cut]


class CompanionProtocolError(RuntimeError):
    """Raised when a frame violates the protocol's invariants."""


@dataclass
class Contact:
    """One entry from the companion's contact table."""

    public_key: bytes
    name: str
    type: int
    last_advert: int
    latitude: float = 0.0
    longitude: float = 0.0


@dataclass
class Message:
    """A decoded inbound message. ``channel`` is None for a DM."""

    text: str
    timestamp: int
    snr: float
    path_len: int
    sender_prefix: bytes = b""
    channel_idx: Optional[int] = None


@dataclass
class Advert:
    """An advert as heard here, with its ingress path."""

    public_key: bytes
    name: str = ""
    type: int = 0
    timestamp: int = 0
    snr: float = 0.0
    path: bytes = b""
    raw: bytes = b""
    extra: Dict[str, Any] = field(default_factory=dict)


class CompanionClient:
    """One exclusive TCP session with a companion identity.

    Callbacks are awaited in the reader's own task, so a slow handler delays
    further frames from this companion. Keep them cheap; anything expensive
    belongs on a queue.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._responses: asyncio.Queue = asyncio.Queue()
        self._command_lock = asyncio.Lock()
        self._msg_waiting = asyncio.Event()
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()

        self.on_connected: Optional[Callable[[], Awaitable[None]]] = None
        self.on_message: Optional[Callable[[Message], Awaitable[None]]] = None
        self.on_advert: Optional[Callable[[Advert], Awaitable[None]]] = None
        self.on_raw_frame: Optional[Callable[[bytes, float, int], Awaitable[None]]] = None

    # ----------------------------------------------------------------
    # Connection lifecycle
    # ----------------------------------------------------------------

    def is_connected(self) -> bool:
        return self._connected.is_set()

    async def run(self) -> None:
        """Connect and service the session, reconnecting until stopped.

        Backoff is capped rather than unbounded: the companion is on localhost,
        so a failure here is the repeater restarting, not a flaky network.
        """
        attempt = 0
        while not self._stop.is_set():
            try:
                await self._session()
                attempt = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                delay = min(2 ** min(attempt, 5), 30)
                self._connected.clear()
                logger.warning("Companion connection lost (%s); retry in %ss", exc, delay)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)

    async def stop(self) -> None:
        self._stop.set()
        await self._teardown()

    async def _session(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        self._responses = asyncio.Queue()
        self._msg_waiting.clear()
        self._reader_task = asyncio.create_task(self._reader_loop(), name="txmesh-reader")
        self._reader_task.add_done_callback(
            lambda _t: asyncio.ensure_future(self._reader_died())
        )

        # APP_START identifies us to the companion and starts the session. Byte
        # 0 is app_target_ver: at 0 the server emits only the pre-v3 message
        # frames, which carry no SNR, so every mirrored message published a
        # fabricated snr=0.0 (audit M5). >=3 selects the *_V3 frames.
        #
        # A handshake failure must tear down too, or the reader task and
        # writer leak per attempt and the stale reader keeps feeding the NEXT
        # session's response queue (audit L1).
        try:
            async with self._command_lock:
                await self._command(bytes([CMD_APP_START, APP_TARGET_VER]) + b"\x00" * 6)
        except BaseException:
            await self._teardown()
            raise

        self._connected.set()
        logger.info("Connected to companion %s:%s", self.host, self.port)

        # Fired once per session, after the handshake: the right moment to
        # push configured channels and warm caches, since state set here is
        # persisted by the repeater and survives our reconnects.
        if self.on_connected:
            try:
                await self.on_connected()
            except Exception as exc:
                logger.error("on_connected failed: %s", exc, exc_info=True)

        try:
            # Drain once on connect: messages queued while we were away are
            # still waiting, and no MSG_WAITING push will be re-sent for them.
            await self.drain_messages()
            while not self._stop.is_set():
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._msg_waiting.wait(), timeout=2.0)
                self._msg_waiting.clear()
                await self.drain_messages()

                if self._reader_task.done():
                    exc = self._reader_task.exception()
                    raise exc or ConnectionError("reader loop exited")
        finally:
            self._connected.clear()
            await self._teardown()

    async def _teardown(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._reader_task
            self._reader_task = None
        if self._writer is not None:
            with contextlib.suppress(Exception):
                self._writer.close()
                await self._writer.wait_closed()
            self._writer = None
        self._reader = None

    # ----------------------------------------------------------------
    # Framing
    # ----------------------------------------------------------------

    async def _reader_loop(self) -> None:
        reader = self._reader
        if reader is None:
            raise ConnectionError("reader not initialised")

        while True:
            prefix = await reader.readexactly(1)
            if prefix[0] != FRAME_OUTBOUND_PREFIX:
                raise CompanionProtocolError(f"unexpected frame prefix {prefix[0]:#x}")
            length = struct.unpack("<H", await reader.readexactly(2))[0]
            payload = await reader.readexactly(length) if length else b""
            if not payload:
                continue

            code = payload[0]
            if code >= _PUSH_FLOOR:
                await self._handle_push(code, payload)
            else:
                await self._responses.put(payload)

    async def _reader_died(self) -> None:
        """Wake any command waiting on a reply that will never come.

        Without this a socket that drops mid-on_connected left every remaining
        set_channel/get_channel to burn its full timeout in turn -- ~7 minutes
        for 40 slots -- with _connected still set (second audit #7).
        """
        await self._responses.put(None)

    async def _write(self, payload: bytes) -> None:
        writer = self._writer
        if writer is None:
            raise ConnectionError("socket not open")
        writer.write(bytes([FRAME_INBOUND_PREFIX]) + struct.pack("<H", len(payload)) + payload)
        await writer.drain()

    async def _command(
        self,
        payload: bytes,
        *,
        expected: Optional[set] = None,
        timeout: float = 10.0,
        max_unexpected: int = 4,
    ) -> bytes:
        """Send one command and return its response frame.

        Caller must hold ``_command_lock``: the protocol has no request ids, so
        two commands in flight would race for each other's responses.

        The lock alone is not enough. A command that times out still leaves its
        response in flight, and that stale frame is then sitting in the queue
        when the *next* command reads it -- which silently returned an empty
        contact walk. When ``expected`` is given, mismatching frames are
        discarded (bounded) rather than mistaken for this command's reply.
        """
        await self._write(payload)
        for _ in range(max_unexpected + 1):
            frame = await asyncio.wait_for(self._responses.get(), timeout=timeout)
            if frame is None:
                # Re-arm: the sentinel is consumed by get(), so without this
                # only the FIRST waiter failed fast and every later command
                # still burned its full timeout (third audit F1).
                self._responses.put_nowait(None)
                raise ConnectionError("companion connection lost")
            if expected is None or frame[0] in expected:
                return frame
            logger.debug("discarding stale response %#x", frame[0] if frame else -1)
        raise asyncio.TimeoutError("no expected response")

    # ----------------------------------------------------------------
    # Pushes
    # ----------------------------------------------------------------

    async def _handle_push(self, code: int, payload: bytes) -> None:
        try:
            if code == PUSH_CODE_MSG_WAITING:
                # Only a doorbell: the messages are fetched with SYNC_NEXT.
                self._msg_waiting.set()
            elif code in (PUSH_CODE_ADVERT, PUSH_CODE_NEW_ADVERT) and self.on_advert:
                advert = self._parse_advert(payload)
                if advert is not None:
                    await self.on_advert(advert)
            elif code == PUSH_CODE_LOG_RX_DATA and self.on_raw_frame:
                # snr(int8, x4) | rssi(int8) | raw frame
                if len(payload) >= 3:
                    snr = struct.unpack("b", payload[1:2])[0] / 4.0
                    rssi = struct.unpack("b", payload[2:3])[0]
                    await self.on_raw_frame(payload[3:], snr, rssi)
        except Exception as exc:
            logger.error("push %#x handler failed: %s", code, exc, exc_info=True)

    def _parse_advert(self, payload: bytes) -> Optional[Advert]:
        """Parse an advert push.

        Layout varies across firmware revisions, so this reads the fields it is
        sure of and leaves the rest empty rather than guessing: a wrong offset
        would publish plausible-looking nonsense to the collector.
        """
        if len(payload) < 33:
            return None
        return Advert(public_key=payload[1:33], raw=payload[1:])

    # ----------------------------------------------------------------
    # Commands
    # ----------------------------------------------------------------

    async def drain_messages(self) -> None:
        """Pull every queued message until the companion says there are none.

        SYNC_NEXT_MESSAGE pops one message per call, so this must loop; a
        single call per doorbell would fall permanently behind on a busy mesh.
        """
        while not self._stop.is_set():
            async with self._command_lock:
                try:
                    frame = await self._command(
                        bytes([CMD_SYNC_NEXT_MESSAGE]),
                        expected={
                            RESP_CODE_NO_MORE_MESSAGES,
                            RESP_CODE_CONTACT_MSG_RECV,
                            RESP_CODE_CONTACT_MSG_RECV_V3,
                            RESP_CODE_CHANNEL_MSG_RECV,
                            RESP_CODE_CHANNEL_MSG_RECV_V3,
                            # Binary channel data is queued alongside text. Not
                            # listing it meant every such packet heard on air --
                            # trivially sent on Public -- was discarded and then
                            # waited out a 10s timeout holding the command lock,
                            # stalling the bridge, the roster and the mirror
                            # (audit M2). It is consumed here and ignored.
                            RESP_CODE_CHANNEL_DATA_RECV,
                        },
                    )
                except asyncio.TimeoutError:
                    return

            code = frame[0]
            if code == RESP_CODE_NO_MORE_MESSAGES:
                return

            message = self._parse_message(frame)
            if message is not None and self.on_message:
                try:
                    await self.on_message(message)
                except Exception as exc:
                    logger.error("message handler failed: %s", exc, exc_info=True)

    def _parse_message(self, frame: bytes) -> Optional[Message]:
        """Decode a sync response into a Message.

        Layouts are taken from openhop_core's own frame_server encoder rather
        than inferred -- three of four carry a txt_type byte that is easy to
        miss, and an off-by-one here yields a plausible-looking but wrong
        timestamp (which then poisons the contract's skew_s field).

        CHANNEL_MSG_RECV_V3  code snr 0 0 chan path_len txt_type ts(4) text
        CHANNEL_MSG_RECV     code chan path_len txt_type ts(4) text
        CONTACT_MSG_RECV_V3  code snr 0 0 prefix(6) path_len txt_type ts(4) text
        CONTACT_MSG_RECV     code prefix(6) path_len txt_type ts(4) text

        snr is transported as SNR x4 in a signed byte; the non-V3 forms carry
        no SNR at all, so it is reported as 0.0 there.
        """
        code = frame[0]

        def text_from(offset: int) -> str:
            return frame[offset:].decode("utf-8", errors="replace").rstrip("\x00")

        if code == RESP_CODE_CHANNEL_MSG_RECV_V3 and len(frame) >= 11:
            return Message(
                snr=struct.unpack("b", frame[1:2])[0] / 4.0,
                channel_idx=frame[4],
                path_len=frame[5],
                timestamp=struct.unpack("<I", frame[7:11])[0],
                text=text_from(11),
            )

        if code == RESP_CODE_CHANNEL_MSG_RECV and len(frame) >= 8:
            return Message(
                snr=0.0,
                channel_idx=frame[1],
                path_len=frame[2],
                timestamp=struct.unpack("<I", frame[4:8])[0],
                text=text_from(8),
            )

        if code == RESP_CODE_CONTACT_MSG_RECV_V3 and len(frame) >= 16:
            return Message(
                snr=struct.unpack("b", frame[1:2])[0] / 4.0,
                sender_prefix=frame[4:10],
                path_len=frame[10],
                timestamp=struct.unpack("<I", frame[12:16])[0],
                text=text_from(16),
            )

        if code == RESP_CODE_CONTACT_MSG_RECV and len(frame) >= 13:
            return Message(
                snr=0.0,
                sender_prefix=frame[1:7],
                path_len=frame[7],
                timestamp=struct.unpack("<I", frame[9:13])[0],
                text=text_from(13),
            )

        logger.debug("ignoring sync response code %#x", code)
        return None

    async def get_contacts(self) -> List[Contact]:
        """Walk the companion's contact table.

        Streams CONTACTS_START, then one CONTACT per entry, then
        END_OF_CONTACTS -- so unlike every other command this consumes many
        response frames for one request.
        """
        contacts: List[Contact] = []
        async with self._command_lock:
            try:
                frame = await self._command(
                    bytes([CMD_GET_CONTACTS]),
                    expected={RESP_CODE_CONTACTS_START, RESP_CODE_CONTACT},
                )
            except asyncio.TimeoutError:
                logger.warning("contact walk got no CONTACTS_START")
                return contacts
            if frame[0] == RESP_CODE_CONTACT:
                parsed = self._parse_contact(frame)
                if parsed:
                    contacts.append(parsed)

            while True:
                try:
                    frame = await asyncio.wait_for(self._responses.get(), timeout=10.0)
                except asyncio.TimeoutError:
                    break
                if frame is None:  # reader died mid-walk (third audit F2)
                    self._responses.put_nowait(None)
                    raise ConnectionError("companion connection lost")
                if frame[0] == RESP_CODE_END_OF_CONTACTS:
                    break
                if frame[0] == RESP_CODE_CONTACT:
                    parsed = self._parse_contact(frame)
                    if parsed:
                        contacts.append(parsed)
        return contacts

    def _parse_contact(self, frame: bytes) -> Optional[Contact]:
        # Verified against a live companion (openhop_repeater 0.3.x), body of 147:
        #   pubkey(32) type(1) flags(1) out_path_len(1) out_path(64)
        #   name(32)@99 last_advert(u32)@131 lat(i32)@135 lon(i32)@139
        # A trailing u32 at 143 is a separate "last modified" stamp, not used here.
        body = frame[1:]
        if len(body) < 143:
            return None
        name = body[99:131].split(b"\x00")[0].decode("utf-8", errors="replace")
        last_advert = struct.unpack("<I", body[131:135])[0]
        lat = struct.unpack("<i", body[135:139])[0] / 1e6
        lon = struct.unpack("<i", body[139:143])[0] / 1e6
        return Contact(
            public_key=body[0:32],
            type=body[32],
            name=name,
            last_advert=last_advert,
            latitude=lat,
            longitude=lon,
        )

    async def get_channel(self, idx: int) -> Optional[str]:
        """Return a channel slot's name, or None when the slot is empty."""
        async with self._command_lock:
            try:
                frame = await self._command(
                    bytes([CMD_GET_CHANNEL, idx]), expected={RESP_CODE_CHANNEL_INFO, 0x01}
                )
            except asyncio.TimeoutError:
                return None
        if not frame or frame[0] != RESP_CODE_CHANNEL_INFO or len(frame) < 3:
            return None
        return frame[2:34].split(b"\x00")[0].decode("utf-8", errors="replace")

    async def set_device_time(self, epoch: Optional[int] = None) -> bool:
        """Push the host's clock to this companion identity (CMD_SET_DEVICE_TIME).

        A companion identity has no RTC of its own -- like real companion
        hardware, it starts at whatever it last had (often unset/epoch 0) and
        stays there until a connecting client pushes real time, the same way
        the official MeshCore app syncs a phone's clock to a companion on
        every session. Without this, every message the identity composes
        carries a bogus timestamp forever, even though the host process this
        virtual companion runs in already knows the correct time.
        """
        secs = int(epoch if epoch is not None else time.time())
        payload = bytes([CMD_SET_DEVICE_TIME]) + struct.pack("<I", secs)
        async with self._command_lock:
            try:
                frame = await self._command(payload, expected={RESP_CODE_OK, 0x01})
            except asyncio.TimeoutError:
                return False
        return bool(frame) and frame[0] == RESP_CODE_OK

    async def set_path_hash_mode(self, hash_bytes: int) -> bool:
        """Set how many bytes each hop hash occupies in paths this node builds.

        Wire: CMD_SET_PATH_HASH_MODE | subtype(0) | mode, where mode 0/1/2 =
        1/2/3-byte hashes. The companion persists it in its prefs.
        """
        if hash_bytes not in (1, 2, 3):
            raise ValueError(f"path_hash_bytes must be 1, 2 or 3, got {hash_bytes!r}")
        payload = bytes([CMD_SET_PATH_HASH_MODE, 0, hash_bytes - 1])
        async with self._command_lock:
            try:
                frame = await self._command(payload, expected={RESP_CODE_OK, 0x01})
            except asyncio.TimeoutError:
                return False
        return bool(frame) and frame[0] == RESP_CODE_OK

    async def set_channel(self, idx: int, name: str, secret: bytes) -> bool:
        """Configure a channel slot on the companion. The repeater persists it.

        Wire: idx(1) | name(32, NUL-padded) | secret(16 raw). A 16-byte secret
        is the MeshCore channel-key size; the server also accepts 32 raw or 64
        hex, but 16 is what hashtag_secret() produces.
        """
        payload = bytes([CMD_SET_CHANNEL, idx & 0xFF]) + name.encode("utf-8")[:32].ljust(32, b"\x00") + secret
        async with self._command_lock:
            try:
                frame = await self._command(payload, expected={RESP_CODE_OK, 0x01})
            except asyncio.TimeoutError:
                return False
        return bool(frame) and frame[0] == RESP_CODE_OK

    async def send_channel_message(self, idx: int, text: str) -> bool:
        """Transmit a channel text message as this companion (§6)."""
        body = utf8_truncate(text, MAX_TEXT_BYTES)
        payload = bytes([CMD_SEND_CHANNEL_TXT_MSG, 0, idx]) + struct.pack("<I", 0) + body
        async with self._command_lock:
            try:
                frame = await self._command(payload)
            except asyncio.TimeoutError:
                logger.warning("channel send timed out")
                return False
        return bool(frame) and frame[0] != 0x01  # RESP_CODE_ERR
