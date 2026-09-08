"""
Bridges a companion session to the observer MQTT contract.

Owns the cadences from MQTT.md §5, the message mirror (§5.5) and the inbound
send bridge (§6). Payload shapes live in ``format.py``; the MQTT connection
lives in ``mqtt.py``; the mesh side is ``companion.py``.

Unlike an in-process integration this has no access to repeater internals, so
everything it publishes is derived from what the companion protocol exposes.
Where the protocol cannot supply a field, the field is omitted rather than
invented -- §4 requires optional keys to be absent, not null, and a plausible
wrong value is worse for a collector than a missing one.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import sys

try:
    import resource  # POSIX only; absent on Windows
except ImportError:  # pragma: no cover
    resource = None
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .companion import Advert, CompanionClient, Message
from .format import (
    CONTACTS_BATCH,
    CONTACTS_INTERVAL_S,
    CONTACTS_SLICE_MS,
    HEARD_INTERVAL_S,
    HEARD_LIMIT,
    SEND_QUEUE_DEPTH,
    SENSORS_INTERVAL_S,
    TELEMETRY_INTERVAL_S,
    RateLimiter,
    build_advert,
    build_channel_message,
    build_contact,
    build_dm,
    build_heard,
    build_packet,
    build_sensors,
    build_telemetry,
    clamp_text,
    decode_advert_frame,
    hashtag_secret,
    heard_at,
    most_recent,
    parse_send_topic,
    pk8,
    resolve_channel,
)
from .mqtt import ObserverMqttClient

logger = logging.getLogger("txmesh.publisher")

# heard/ publishes the 16 most recent (§5.4); the surplus is headroom for the
# roster fallback before the contact table has been walked.
_ADVERT_CACHE_MAX = 512


class _Slot:
    """A channel slot, shaped like the object resolve_channel() expects."""

    def __init__(self, name: str):
        self.name = name


class ObserverPublisher:
    """Mirrors one companion onto MQTT and services its send bridge."""

    def __init__(self, client: CompanionClient, config: Mapping[str, Any]):
        self.client = client
        self.config = config
        self.node_name = config.get("node_name") or "observer"

        self.advert_dump = bool(config.get("advert_dump", False))
        self.publish_packets = bool(config.get("packets", False))

        self.mqtt = ObserverMqttClient(config, self.node_name, on_send=self.on_mqtt_message)

        # Producer is the paho thread, consumer is the asyncio loop. A bounded
        # queue.Queue makes the depth-4 limit and the cross-thread handoff
        # explicit rather than relying on CPython list-op atomicity (audit INFO).
        self._send_queue: "queue.Queue[tuple]" = queue.Queue(maxsize=SEND_QUEUE_DEPTH)
        self._rate_limiter = RateLimiter()

        self._adverts: Dict[str, dict] = {}
        self._channels: Dict[int, _Slot] = {}
        self._contacts: List[dict] = []

        self._start_time = time.time()
        self._rx = 0

        # Telemetry sources for a host-side node. The spec's fields are
        # embedded concepts; here they are mapped onto the nearest real thing:
        #   heap  -> this process's resident memory, in bytes
        #   boots -> a persistent restart counter (state file)
        # and, at the operator's explicit request, a constant battery voltage
        # (no host equivalent exists; unset in the shipped defaults so a
        # catalogue install never emits a fabricated reading).
        self._battery_mv = _positive_int_or_none(config.get("battery_mv"), "battery_mv")
        self._boots = _bump_boot_counter(config.get("state_dir"))
        self._last_snr: Optional[float] = None
        self._last_rssi: Optional[int] = None

        self._stop = asyncio.Event()

        client.on_connected = self.on_companion_connected
        client.on_message = self.on_message
        client.on_advert = self.on_advert
        client.on_raw_frame = self.on_raw_frame

    # ----------------------------------------------------------------

    async def run(self) -> None:
        """Bring up MQTT and drive every cadence until stopped."""
        self.mqtt.start()
        await asyncio.gather(
            self._cadence(TELEMETRY_INTERVAL_S, self.tick_telemetry, "telemetry"),
            self._cadence(SENSORS_INTERVAL_S, self.tick_sensors, "sensors"),
            self._cadence(CONTACTS_INTERVAL_S, self.tick_contacts, "contacts"),
            self._cadence(HEARD_INTERVAL_S, self.tick_heard, "heard"),
            self._drain_sends(),
        )

    async def stop(self) -> None:
        self._stop.set()
        self.mqtt.stop()

    async def _cadence(self, interval: float, tick, label: str) -> None:
        """Run one periodic publisher. Each is independently guarded so a
        failing tick cannot stall the others.

        A skipped tick waits only briefly rather than the full interval: MQTT
        is still connecting at t=0, and charging that a whole period delayed
        the first contact roster by five minutes.
        """
        while not self._stop.is_set():
            ready = self.mqtt.is_connected() and self.client.is_connected()
            if ready:
                try:
                    await tick()
                except Exception as exc:
                    logger.error("%s tick failed: %s", label, exc, exc_info=True)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval if ready else 2.0)
            except asyncio.TimeoutError:
                pass

    # ----------------------------------------------------------------
    # Companion session
    # ----------------------------------------------------------------

    async def on_companion_connected(self) -> None:
        """Push configured channels, then warm the slot cache.

        Runs after the handshake, so the socket is actually open -- warming in
        run() raced the connection and left the cache empty until the first
        send happened to fill it, mirroring messages as channel "?" until then.

        Channels come from config so joining ``#bot`` is a config line rather
        than a hand-built frame. A ``#`` name needs no secret (hashtag key);
        anything else must supply ``secret`` as hex.
        """
        # Path hash width for packets this node originates. 2-byte hashes
        # (the default here) make hop paths unambiguous on a busy mesh, where
        # 1-byte hashes collide. Persisted by the companion, re-asserted on
        # every connect so a config change takes effect on restart.
        raw_hb = self.config.get("path_hash_bytes", 2)
        try:
            hb = int(raw_hb)
            if hb not in (1, 2, 3):
                raise ValueError("must be 1, 2 or 3")
        except (TypeError, ValueError) as exc:
            logger.warning("ignoring path_hash_bytes=%r: %s", raw_hb, exc)
        else:
            try:
                ok = await self.client.set_path_hash_mode(hb)
                logger.info("path hash: %d-byte -> %s", hb, "ok" if ok else "REFUSED")
            except Exception as exc:
                logger.error("path hash mode failed: %s", exc)

        for entry in self.config.get("channels") or []:
            try:
                idx = int(entry["idx"])
                name = str(entry["name"])
                secret_hex = entry.get("secret")
                secret = bytes.fromhex(secret_hex) if secret_hex else hashtag_secret(name)
                ok = await self.client.set_channel(idx, name, secret)
                logger.info("channel slot %s = %r -> %s", idx, name, "ok" if ok else "REFUSED")
            except Exception as exc:
                # Never echo the entry itself: it may carry a channel secret.
                logger.error(
                    "channel entry idx=%r name=%r rejected: %s",
                    entry.get("idx") if isinstance(entry, dict) else None,
                    entry.get("name") if isinstance(entry, dict) else None,
                    exc,
                )

        self._channels.clear()
        await self._refresh_channels()

    # ----------------------------------------------------------------
    # Inbound from the mesh
    # ----------------------------------------------------------------

    async def on_message(self, msg: Message) -> None:
        """Mirror a decoded message (§5.5)."""
        try:
            # The on-wire path_len encodes the hop count in its low 6 bits
            # (0xFF means direct/unknown). The hop *hashes* are not carried on
            # this path, so hops_n is reported without hops -- which §4 allows,
            # and which is honest rather than inventing a chain.
            hops_n = 0 if msg.path_len in (0, 0xFF) else msg.path_len & 0x3F
            # The V3 frame carries SNR as a signed byte where 0 encodes both
            # "0.0 dB" and "not set". openhop_core does populate it for
            # messages that carry RF metadata, but a pre-v3 peer or a message
            # without it yields a bare 0 that would publish as a fabricated
            # reading (§4). Omitting on 0 costs only a genuine 0.0 dB sample.
            event = {
                "message_text": msg.text,
                "timestamp": msg.timestamp,
                "network_info": {"snr": msg.snr if msg.snr else None},
                "path": None,
            }
            if msg.channel_idx is None:
                event["contact_name"] = self._contact_name(msg.sender_prefix)
                payload = build_dm(event)
                payload["hops_n"] = hops_n
                self._publish("msg/dm", payload)
            else:
                slot = self._channels.get(msg.channel_idx)
                event["channel_name"] = slot.name if slot else "?"
                # The sender is already embedded in the on-air text as
                # "<sender>: <message>", so nothing is prepended here.
                payload = build_channel_message(event)
                payload["hops_n"] = hops_n
                self._publish("msg/channel", payload)
        except Exception as exc:
            logger.error("message mirror failed: %s", exc, exc_info=True)

    async def on_advert(self, advert: Advert) -> None:
        """PUSH_CODE_ADVERT: pubkey only, so it just notes that a node exists.

        The rich version arrives via on_raw_frame(); this keeps a bare entry so
        a node heard before its frame decoded still appears in the roster.
        """
        key = pk8(advert.public_key)
        if key and key not in self._adverts:
            await self._record_advert(
                {"pubkey": advert.public_key, "rx_ts": int(time.time())}
            )

    async def _record_advert(self, row: Dict[str, Any]) -> None:
        """Cache an advert and, when enabled, dump it (§5.7).

        The cache is what feeds heard/: it is the only place the ingress hop
        path is retained, and it is bounded to roughly §5.4's scope.
        """
        key = pk8(row.get("pubkey"))
        if not key:
            return

        # A later, richer decode must not be overwritten by a bare push.
        existing = self._adverts.get(key) or {}
        merged = {**existing, **{k: v for k, v in row.items() if v is not None}}
        self._adverts[key] = merged

        if len(self._adverts) > _ADVERT_CACHE_MAX:
            # Evict by when WE heard it, never by the advertiser's clock (M6).
            stale = sorted(self._adverts.items(), key=lambda kv: heard_at(kv[1]))
            for k, _ in stale[: len(self._adverts) - _ADVERT_CACHE_MAX]:
                self._adverts.pop(k, None)

        if self.advert_dump and "raw" in merged:
            self._publish("advert", build_advert(merged))

    async def on_raw_frame(self, raw: bytes, snr: float, rssi: int) -> None:
        """Count a heard frame, decode it if it is an advert, publish if enabled.

        Adverts are recovered here rather than from PUSH_CODE_ADVERT: that push
        carries only the 32-byte pubkey, while the raw frame carries the name,
        type, position, the advertiser's clock and the hop path -- and this
        callback also supplies the SNR it was heard at. Adverts are signed, not
        encrypted, so no keys are needed.
        """
        self._rx += 1
        self._last_snr = snr
        self._last_rssi = rssi

        try:
            decoded = decode_advert_frame(raw)
        except Exception as exc:
            decoded = None
            logger.debug("advert decode failed: %s", exc)

        if decoded is not None:
            decoded["snr"] = snr
            decoded["rssi"] = rssi
            decoded["rx_ts"] = int(time.time())
            await self._record_advert(decoded)

        if self.publish_packets:
            self._publish("packets", build_packet(raw.hex(), snr, rssi))

    def _contact_name(self, prefix: bytes) -> Optional[str]:
        if not prefix:
            return None
        want = prefix.hex().lower()
        for row in self._contacts:
            key = row.get("pubkey")
            hexkey = key.hex().lower() if isinstance(key, (bytes, bytearray)) else str(key).lower()
            if hexkey.startswith(want[:12]):
                return row.get("node_name")
        return None

    # ----------------------------------------------------------------
    # Cadences (§5.1-5.4)
    # ----------------------------------------------------------------

    async def tick_telemetry(self) -> None:
        stats = {
            "uptime_secs": int(time.time() - self._start_time),
            "packets_received": self._rx,
            "packets_sent": 0,
        }
        mv = self._battery_mv
        self._publish(
            "telemetry",
            build_telemetry(
                stats,
                boot_reason="plugin-start",
                boot_count=self._boots,
                batt_mv=mv,
                batt_pct=100 if mv else None,
                heap=_process_rss_bytes(),
                last_snr=self._last_snr,
                last_rssi=self._last_rssi,
            ),
        )

    async def tick_sensors(self) -> None:
        """A plugin has no sensor bus of its own, so this is always ``{}`` --
        published deliberately, so a subscriber can tell "no sensors" from
        "node offline" (§5.2)."""
        self._publish("sensors", build_sensors([]))

    async def tick_contacts(self) -> None:
        """Walk the contact table and publish it retained, sliced (§5.3)."""
        await self._refresh_channels()
        contacts = await self.client.get_contacts()
        if contacts:
            self._contacts = [
                {
                    "pubkey": c.public_key,
                    "node_name": c.name,
                    "contact_type": c.type,
                    "timestamp": c.last_advert,
                    "latitude": c.latitude,
                    "longitude": c.longitude,
                }
                for c in contacts
            ]
        # §5.3 defines the roster as the contact table. Publishing heard
        # adverts here as well left retained contact/ topics for nodes the
        # companion never actually added, and nothing ever cleared them
        # (audit L6). heard/ is the topology feed; contact/ is the roster.
        rows = self._contacts

        # Sliced rather than published in one burst: a few hundred retained
        # contacts at once would put ~100 KB of copies in the outbound queue.
        for start in range(0, len(rows), CONTACTS_BATCH):
            if self._stop.is_set():
                return
            for row in rows[start : start + CONTACTS_BATCH]:
                key = pk8(row.get("pubkey"))
                if key:
                    self._publish(f"contact/{key}", build_contact(row), retain=True)
            await asyncio.sleep(CONTACTS_SLICE_MS / 1000.0)

    async def tick_heard(self) -> None:
        for row in most_recent(list(self._adverts.values()), limit=HEARD_LIMIT):
            key = pk8(row.get("pubkey"))
            if key:
                self._publish(f"heard/{key}", build_heard(row), retain=True)

    # ----------------------------------------------------------------
    # Send bridge (§6)
    # ----------------------------------------------------------------

    def on_mqtt_message(self, topic: str, payload: bytes) -> None:
        """Parse and queue one inbound send. Runs on the MQTT client thread.

        Failures are silent (§6.4): an error reply would itself be airtime.
        """
        try:
            token = parse_send_topic(topic, self.mqtt.send_prefixes())
            if token is None:
                return
            text = clamp_text(payload)
            if text is None:
                return
            try:
                self._send_queue.put_nowait((token, text))
            except queue.Full:
                logger.debug("send queue full; dropping")
        except Exception as exc:
            logger.error("send parse failed: %s", exc, exc_info=True)

    async def _drain_sends(self) -> None:
        """Drain queued sends on the asyncio side, one per rate-limit grant.

        Resolving a channel name needs companion state, so it happens here
        rather than on the MQTT thread (§6.5).
        """
        while not self._stop.is_set():
            await asyncio.sleep(0.5)
            while not self._send_queue.empty() and self._rate_limiter.allow():
                try:
                    token, text = self._send_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    await self._refresh_channels()
                    idx = resolve_channel(token, self._channels)
                    if idx is None:
                        logger.debug("send bridge: no channel matching %r; dropped", token)
                        continue
                    ok = await self.client.send_channel_message(idx, text)
                    logger.info("send bridge: channel %s (%r) -> %s", idx, token, ok)
                except Exception as exc:
                    logger.error("send failed: %s", exc, exc_info=True)

    async def _refresh_channels(self) -> None:
        """Cache the configured channel slots, for name resolution (§6.1).

        Scans the server's full default of 40 slots, not 16, and is re-run on
        every roster tick so channels added later still resolve (audit L2).
        """
        fresh: Dict[int, _Slot] = {}
        for idx in range(40):
            name = await self.client.get_channel(idx)
            if name:
                fresh[idx] = _Slot(name)
        if fresh or not self._channels:
            self._channels = fresh

    # ----------------------------------------------------------------

    def _publish(self, subtopic: str, payload: dict, retain: bool = False) -> None:
        self.mqtt.publish(subtopic, json.dumps(payload, separators=(",", ":")), retain=retain)


def _positive_int_or_none(value, key: str) -> Optional[int]:
    """Coerce an operator value once, at startup. Bad input warns and is
    ignored rather than raising inside tick_telemetry every 60s for the life
    of the process (fourth audit #3); the env layer delivers strings, so "0"
    and "abc" must both be handled (#4)."""
    if value is None or value is False or value == "":
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        logger.warning("ignoring %s=%r: not an integer", key, value)
        return None
    if n <= 0:
        return None
    return n


def _process_rss_bytes() -> Optional[int]:
    """Resident set size of this process. ru_maxrss is bytes on macOS, KiB on Linux."""
    if resource is None:
        return None
    try:
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(rss if sys.platform == "darwin" else rss * 1024)
    except Exception:
        return None


def _bump_boot_counter(state_dir: Optional[str]) -> Optional[int]:
    """Increment and return a persistent start counter, or None if unwritable."""
    try:
        # expanduser() raises RuntimeError with no HOME and no passwd entry
        # (a `docker --user N` container). It must be inside the try, or the
        # constructor -- and so startup -- died instead of omitting `boots`.
        # An empty XDG_STATE_HOME is treated as unset, per the XDG spec.
        xdg = os.environ.get("XDG_STATE_HOME") or "~/.local/state"
        base = Path(state_dir).expanduser() if state_dir else Path(xdg).expanduser() / "openhop-txmesh"
        base.mkdir(parents=True, exist_ok=True)
        f = base / "boots"
        count = (int(f.read_text().strip() or 0) + 1) if f.exists() else 1
        f.write_text(str(count))
        return count
    except Exception as exc:
        logger.warning("boot counter unavailable (state_dir=%r): %s", state_dir, exc)
        return None
