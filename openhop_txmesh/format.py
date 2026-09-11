"""
Payload and topic builders for the MeshCore Observer MQTT contract.

Implements the interface specified in MeshCore's
``examples/observer_node/MQTT.md``. That contract belongs to a *companion*
node (upstream's observer_node is a ``BaseChatMesh``, not a repeater), which
is why it can decrypt channel traffic and reply on the send bridge.

Topic layout differs from openHop's own MC2MQTT family, which is why this
node runs its own MQTT client rather than reusing ``mqtt_handler``:

    MC2MQTT    meshcore/{IATA}/{FULL_PUBKEY}/...
    observer   meshcore/{user|IATA}/{node_name}/...   with <pk8> sub-segments

Everything here is a pure function over plain dicts so the contract can be
unit-tested without a broker, a radio, or a running daemon. The stateful side
(timers, retained-topic bookkeeping, the send bridge) lives in
``publisher.py``.

Two conventions from §4 are load-bearing and enforced throughout:

* **Optional keys are omitted, not null.** A consumer tests for presence
  (``"lat" in c``); emitting ``null`` would break that. ``_compact()`` drops
  ``None`` values, so builders may assign freely and let it filter.
* **Additive evolution only.** Never change the meaning or type of an existing
  key. Consumers are required to ignore unknown keys, so adding is safe.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Mapping, Optional

# Cadences from MQTT.md §5. Seconds.
TELEMETRY_INTERVAL_S = 60
SENSORS_INTERVAL_S = 60
CONTACTS_INTERVAL_S = 300
HEARD_INTERVAL_S = 180

# Contact-roster walk is sliced rather than published in one burst (§5.3).
# A few hundred retained contacts published at once stalls the outbound queue.
CONTACTS_BATCH = 8
CONTACTS_SLICE_MS = 250

# §5.4 publishes only the N most recently heard nodes.
HEARD_LIMIT = 16

# §6.2 - the send bridge converts broker writes into mesh airtime.
SEND_MAX_PER_MIN = 6
SEND_QUEUE_DEPTH = 4
MAX_TEXT_LEN = 160
MAX_CHANNEL_TOKEN_LEN = 31

# MQTT topics cannot contain these; §3.1 maps each to '-' in free-form segments.
_TOPIC_UNSAFE = ("#", "+", "/", " ")

# openhop_core's get_contact_type_name() output -> MQTT.md §5.3 vocabulary.
_CONTACT_TYPE_MAP = {
    "chat node": "chat",
    "repeater": "repeater",
    "room server": "room",
    "sensor": "sensor",
    "unknown": "unknown",
}

# ADV_TYPE_* ids, as carried by NODE_DISCOVERED's `contact_type`.
_CONTACT_TYPE_IDS = {
    0: "unknown",
    1: "chat",
    2: "repeater",
    3: "room",
    4: "sensor",
}

# MQTT.md §5.2 sensor types -> Cayenne-LPP-style channel-suffixed keys.
_SENSOR_KEY_MAP = {
    "temperature": "temperature",
    "temperature_c": "temperature",
    "humidity": "humidity",
    "pressure": "pressure",
    "voltage": "voltage",
    "bus_voltage": "voltage",
    "current": "current",
    "current_a": "current",
    "power": "power",
    "power_w": "power",
    "altitude": "altitude",
}


def _compact(d: Mapping[str, Any]) -> Dict[str, Any]:
    """Drop keys whose value is None. §4: optional keys are omitted, not null."""
    return {k: v for k, v in d.items() if v is not None}


def sanitise_segment(segment: str) -> str:
    """Make one free-form topic segment safe (§3.1).

    ``#``, ``+``, ``/`` and space each become ``-``; a trailing ``/`` is
    stripped. Note this is deliberately NOT applied to a verbatim
    ``base_topic`` override, which is used exactly as given.
    """
    out = str(segment or "").rstrip("/")
    for ch in _TOPIC_UNSAFE:
        out = out.replace(ch, "-")
    return out


def resolve_prefix(broker: Mapping[str, Any], node_name: str) -> str:
    """Resolve ``<prefix>`` once per client build, first match wins (§3.1).

    1. ``base_topic``  -> that string, verbatim and unsanitised
    2. ``iata``        -> meshcore/<IATA>/<node_name>     (public collectors)
    3. ``username``    -> meshcore/<username>/<node_name> (ACL-aligned default)
    4. otherwise       -> meshcore/<node_name>

    Layouts 2 and 3 are mutually exclusive by design: a ``meshcore/%u/#`` ACL
    stops matching the moment segment 1 becomes a region.
    """
    base = broker.get("base_topic")
    if base:
        return str(base)

    node = sanitise_segment(node_name)

    iata = broker.get("iata")
    if iata:
        return f"meshcore/{sanitise_segment(str(iata).upper())}/{node}"

    user = broker.get("username")
    if user:
        return f"meshcore/{sanitise_segment(user)}/{node}"

    return f"meshcore/{node}"


def fleet_prefix(broker: Mapping[str, Any]) -> Optional[str]:
    """The per-user fleet send prefix, ``meshcore/<username>/all`` (§6.3).

    Derived from the username alone - never from ``base_topic`` or ``iata``, so
    a node on the per-region collector layout has no fleet subscription.
    """
    if broker.get("iata") or broker.get("base_topic"):
        return None
    user = broker.get("username")
    if not user:
        return None
    return f"meshcore/{sanitise_segment(user)}/all"


def pk8(pubkey: Any) -> str:
    """Node identity in topics: first 4 pubkey bytes as 8 lowercase hex (§3.2)."""
    if isinstance(pubkey, (bytes, bytearray, memoryview)):
        return bytes(pubkey)[:4].hex()
    return str(pubkey or "").replace(" ", "").lower()[:8]


def contact_type(raw: Any) -> str:
    """Map openhop_core's contact type onto the §5.3 vocabulary.

    Accepts either the numeric ADV_TYPE id (as carried by NODE_DISCOVERED) or
    the human-readable name (as stored in the adverts table), since the two
    sources disagree on representation.
    """
    if isinstance(raw, bool):
        return "unknown"
    if isinstance(raw, int):
        return _CONTACT_TYPE_IDS.get(raw, "unknown")
    return _CONTACT_TYPE_MAP.get(str(raw or "").strip().lower(), "unknown")


def normalise_advert(event: Mapping[str, Any]) -> Dict[str, Any]:
    """Flatten a ``NODE_DISCOVERED`` event into the shape the builders expect.

    The event is the richest advert source available: unlike the adverts table
    it carries the decoded ``inbound_path`` (so ``hops`` can be populated) and
    the raw wire packet (so §5.7's ``raw`` is exact).
    """
    raw_packet = event.get("raw_advert_packet")
    return {
        "pubkey": event.get("public_key"),
        "node_name": event.get("name"),
        "contact_type": event.get("contact_type"),
        # advert_timestamp is the advertiser's own clock; timestamp is ours.
        "timestamp": event.get("advert_timestamp") or event.get("timestamp"),
        "rx_ts": event.get("timestamp"),
        "latitude": event.get("lat"),
        "longitude": event.get("lon"),
        "snr": event.get("snr"),
        "rssi": event.get("rssi"),
        "path": event.get("inbound_path") or b"",
        "raw": raw_packet.hex() if isinstance(raw_packet, (bytes, bytearray)) else raw_packet,
    }


def hops_fields(path: Optional[Iterable[int]], hash_size: int = 1) -> Dict[str, Any]:
    """Build ``hops_n`` / ``hops`` from decoded path bytes (§4).

    ``path`` is the raw MeshCore path: the node hashes in travel order, each
    ``hash_size`` bytes wide (1, 2 or 3 -- uniform within one packet).
    ``hops`` is the full-width chain as hex, so a 2-byte mesh publishes four
    characters per hop, which is what the observer firmware publishes and what
    txme.sh's path resolver expects. ``hops_n`` is the hop count, not the byte
    count. ``hops`` is omitted when heard direct, and ``hops_n`` is 0.
    Callers must pass *decoded* values - the on-wire ``path_len`` packs the
    count into the low 6 bits and hash-size-1 into the top 2.
    """
    if not path:
        return {"hops_n": 0}
    raw = bytes(b & 0xFF for b in path)
    width = hash_size if hash_size in (1, 2, 3) else 1
    return {"hops_n": len(raw) // width, "hops": raw.hex()}


def _coord(value: Any) -> Optional[float]:
    """A coordinate, or None when the node adverts no position (0,0 = none)."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return None if f == 0.0 else f


# --------------------------------------------------------------------
# Outbound payload builders (§5)
# --------------------------------------------------------------------


def build_telemetry(
    stats: Mapping[str, Any],
    *,
    boot_reason: Optional[str] = None,
    boot_count: Optional[int] = None,
    batt_mv: Optional[int] = None,
    batt_pct: Optional[int] = None,
    heap: Optional[int] = None,
    last_snr: Optional[float] = None,
    last_rssi: Optional[int] = None,
) -> Dict[str, Any]:
    """``<prefix>/telemetry`` - node health (§5.1). Not retained.

    ``stats`` is openhop's live-stats dict. Keys the host genuinely cannot
    report are omitted rather than faked: a Linux/macOS host has no battery or
    LiPo curve unless an INA219 is attached, and ``heap`` is an embedded
    concept with no host equivalent.
    """
    return _compact(
        {
            "batt_mv": batt_mv,
            "batt_pct": batt_pct,
            "uptime_s": int(stats.get("uptime_secs", 0)),
            "heap": heap,
            "rx": int(stats.get("packets_received", 0)),
            "relayed": int(stats.get("packets_sent", 0)),
            "dropped": int(stats.get("dropped", 0)) or None,
            "snr": last_snr,
            "rssi": last_rssi,
            "boot": boot_reason,
            "boots": boot_count,
        }
    )


def build_sensors(readings: Optional[Iterable[Mapping[str, Any]]]) -> Dict[str, Any]:
    """``<prefix>/sensors`` - attached sensors (§5.2). Not retained.

    Keys are ``<type><channel>`` so several sensors of one type don't collide.
    Returns ``{}`` when nothing is attached - deliberately, so a subscriber can
    tell "no sensors" from "node offline".
    """
    out: Dict[str, Any] = {}
    if not readings:
        return out

    channel: Dict[str, int] = {}
    for idx, reading in enumerate(readings, start=1):
        if not reading or not reading.get("ok", False):
            continue
        data = reading.get("data") or {}

        lat, lon = data.get("latitude"), data.get("longitude")
        if lat is not None and lon is not None:
            channel["gps"] = channel.get("gps", 0) + 1
            out[f"gps{channel['gps']}"] = _compact(
                {
                    "lat": _coord(lat),
                    "lon": _coord(lon),
                    "alt": data.get("altitude"),
                }
            )

        for field, value in data.items():
            kind = _SENSOR_KEY_MAP.get(str(field).lower())
            if kind is None or not isinstance(value, (int, float)):
                continue
            channel[kind] = channel.get(kind, 0) + 1
            out[f"{kind}{channel[kind]}"] = float(value)

        del idx
    return out


def build_contact(advert: Mapping[str, Any]) -> Dict[str, Any]:
    """``<prefix>/contact/<pk8>`` - roster entry (§5.3). Retained.

    ``heard`` is the epoch of the last advert from this node; consumers should
    treat it as the staleness signal rather than relying on topic presence
    (an evicted contact keeps its last retained value forever).
    """
    return _compact(
        {
            "pubkey": pk8(advert.get("pubkey")),
            "name": advert.get("node_name") or advert.get("name"),
            "type": contact_type(advert.get("contact_type")),
            "heard": int(advert.get("timestamp", 0)) or None,
            "lat": _coord(advert.get("latitude")),
            "lon": _coord(advert.get("longitude")),
        }
    )


def build_heard(advert: Mapping[str, Any]) -> Dict[str, Any]:
    """``<prefix>/heard/<pk8>`` - advert ingress topology (§5.4). Retained.

    ``snr`` is the advert as received here: for ``hops_n: 0`` that is the
    direct link to the origin; for multi-hop it is the last relay's
    transmission, not the origin's.
    """
    return _compact(
        {
            "pubkey": pk8(advert.get("pubkey")),
            "name": advert.get("node_name") or advert.get("name"),
            "ts": int(advert.get("timestamp", 0)) or None,
            "snr": advert.get("snr"),
            **hops_fields(advert.get("path"), advert.get("hash_size") or 1),
        }
    )


def build_dm(event: Mapping[str, Any], *, rx_ts: Optional[int] = None) -> Dict[str, Any]:
    """``<prefix>/msg/dm`` - mirrored direct message (§5.5). Not retained.

    Sourced from ``MeshEvents.NEW_MESSAGE``. Mirrors every readable DM onto the
    broker; that is the operator's explicit choice in pointing the node at one.
    """
    now = int(rx_ts if rx_ts is not None else time.time())
    net = event.get("network_info") or {}
    sender_ts = int(event.get("timestamp", 0)) or None

    return _compact(
        {
            "from": event.get("contact_name") or event.get("sender_name"),
            "text": event.get("message_text", ""),
            "snr": net.get("snr"),
            **hops_fields(event.get("path"), event.get("hash_size") or 1),
            "sender_ts": sender_ts,
            "rx_ts": now,
            "skew_s": (now - sender_ts) if sender_ts else None,
        }
    )


def build_channel_message(
    event: Mapping[str, Any], *, rx_ts: Optional[int] = None
) -> Dict[str, Any]:
    """``<prefix>/msg/channel`` - mirrored channel message (§5.5). Not retained.

    Sourced from ``MeshEvents.NEW_CHANNEL_MESSAGE``. Per §5.5 the sender is
    embedded in ``text`` as ``<sender>: <message>`` (the on-air group-message
    format), so it is reassembled here when the handler split it out.
    """
    now = int(rx_ts if rx_ts is not None else time.time())
    net = event.get("network_info") or {}
    sender_ts = int(event.get("timestamp", 0)) or None

    text = event.get("full_content") or event.get("message_text", "")
    sender = event.get("sender_name")
    if sender and not str(text).startswith(f"{sender}:"):
        text = f"{sender}: {text}"

    return _compact(
        {
            "channel": event.get("channel_name") or "?",
            "text": text,
            "snr": net.get("snr"),
            **hops_fields(event.get("path"), event.get("hash_size") or 1),
            "sender_ts": sender_ts,
            "rx_ts": now,
            "skew_s": (now - sender_ts) if sender_ts else None,
        }
    )


def build_advert(advert: Mapping[str, Any], *, rx_ts: Optional[int] = None) -> Dict[str, Any]:
    """``<prefix>/advert`` - advert dump (§5.7), opt-in. Not retained.

    ``skew_s`` is the advertiser's clock error: ~0 is fine, large positive
    means the node is in the past. ``hash`` is path-independent, so it is the
    right key for deduping relay copies of one advert.
    """
    now = int(rx_ts if rx_ts is not None else time.time())
    adv_ts = int(advert.get("timestamp", 0)) or None

    return _compact(
        {
            "pubkey": pk8(advert.get("pubkey")),
            "hash": advert.get("packet_hash"),
            "adv_ts": adv_ts,
            "rx_ts": now,
            "skew_s": (now - adv_ts) if adv_ts else None,
            "type": contact_type(advert.get("contact_type")),
            "name": advert.get("node_name") or advert.get("name"),
            "snr": advert.get("snr"),
            **hops_fields(advert.get("path"), advert.get("hash_size") or 1),
            "raw": advert.get("raw"),
        }
    )


def build_packet(raw_hex: str, snr: Optional[float], rssi: Optional[int]) -> Dict[str, Any]:
    """``<prefix>/packets`` - raw frame uplink (§5.8), opt-in. Not retained.

    Deliberately uses the collector's key casing (``SNR``/``RSSI``, not ours):
    this topic exists so a collector that decodes frames itself can ingest us,
    and the decoded topics stay as they are for consumers that know them.
    Highest-volume topic here - one publish per frame heard.
    """
    return _compact(
        {
            "raw": (raw_hex or "").lower(),
            "SNR": round(float(snr), 1) if snr is not None else None,
            "RSSI": int(rssi) if rssi is not None else None,
        }
    )


# --------------------------------------------------------------------
# Inbound: the MQTT -> mesh send bridge (§6)
# --------------------------------------------------------------------


def parse_send_topic(topic: str, prefixes: Iterable[str]) -> Optional[str]:
    """Extract ``<channel>`` from ``<prefix>/send/<channel>``.

    Returns the raw channel token (slot index or name), or None when the topic
    is not a send topic for any of our prefixes. Over-long tokens are rejected
    here rather than truncated (§6.2).
    """
    for prefix in prefixes:
        if not prefix:
            continue
        head = f"{prefix}/send/"
        if topic.startswith(head):
            token = topic[len(head) :]
            if not token or "/" in token:
                return None
            if len(token) > MAX_CHANNEL_TOKEN_LEN:
                return None
            return token
    return None


def resolve_channel(token: str, channels: Mapping[int, Any]) -> Optional[int]:
    """Resolve a send-topic channel token to a configured slot index (§6.1).

    ``token`` is either a decimal slot index (0-255, max 3 digits) or a channel
    name matched case-insensitively, with a leading ``#`` on the *stored* name
    ignored (MQTT topics cannot contain ``#``).

    Prefer names in a fleet: slot order is per-node configuration history, and
    a name miss sends nothing rather than posting into whatever occupies that
    slot number on some other node.
    """
    # str.isdigit() is True for "²" but int("²") raises; require ASCII (audit L3).
    if token.isascii() and token.isdigit() and len(token) <= 3:
        idx = int(token)
        if 0 <= idx <= 255 and channels.get(idx) is not None:
            return idx
        return None

    want = token.strip().lower()
    for idx, channel in channels.items():
        name = getattr(channel, "name", None) or ""
        if str(name).lstrip("#").strip().lower() == want:
            return idx
    return None


def clamp_text(payload: bytes) -> Optional[str]:
    """Decode and length-clamp a send-bridge payload (§6.2).

    Returns None for an empty payload or undecodable bytes - dropped silently,
    because any error reply would itself cost mesh airtime (§6.4).
    """
    # Bound BEFORE decoding: a multi-megabyte publish would otherwise cost a
    # full decode and allocation per message (audit L8). 4 bytes/char is the
    # UTF-8 maximum, so this can never clip a payload that would have fit.
    try:
        if len(payload) > MAX_TEXT_LEN * 4:
            payload = payload[: MAX_TEXT_LEN * 4]
        # strict: undecodable bytes DROP the message (§6.4 silent drop) rather
        # than transmit a stripped version the sender never wrote.
        text = payload.decode("utf-8", errors="strict")
    except (UnicodeDecodeError, AttributeError):
        return None
    text = text.strip()
    if not text:
        return None
    return text[:MAX_TEXT_LEN]


class RateLimiter:
    """Fixed-window send budget: ``max_per_min`` messages per 60 s (§6.2).

    The bridge turns broker writes into transmit time, so anyone with broker
    write access spends the mesh's airtime. This is the backstop for that.
    """

    def __init__(self, max_per_min: int = SEND_MAX_PER_MIN):
        self.max_per_min = max_per_min
        self._window_start = 0.0
        self._count = 0

    def allow(self, now: Optional[float] = None) -> bool:
        t = now if now is not None else time.monotonic()
        if t - self._window_start >= 60.0:
            self._window_start = t
            self._count = 0
        if self._count >= self.max_per_min:
            return False
        self._count += 1
        return True


def heard_at(advert: Mapping[str, Any]) -> int:
    """When *we* heard this advert -- the only clock an RF peer cannot set.

    ``timestamp`` is the advertiser's own clock and is published as such, but it
    must never decide ordering or eviction: one advert stamped 0xFFFFFFFF would
    otherwise sit in heard/ forever while correctly-clocked nodes were evicted
    around it (audit M6).
    """
    return int(advert.get("rx_ts") or 0)


def most_recent(adverts: Iterable[Mapping[str, Any]], limit: int = HEARD_LIMIT) -> List[dict]:
    """The ``limit`` most recently *heard* adverts, newest first, one per node."""
    seen: Dict[str, dict] = {}
    for advert in sorted(adverts, key=heard_at, reverse=True):
        key = pk8(advert.get("pubkey"))
        if key and key not in seen:
            seen[key] = dict(advert)
        if len(seen) >= limit:
            break
    return list(seen.values())


# --------------------------------------------------------------------
# Channel secrets
# --------------------------------------------------------------------


def hashtag_secret(name: str) -> bytes:
    """The auto-derived key for a ``#``-prefixed public channel.

    MeshCore derives it from the name alone -- ``sha256(name)[:16]`` -- which
    is what lets any node join ``#bot`` without exchanging a secret. Verified
    live: a message sent with this key was decrypted and answered by a bot on
    the mesh. A name without ``#`` is not a hashtag channel and gets no key.
    """
    import hashlib

    if not name.startswith("#"):
        raise ValueError(f"{name!r} is not a hashtag channel; supply an explicit secret")
    return hashlib.sha256(name.encode("ascii")).digest()[:16]


# --------------------------------------------------------------------
# Advert decoding from raw frames (§5.7)
# --------------------------------------------------------------------
#
# The companion protocol's advert push (PUSH_CODE_ADVERT) carries only the
# 32-byte public key -- no name, SNR, timestamp or path. Everything the
# contract needs is instead recovered by decoding the raw frame delivered by
# PUSH_CODE_LOG_RX_DATA, which also supplies the SNR/RSSI it was heard at.
#
# Adverts are signed, not encrypted, so this needs no keys.

_PAYLOAD_TYPE_ADVERT = 4
PAYLOAD_TYPE_TXT_MSG = 2
PAYLOAD_TYPE_GRP_TXT = 5

# Header bits[1:0]. The two TRANSPORT_* routes carry 4 bytes of transport
# codes between the header and path_len; the others do not.
_ROUTE_TRANSPORT_FLOOD = 0
_ROUTE_TRANSPORT_DIRECT = 3

# Text payloads: AES-128-ECB over ts(4) | flags(1) | text, zero-padded to the
# block, behind a 2-byte MAC (openhop_core.protocol.crypto, matching firmware).
CIPHER_BLOCK = 16
CIPHER_MAC = 2


def decode_frame_header(raw: bytes) -> Optional[Dict[str, Any]]:
    """Split an on-wire frame into header fields, path and payload.

    Layout: ``header(1) | [transport codes(4)] | path_len(1) | path | payload``
    where ``path_len`` packs ``hash_count`` into bits[5:0] and ``hash_size-1``
    into bits[7:6]. ``path`` is returned at full width: a 2-byte mesh gives
    two bytes per hop. Returns None for a frame too short to hold its header.
    """
    if len(raw) < 2:
        return None
    header = raw[0]
    route = header & 0x03
    payload_type = (header >> 2) & 0x0F
    pos = 1
    if route in (_ROUTE_TRANSPORT_FLOOD, _ROUTE_TRANSPORT_DIRECT):
        pos += 4
    if len(raw) <= pos:
        return None
    path_len = raw[pos]
    hash_size = (path_len >> 6) + 1
    hop_count = path_len & 0x3F
    start = pos + 1
    end = start + hop_count * hash_size
    if len(raw) < end:
        return None
    return {
        "route": route,
        "payload_type": payload_type,
        "path_len": path_len,
        "hash_size": hash_size,
        "hop_count": hop_count,
        "path": bytes(raw[start:end]),
        "payload": bytes(raw[end:]),
    }


def channel_hash_byte(secret: bytes) -> int:
    """The 1-byte channel hash a GRP_TXT payload starts with.

    Mirrors openhop_core's PacketBuilder / GroupTextHandler: sha256 of the
    key material, where a 32-byte key whose second half is zero hashes as
    its first 16 bytes.
    """
    import hashlib

    key = bytes(secret)
    if len(key) >= 32 and key[16:32] == b"\x00" * 16:
        material = key[:16]
    else:
        material = key[:32] if len(key) > 32 else key
    return hashlib.sha256(material).digest()[0]


def text_cipher_lengths(text_bytes: int, trailing_nul: bool = False) -> range:
    """Ciphertext lengths a text of ``text_bytes`` bytes can encrypt to.

    Plaintext is ts(4) | flags(1) | text, and openhop_core appends a NUL to a
    direct message that firmware may not, so the range spans both.
    """
    lo = -(-(5 + text_bytes) // CIPHER_BLOCK) * CIPHER_BLOCK
    hi = -(-(6 + text_bytes) // CIPHER_BLOCK) * CIPHER_BLOCK if trailing_nul else lo
    return range(lo, hi + 1, CIPHER_BLOCK)


_ADV_FLAG_TYPE = 0x0F
_ADV_FLAG_HAS_LATLON = 0x10
_ADV_FLAG_HAS_NAME = 0x80

_ADV_TYPE_BY_ID = {0: "unknown", 1: "chat", 2: "repeater", 3: "room", 4: "sensor"}


def verify_advert_signature(body: bytes) -> bool:
    """Ed25519-verify an advert payload, exactly as openhop_core does.

    ``signed_region = pubkey + timestamp + appdata`` (i.e. everything except
    the signature itself), checked with the advertiser's own public key via
    openhop_core's Identity so MeshCore-specific key handling matches.

    This matters because LOG_RX_DATA hands us frames BEFORE openhop_core's
    own advert handler has verified them. Without this check an RF attacker
    could advert a real neighbour's pubkey with a forged name/position and it
    would become that node's retained heard/ row (second audit #1).
    """
    if len(body) < 101:
        return False
    try:
        from openhop_core.protocol import Identity

        return bool(Identity(body[0:32]).verify(body[0:36] + body[100:], body[36:100]))
    except Exception:
        return False


def decode_advert_frame(raw: bytes, *, verify: bool = True) -> Optional[Dict[str, Any]]:
    """Decode an on-wire advert frame, or None if it is not one.

    Returns None for a frame whose signature does not verify, unless
    ``verify=False`` (tests only). Adverts are signed, not encrypted, so
    verification needs no keys -- only the pubkey the advert itself carries.

    Layout (§5.7), offsets into the frame:

        0   header    1B   bits[1:0] route, bits[5:2] payload type, bits[7:6] ver
        1   path_len  1B   hash_size = (b >> 6) + 1;  hash_count = b & 0x3F
        2   path      hash_count * hash_size bytes, travel order
            --- advert payload ---
        +0    pubkey     32B
        +32   timestamp   4B  LE uint32, the advertiser's own clock
        +36   signature  64B
        +100  app flags   1B  0x0F type, 0x10 has lat/lon, 0x80 has name
        +101  latitude    4B  LE int32, degrees x 1e6   (iff 0x10)
        +105  longitude   4B  LE int32                  (iff 0x10)
        +109  name        rest                          (iff 0x80)

    The timestamp's absolute offset shifts with hop count, which is why the
    path has to be measured rather than assumed.
    """
    hdr = decode_frame_header(raw)
    if hdr is None or hdr["payload_type"] != _PAYLOAD_TYPE_ADVERT:
        return None

    body = hdr["payload"]
    if len(body) < 101:
        return None

    if verify and not verify_advert_signature(body):
        return None

    pubkey = body[0:32]
    timestamp = int.from_bytes(body[32:36], "little")
    flags = body[100]

    out: Dict[str, Any] = {
        "pubkey": pubkey,
        "contact_type": _ADV_TYPE_BY_ID.get(flags & _ADV_FLAG_TYPE, "unknown"),
        "timestamp": timestamp,
        # Full-width hop hashes, in travel order. On a 2-byte mesh each hop
        # is two bytes of the relay's pubkey; truncating to one byte would
        # publish a chain txme.sh cannot resolve.
        "path": hdr["path"],
        "hash_size": hdr["hash_size"],
        "raw": raw.hex(),
    }

    offset = 101
    if flags & _ADV_FLAG_HAS_LATLON and len(body) >= offset + 8:
        lat = int.from_bytes(body[offset : offset + 4], "little", signed=True) / 1e6
        lon = int.from_bytes(body[offset + 4 : offset + 8], "little", signed=True) / 1e6
        out["latitude"] = lat
        out["longitude"] = lon
        offset += 8

    if flags & _ADV_FLAG_HAS_NAME and len(body) > offset:
        out["node_name"] = (
            body[offset:].split(b"\x00")[0].decode("utf-8", errors="replace").strip()
        )

    return out
