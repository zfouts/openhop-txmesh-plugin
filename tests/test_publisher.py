"""
Tests for the publisher against a fake companion session.

The publisher's whole job is turning companion-protocol events into contract
payloads, so these exercise it through the same surface the real
``CompanionClient`` presents: callbacks in, commands out.
"""

import asyncio

import pytest

from openhop_txmesh.companion import Advert, Message
from openhop_txmesh.publisher import ObserverPublisher


class FakeCompanionClient:
    """Stands in for CompanionClient: records sends, serves canned state."""

    def __init__(self):
        self.on_connected = None
        self.on_message = None
        self.on_advert = None
        self.on_raw_frame = None
        self.sent = []
        self.contacts = []
        self.channels = {0: "Public", 3: "#bot"}

    def is_connected(self):
        return True

    async def get_contacts(self):
        return self.contacts

    async def get_channel(self, idx):
        return self.channels.get(idx)

    async def send_channel_message(self, idx, text):
        self.sent.append((idx, text))
        return True


@pytest.fixture
def pub():
    client = FakeCompanionClient()
    # host "" keeps the MQTT client from dialling out during tests.
    config = {"host": "", "username": "obs-zach", "node_name": "txmesh_bot"}
    publisher = ObserverPublisher(client, config)
    published = []
    publisher._publish = lambda sub, payload, retain=False: published.append(
        (sub, payload, retain)
    )
    return publisher, client, published


def topics(published):
    return [t for t, _, _ in published]


# ---------------------------------------------------------------- wiring


def test_publisher_registers_its_callbacks(pub):
    publisher, client, _ = pub
    # Bound methods are re-created per attribute access, so compare by equality
    # (same function + same instance), not identity.
    assert client.on_message == publisher.on_message
    assert client.on_advert == publisher.on_advert
    assert client.on_raw_frame == publisher.on_raw_frame


def test_prefix_comes_from_username_and_node_name(pub):
    publisher, _, _ = pub
    assert publisher.mqtt.prefix == "meshcore/obs-zach/txmesh_bot"


# ---------------------------------------------------------------- §5.5


def test_channel_message_is_mirrored(pub):
    publisher, _, published = pub
    asyncio.run(
        publisher.on_message(
            Message(text="KJ5DHR: !path", timestamp=1784504030, snr=11.8, path_len=2, channel_idx=3)
        )
    )
    sub, payload, retain = published[0]
    assert sub == "msg/channel"
    assert payload["text"] == "KJ5DHR: !path"
    assert retain is False


def test_channel_name_resolved_from_slot_cache(pub):
    publisher, _, published = pub
    asyncio.run(publisher._refresh_channels())
    asyncio.run(publisher.on_message(Message(text="hi", timestamp=1, snr=0, path_len=0, channel_idx=3)))
    assert published[0][1]["channel"] == "#bot"


def test_unknown_channel_slot_publishes_question_mark(pub):
    """§5.5: a channel with no stored name is '?', not omitted."""
    publisher, _, published = pub
    asyncio.run(publisher.on_message(Message(text="hi", timestamp=1, snr=0, path_len=0, channel_idx=9)))
    assert published[0][1]["channel"] == "?"


def test_dm_is_mirrored_with_sender_name(pub):
    publisher, _, published = pub
    publisher._contacts = [{"pubkey": bytes.fromhex("aabbccddeeff0011"), "node_name": "alice"}]
    asyncio.run(
        publisher.on_message(
            Message(
                text="on my way",
                timestamp=1784504030,
                snr=-7.5,
                path_len=0,
                sender_prefix=bytes.fromhex("aabbccddeeff"),
            )
        )
    )
    sub, payload, _ = published[0]
    assert sub == "msg/dm"
    assert payload["from"] == "alice"


# ---------------------------------------------------------------- §5.4 / 5.7


def make_advert_frame(pubkey: bytes = None, name: str = "node", *, hops: bytes = b"", node_type: int = 2,
                      timestamp: int = 1784431091, lat: float = None, lon: float = None) -> bytes:
    """Build a GENUINELY SIGNED on-wire advert the way a node would (§5.7).

    Signed with openhop_core's LocalIdentity so the frame passes the same
    verification the plugin now applies to every raw advert. ``pubkey`` is
    ignored (kept for call-site compatibility): a real key pair is generated
    and the frame carries its public half.
    """
    from openhop_core.protocol import LocalIdentity

    ident = LocalIdentity()
    pk = ident.get_public_key()
    header = (4 << 2) | 1                      # payload type 4 = ADVERT, route FLOOD
    path_len = len(hops)                       # hash_size 1 -> top 2 bits zero
    flags = (node_type & 0x0F) | 0x80          # has name
    appdata = b""
    if lat is not None:
        flags |= 0x10
        appdata += int(lat * 1e6).to_bytes(4, "little", signed=True)
        appdata += int(lon * 1e6).to_bytes(4, "little", signed=True)
    appdata = bytes([flags]) + appdata + name.encode()
    ts = timestamp.to_bytes(4, "little")
    sig = ident.sign(pk + ts + appdata)
    return bytes([header, path_len]) + hops + pk + ts + sig + appdata


def test_advert_decoded_from_raw_frame_feeds_heard(pub):
    """PUSH_CODE_ADVERT carries only a pubkey, so heard/ is built by decoding
    the raw frame -- which is also where the hop path comes from."""
    publisher, _, published = pub
    frame = make_advert_frame(None, "South-Gate", hops=b"\xdd\x17")
    key = frame[4:8].hex()  # pk8 of the generated identity (after header+path_len+2 hops)
    asyncio.run(publisher.on_raw_frame(frame, -3.2, -110))

    assert key in publisher._adverts
    assert topics(published) == []  # advert_dump and packets both default off

    asyncio.run(publisher.tick_heard())
    sub, payload, retain = published[0]
    assert sub == f"heard/{key}"
    assert retain is True
    assert payload["hops"] == "dd17" and payload["hops_n"] == 2
    assert payload["name"] == "South-Gate"
    assert payload["snr"] == -3.2


def test_bare_advert_push_still_registers_the_node(pub):
    """A pubkey-only push should not be lost, but must not overwrite a decode."""
    publisher, _, _ = pub
    frame = make_advert_frame(None, "South-Gate")
    key = frame[2:34]  # pubkey: header + path_len, no hops
    asyncio.run(publisher.on_raw_frame(frame, -3.2, -110))
    asyncio.run(publisher.on_advert(Advert(public_key=key)))
    assert publisher._adverts[key[:4].hex()]["node_name"] == "South-Gate"


def test_advert_dump_is_opt_in(pub):
    publisher, _, published = pub
    publisher.advert_dump = True
    asyncio.run(publisher.on_raw_frame(make_advert_frame(None, "South-Gate"), 7.0, -100))
    assert "advert" in topics(published)


def test_advert_cache_is_bounded(pub):
    publisher, _, _ = pub
    from openhop_txmesh.publisher import _ADVERT_CACHE_MAX

    for i in range(_ADVERT_CACHE_MAX + 50):
        asyncio.run(
            publisher._record_advert(
                {"pubkey": i.to_bytes(4, "big") + b"\x00" * 28, "rx_ts": i}
            )
        )
    assert len(publisher._adverts) <= _ADVERT_CACHE_MAX


# ---------------------------------------------------------------- §5.1 / 5.8


def test_raw_frames_count_toward_telemetry(pub):
    publisher, _, published = pub
    for _ in range(3):
        asyncio.run(publisher.on_raw_frame(b"\x11\x02", -7.5, -112))
    # packets is opt-in, so nothing published yet...
    assert topics(published) == []
    asyncio.run(publisher.tick_telemetry())
    payload = published[0][1]
    assert payload["rx"] == 3
    assert payload["snr"] == -7.5 and payload["rssi"] == -112


def test_packets_opt_in(pub):
    publisher, _, published = pub
    publisher.publish_packets = True
    asyncio.run(publisher.on_raw_frame(bytes.fromhex("1102dd17"), -7.5, -112))
    sub, payload, _ = published[0]
    assert sub == "packets"
    assert payload == {"raw": "1102dd17", "SNR": -7.5, "RSSI": -112}


def test_sensors_always_empty_object(pub):
    """A plugin has no sensor bus; {} still distinguishes it from offline."""
    publisher, _, published = pub
    asyncio.run(publisher.tick_sensors())
    assert published[0][0] == "sensors"
    assert published[0][1] == {}


# ---------------------------------------------------------------- §5.3


def test_contacts_published_retained(pub):
    publisher, client, published = pub
    from openhop_txmesh.companion import Contact

    client.contacts = [
        Contact(public_key=bytes.fromhex("d1d4c4cd" + "00" * 28), name="Rocky Creek",
                type=2, last_advert=1787534185, latitude=30.286468, longitude=-98.036593),
    ]
    asyncio.run(publisher.tick_contacts())
    sub, payload, retain = published[0]
    assert sub == "contact/d1d4c4cd"
    assert retain is True
    assert payload["name"] == "Rocky Creek"
    assert payload["type"] == "repeater"
    assert payload["lat"] == pytest.approx(30.286468)


# ---------------------------------------------------------------- §6


def test_send_bridge_resolves_name_and_transmits(pub):
    publisher, client, _ = pub

    async def drive():
        publisher.on_mqtt_message("meshcore/obs-zach/txmesh_bot/send/bot", b"hello mesh")
        assert publisher._send_queue.qsize() == 1  # queued, not yet sent
        await publisher._refresh_channels()
        token, text = publisher._send_queue.get_nowait()
        idx = __import__("openhop_txmesh.format", fromlist=["x"]).resolve_channel(
            token, publisher._channels
        )
        await client.send_channel_message(idx, text)

    asyncio.run(drive())
    assert client.sent == [(3, "hello mesh")]


def test_send_bridge_ignores_foreign_topics(pub):
    publisher, _, _ = pub
    publisher.on_mqtt_message("meshcore/someone-else/node/send/bot", b"hi")
    publisher.on_mqtt_message("meshcore/obs-zach/txmesh_bot/telemetry", b"hi")
    assert publisher._send_queue.empty()


def test_send_bridge_queue_overflow_dropped(pub):
    """§6.2: depth 4; overflow beyond the queue is dropped, not buffered."""
    publisher, _, _ = pub
    for i in range(10):
        publisher.on_mqtt_message("meshcore/obs-zach/txmesh_bot/send/bot", f"m{i}".encode())
    assert publisher._send_queue.qsize() == 4


def test_send_bridge_truncates_overlong_text(pub):
    publisher, _, _ = pub
    publisher.on_mqtt_message("meshcore/obs-zach/txmesh_bot/send/0", b"x" * 500)
    assert len(publisher._send_queue.get_nowait()[1]) == 160



# ---------------------------------------------------------------- host telemetry


def test_telemetry_host_fields(tmp_path, monkeypatch):
    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True

    cfg = {"host": "", "username": "u", "node_name": "n", "state_dir": str(tmp_path)}
    pub = ObserverPublisher(FakeClient(), cfg)
    out = []
    pub._publish = lambda s, p, retain=False: out.append(p)
    asyncio.run(pub.tick_telemetry())
    t = out[0]
    assert t["boots"] == 1 and (tmp_path / "boots").read_text() == "1"
    assert isinstance(t["heap"], int) and t["heap"] > 0
    assert "batt_mv" not in t and "batt_pct" not in t  # unset -> omitted

    pub2 = ObserverPublisher(FakeClient(), {**cfg, "battery_mv": 4600})
    out.clear(); pub2._publish = lambda s, p, retain=False: out.append(p)
    asyncio.run(pub2.tick_telemetry())
    assert out[0]["boots"] == 2  # counter persisted across instances
    assert out[0]["batt_mv"] == 4600 and out[0]["batt_pct"] == 100



def test_boot_counter_survives_homeless_container(monkeypatch, tmp_path):
    """expanduser() raising must not abort startup (fourth audit #1)."""
    import pwd
    from openhop_txmesh.publisher import _bump_boot_counter

    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    monkeypatch.setattr(pwd, "getpwuid", lambda uid: (_ for _ in ()).throw(KeyError(uid)))
    assert _bump_boot_counter(None) is None      # omitted, not crashed
    assert _bump_boot_counter("~/x") is None
    assert _bump_boot_counter(str(tmp_path)) == 1  # explicit absolute dir still works


@pytest.mark.parametrize("bad", ["abc", "0", 0, -5, "", None, False])
def test_battery_mv_invalid_values_are_omitted(bad):
    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n", "battery_mv": bad})
    out = []
    pub._publish = lambda s, p, retain=False: out.append(p)
    asyncio.run(pub.tick_telemetry())
    assert "batt_mv" not in out[0] and "batt_pct" not in out[0]


def test_battery_mv_string_from_env_is_coerced():
    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n", "battery_mv": "4600"})
    out = []
    pub._publish = lambda s, p, retain=False: out.append(p)
    asyncio.run(pub.tick_telemetry())
    assert out[0]["batt_mv"] == 4600 and out[0]["batt_pct"] == 100
