"""
Regression tests for the pre-release audit findings.

Each test names the finding it pins. These exist because the audit showed the
original bugs shipped in exactly the code paths that had no tests: env config
coercion (H1), the reconnect loop (M3/M4), and the companion handshake (M5).
"""

import asyncio
import json
import queue
import struct
import time
from types import SimpleNamespace

import pytest

import openhop_txmesh.mqtt as mqtt_mod
from openhop_txmesh.companion import (
    APP_TARGET_VER,
    MAX_TEXT_BYTES,
    CompanionClient,
    utf8_truncate,
)
from openhop_txmesh.format import (
    clamp_text,
    hashtag_secret,
    heard_at,
    most_recent,
    resolve_channel,
)
from openhop_txmesh.main import load_config
from openhop_txmesh.mqtt import ObserverMqttClient
from openhop_txmesh.publisher import ObserverPublisher


class FakePaho:
    def __init__(self):
        self.will = None
        self.published = []
        self.subscribed = []
        self.loop_stopped = 0
        self.connect_calls = 0
        self.on_connect = self.on_disconnect = self.on_message = None

    def will_set(self, *a, **k): self.will = (a, k)
    def username_pw_set(self, **k): pass
    def tls_set(self, **k): pass
    def tls_insecure_set(self, v): pass
    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload)); return SimpleNamespace(wait_for_publish=lambda timeout=None: True)
    def subscribe(self, topic, qos=0): self.subscribed.append(topic)
    def connect(self, *a, **k): self.connect_calls += 1
    def loop_start(self): pass
    def loop_stop(self): self.loop_stopped += 1
    def disconnect(self): pass


@pytest.fixture
def mqtt_client(monkeypatch):
    monkeypatch.setattr(mqtt_mod.mqtt, "Client", lambda **kw: FakePaho())
    c = ObserverMqttClient({"host": "h", "username": "u", "tls": {"enabled": False}}, "n")
    c._client = c._build()
    return c, c._client


# ---------------------------------------------------------------- H1


@pytest.mark.parametrize("value", ["false", "0", "no", "off", ""])
def test_h1_tls_insecure_env_false_means_false(monkeypatch, value):
    """bool("false") is True; the TLS keys must be coerced like the others."""
    monkeypatch.setenv("OPENHOP_TXMESH_TLS_INSECURE", value)
    monkeypatch.setenv("OPENHOP_TXMESH_TLS_ENABLED", "true")
    c = load_config(None)
    assert c["tls"]["insecure"] is False
    assert c["tls"]["enabled"] is True


@pytest.mark.parametrize("value", ["", "maybe", "${UNSET}", " "])
def test_h1_unrecognised_tls_env_is_ignored_not_false(monkeypatch, value):
    """A templated-but-empty TLS_ENABLED must not downgrade to plaintext
    (second audit #4): unrecognised means UNSET, and the default is on."""
    monkeypatch.setenv("OPENHOP_TXMESH_TLS_ENABLED", value)
    assert load_config(None)["tls"]["enabled"] is True
    monkeypatch.setenv("OPENHOP_TXMESH_TLS_ENABLED", "off")
    assert load_config(None)["tls"]["enabled"] is False


# ---------------------------------------------------------------- M1


def test_m1_retained_send_is_never_transmitted(mqtt_client):
    c, _ = mqtt_client
    got = []
    c.on_send = lambda t, p: got.append((t, p))
    c._on_message(None, None, SimpleNamespace(topic=f"{c.prefix}/send/bot", payload=b"x", retain=True))
    assert got == []
    c._on_message(None, None, SimpleNamespace(topic=f"{c.prefix}/send/bot", payload=b"x", retain=False))
    assert got == [(f"{c.prefix}/send/bot", b"x")]


# ---------------------------------------------------------------- M2


def test_m2_channel_data_frame_does_not_stall_drain():
    """RESP_CODE_CHANNEL_DATA_RECV (27) must be consumed as a sync reply, not
    discarded and waited out for 10s while holding the command lock."""
    from openhop_core.companion.constants import RESP_CODE_CHANNEL_DATA_RECV, RESP_CODE_NO_MORE_MESSAGES

    client = CompanionClient("127.0.0.1", 1)
    frames = [bytes([RESP_CODE_CHANNEL_DATA_RECV, 0, 0, 0, 0, 0]), bytes([RESP_CODE_NO_MORE_MESSAGES])]

    async def fake_write(payload): pass
    client._write = fake_write

    async def run():
        for f in frames:
            await client._responses.put(f)
        t0 = time.monotonic()
        await asyncio.wait_for(client.drain_messages(), timeout=3)
        return time.monotonic() - t0

    assert asyncio.run(run()) < 1.0


# ---------------------------------------------------------------- M3 / M4


def test_m3_refused_connack_stops_paho_loop(mqtt_client):
    c, fake = mqtt_client
    c._on_connect(fake, None, None, 5)  # not authorised
    assert fake.loop_stopped == 1
    assert not c.is_connected()


def test_m4_successful_dial_is_not_redialled_next_second(mqtt_client):
    c, fake = mqtt_client
    c._next_attempt = 0
    c._attempt()
    assert fake.connect_calls == 1
    # CONNACK hasn't arrived; _loop would poll again in 1s.
    assert c._next_attempt > time.monotonic() + 5


# ---------------------------------------------------------------- M5


def test_m5_app_start_requests_v3_frames():
    """app_target_ver byte must be >=3 or the server never sends SNR."""
    assert APP_TARGET_VER >= 3
    client = CompanionClient("127.0.0.1", 1)
    sent = []

    async def fake_write(payload): sent.append(payload)
    client._write = fake_write

    async def run():
        await client._responses.put(b"\x05" + b"\x00" * 67)  # SELF_INFO
        async with client._command_lock:
            await client._command(bytes([0x01, APP_TARGET_VER]) + b"\x00" * 6)

    asyncio.run(run())
    assert sent[0][1] == APP_TARGET_VER


# ---------------------------------------------------------------- M6


def test_m6_heard_orders_by_our_clock_not_advertisers():
    forged = {"pubkey": "aaaaaaaa", "timestamp": 0xFFFFFFFF, "rx_ts": 100}
    real = {"pubkey": "bbbbbbbb", "timestamp": 1_700_000_000, "rx_ts": 200}
    out = most_recent([forged, real], limit=1)
    assert out[0]["pubkey"] == "bbbbbbbb"
    assert heard_at(forged) == 100


# ---------------------------------------------------------------- channels feature


def test_hashtag_secret_matches_live_companion():
    """The value the companion stored for #bot when set over the wire."""
    assert hashtag_secret("#bot").hex() == "eb50a1bcb3e4e5d7bf69a57c9dada211"
    with pytest.raises(ValueError):
        hashtag_secret("private")


def test_set_channel_wire_layout():
    from openhop_core.companion.constants import CMD_SET_CHANNEL, RESP_CODE_OK

    client = CompanionClient("127.0.0.1", 1)
    sent = []

    async def fake_write(payload): sent.append(payload)
    client._write = fake_write

    async def run():
        await client._responses.put(bytes([RESP_CODE_OK]))
        return await client.set_channel(1, "#bot", hashtag_secret("#bot"))

    assert asyncio.run(run()) is True
    frame = sent[0]
    assert frame[0] == CMD_SET_CHANNEL and frame[1] == 1
    assert frame[2:34] == b"#bot".ljust(32, b"\x00")
    assert frame[34:50] == hashtag_secret("#bot")
    assert len(frame) == 50


def test_on_connected_pushes_configured_channels():
    calls = []

    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True
        async def set_channel(self, idx, name, secret): calls.append((idx, name, secret)); return True
        async def get_channel(self, idx): return {1: "#bot"}.get(idx)

    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n",
                                          "channels": [{"idx": 1, "name": "#bot"},
                                                       {"idx": 2, "name": "priv", "secret": "00" * 16}]})
    asyncio.run(pub.on_companion_connected())
    assert calls[0] == (1, "#bot", hashtag_secret("#bot"))
    assert calls[1] == (2, "priv", b"\x00" * 16)
    assert pub._channels[1].name == "#bot"  # cache warmed after push


# ---------------------------------------------------------------- L1


def test_l1_handshake_failure_tears_down():
    client = CompanionClient("127.0.0.1", 1)
    torn = []

    async def fake_open(*a, **k):
        r = SimpleNamespace(readexactly=lambda n: asyncio.sleep(3600))
        w = SimpleNamespace(write=lambda b: None, drain=lambda: asyncio.sleep(0), close=lambda: None,
                            wait_closed=lambda: asyncio.sleep(0))
        return r, w

    async def fake_teardown(): torn.append(1)
    client._teardown = fake_teardown

    async def run():
        import openhop_txmesh.companion as m
        orig = m.asyncio.open_connection
        m.asyncio.open_connection = fake_open
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(client._session(), timeout=0.5)
        finally:
            m.asyncio.open_connection = orig

    asyncio.run(run())
    assert torn  # teardown ran on the failure path


# ---------------------------------------------------------------- L3 / L8


def test_l3_utf8_truncate_never_splits_a_codepoint():
    out = utf8_truncate("a" * 159 + "é", MAX_TEXT_BYTES)
    assert out == b"a" * 159
    out.decode("utf-8")  # must not raise
    assert len(utf8_truncate("é" * 100, 160)) == 160  # 2-byte chars, even boundary


def test_l3_superscript_digit_is_not_a_slot_index():
    assert resolve_channel("²", {2: SimpleNamespace(name="x")}) is None


def test_l8_oversize_payload_is_bounded_before_decode():
    big = b"x" * (5 * 1024 * 1024)
    assert len(clamp_text(big)) == 160


# ---------------------------------------------------------------- L5


def test_l5_tls_defaults_on_when_config_lacks_it(monkeypatch):
    monkeypatch.setattr(mqtt_mod.mqtt, "Client", lambda **kw: FakePaho())
    c = ObserverMqttClient({"host": "h"}, "n")
    assert c.tls == {"enabled": True, "insecure": False}


def test_l5_nested_tls_dict_is_merged_not_clobbered(tmp_path, monkeypatch):
    cfg = tmp_path / "c.json"
    cfg.write_text('{"tls": {"enabled": true, "insecure": true}}')
    monkeypatch.setenv("OPENHOP_TXMESH_TLS_INSECURE", "false")
    c = load_config(str(cfg))
    assert c["tls"] == {"enabled": True, "insecure": False}


# ---------------------------------------------------------------- L6


def test_l6_contact_roster_is_the_contact_table_only():
    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True
        async def get_contacts(self): return []
        async def get_channel(self, idx): return None

    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n"})
    published = []
    pub._publish = lambda s, p, retain=False: published.append(s)
    asyncio.run(pub._record_advert({"pubkey": b"\xaa\xbb\xcc\xdd" + b"\x00" * 28, "rx_ts": 1}))
    asyncio.run(pub.tick_contacts())
    assert not any(t.startswith("contact/") for t in published)


# ---------------------------------------------------------------- INFO: explicit bounded queue


def test_send_queue_is_a_bounded_queue():
    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n"})
    assert isinstance(pub._send_queue, queue.Queue)
    for i in range(10):
        pub.on_mqtt_message("meshcore/u/n/send/0", f"m{i}".encode())
    assert pub._send_queue.qsize() == 4


# ---------------------------------------------------------------- msg/* snr honesty


def test_message_snr_omitted_when_frame_byte_is_zero():
    """0 on the wire is indistinguishable from unpopulated; never publish it."""
    from openhop_txmesh.companion import Message

    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True

    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n"})
    out = []
    pub._publish = lambda s, p, retain=False: out.append(p)
    asyncio.run(pub.on_message(Message(text="hi", timestamp=1, snr=0.0, path_len=0, channel_idx=0)))
    asyncio.run(pub.on_message(Message(text="hi", timestamp=1, snr=-7.5, path_len=0, channel_idx=0)))
    assert "snr" not in out[0]
    assert out[1]["snr"] == -7.5



# ================================================================ second audit


def _signed_advert(name: str = "Real-Node", node_type: int = 2, hops: bytes = b"") -> tuple:
    """A genuinely signed advert frame plus its identity, via openhop_core."""
    from openhop_core.protocol import LocalIdentity

    ident = LocalIdentity()
    pubkey = ident.get_public_key()
    ts = (1784431091).to_bytes(4, "little")
    appdata = bytes([(node_type & 0x0F) | 0x80]) + name.encode()
    sig = ident.sign(pubkey + ts + appdata)
    body = pubkey + ts + sig + appdata
    header = (4 << 2) | 1
    return bytes([header, len(hops)]) + hops + body, pubkey


def test_2a_forged_advert_is_rejected():
    """An RF attacker adverting a real pubkey with a fake name must not land
    in heard/ (second audit #1)."""
    from openhop_txmesh.format import decode_advert_frame

    frame, pubkey = _signed_advert("Real-Node")
    assert decode_advert_frame(frame)["node_name"] == "Real-Node"

    # Same pubkey, tampered name: signature no longer matches.
    forged = frame.replace(b"Real-Node", b"Evil-Node")
    assert decode_advert_frame(forged) is None
    # Tampered position/flags byte likewise.
    body_off = 2
    forged2 = bytearray(frame); forged2[body_off + 100] ^= 0x04
    assert decode_advert_frame(bytes(forged2)) is None
    # And a frame with garbage where the signature should be.
    junk = bytearray(frame); junk[body_off + 40] ^= 0xFF
    assert decode_advert_frame(bytes(junk)) is None


def test_2a_unsigned_decode_only_when_explicitly_disabled():
    from openhop_txmesh.format import decode_advert_frame

    frame, _ = _signed_advert()
    forged = frame.replace(b"Real-Node", b"Evil-Node")
    assert decode_advert_frame(forged, verify=False)["node_name"] == "Evil-Node"


def test_2a_publisher_drops_forged_adverts():
    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True

    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n"})
    frame, pubkey = _signed_advert("Real-Node")
    asyncio.run(pub.on_raw_frame(frame.replace(b"Real-Node", b"Evil-Node"), 5.0, -90))
    assert pub._adverts == {}
    asyncio.run(pub.on_raw_frame(frame, 5.0, -90))
    assert pub._adverts[pubkey[:4].hex()]["node_name"] == "Real-Node"


def test_2b_paho_floor_is_2():
    import re
    txt = open("pyproject.toml").read()
    assert re.search(r'"paho-mqtt>=2', txt), "loop_stop() in-thread is only safe on paho 2.x"


def test_2c_nested_tls_dict_beats_shipped_flat_defaults(tmp_path, monkeypatch):
    """The shipped defaults carry flat tls_*; they must not clobber the
    operator's nested dict (second audit #3)."""
    for k in ("OPENHOP_TXMESH_TLS_ENABLED", "OPENHOP_TXMESH_TLS_INSECURE"):
        monkeypatch.delenv(k, raising=False)
    cfg = tmp_path / "c.json"
    cfg.write_text('{"tls": {"insecure": true}}')
    assert load_config(str(cfg))["tls"] == {"enabled": True, "insecure": True}
    cfg.write_text('{"tls": {"enabled": false}}')
    assert load_config(str(cfg))["tls"]["enabled"] is False
    cfg.write_text('{"tls": "garbage"}')
    assert load_config(str(cfg))["tls"] == {"enabled": True, "insecure": False}  # no crash


def test_2e_next_attempt_is_set_before_network_thread_starts(mqtt_client):
    """CONNACK can arrive inside loop_start(); the write must precede it."""
    c, fake = mqtt_client
    seen = {}
    fake.loop_start = lambda: seen.setdefault("at_loop_start", c._next_attempt)
    c._next_attempt = 0
    c._attempt()
    assert seen["at_loop_start"] > time.monotonic() + 5


def test_2e_refusal_with_breaker_uses_probe_interval(mqtt_client):
    from openhop_txmesh.mqtt import _BREAKER_PROBE_S, _LADDER
    c, fake = mqtt_client
    c._rung = len(_LADDER) - 1
    c._fails_at_top = 2
    c._on_connect(fake, None, None, 5)  # trips the breaker
    assert c._breaker_tripped
    assert c._next_attempt > time.monotonic() + _BREAKER_PROBE_S - 5


def test_2e_paho_auto_reconnect_is_disabled(monkeypatch):
    captured = {}
    def fake_client(**kw): captured.update(kw); return FakePaho()
    monkeypatch.setattr(mqtt_mod.mqtt, "Client", fake_client)
    ObserverMqttClient({"host": "h", "tls": {"enabled": False}}, "n")._build()
    assert captured.get("reconnect_on_failure") is False


def test_2f_rejected_channel_entry_does_not_log_its_secret(caplog):
    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True
        async def get_channel(self, idx): return None

    secret = "deadbeef" * 4
    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n",
                                          "channels": [{"idx": 1, "name": "x", "secret": "zz" + secret}]})
    with caplog.at_level("ERROR"):
        asyncio.run(pub.on_companion_connected())
    assert "rejected" in caplog.text
    assert secret not in caplog.text


def test_2g_reader_death_wakes_pending_command():
    """A command awaiting a reply must fail fast when the reader dies, not
    burn its full timeout (second audit #7)."""
    client = CompanionClient("127.0.0.1", 1)

    async def fake_write(payload): pass
    client._write = fake_write

    async def run():
        async def die_soon():
            await asyncio.sleep(0.05)
            await client._reader_died()
        asyncio.create_task(die_soon())
        t0 = time.monotonic()
        with pytest.raises(ConnectionError):
            await client._command(b"\x04", timeout=10)
        return time.monotonic() - t0

    assert asyncio.run(run()) < 1.0


def test_clamp_text_strict_drops_undecodable():
    assert clamp_text(b"\xff\xfe hello") is None
    assert clamp_text("héllo".encode()) == "héllo"



# ---------------------------------------------------------------- third audit


def test_3f1_sentinel_stays_armed_for_every_later_command():
    """After the reader dies, the second and third commands must fail as fast
    as the first, not burn their full timeouts (third audit F1)."""
    client = CompanionClient("127.0.0.1", 1)

    async def fake_write(payload): pass
    client._write = fake_write

    async def run():
        await client._reader_died()
        t0 = time.monotonic()
        for _ in range(3):
            with pytest.raises(ConnectionError):
                await client._command(b"\x04", timeout=10)
        return time.monotonic() - t0

    assert asyncio.run(run()) < 1.0


def test_3f2_contact_walk_does_not_dereference_sentinel():
    from openhop_core.companion.constants import RESP_CODE_CONTACTS_START

    client = CompanionClient("127.0.0.1", 1)

    async def fake_write(payload): pass
    client._write = fake_write

    async def run():
        await client._responses.put(bytes([RESP_CODE_CONTACTS_START, 0, 0, 0, 0]))
        await client._reader_died()
        with pytest.raises(ConnectionError):
            await client.get_contacts()

    asyncio.run(run())  # a TypeError here would be the F2 regression



# ---------------------------------------------------------------- device clock


def test_set_device_time_wire_layout():
    from openhop_core.companion.constants import CMD_SET_DEVICE_TIME, RESP_CODE_OK

    client = CompanionClient("127.0.0.1", 1)
    sent = []

    async def fake_write(payload): sent.append(payload)
    client._write = fake_write

    async def run():
        await client._responses.put(bytes([RESP_CODE_OK]))
        return await client.set_device_time(1788917678)

    assert asyncio.run(run()) is True
    frame = sent[0]
    assert frame[0] == CMD_SET_DEVICE_TIME
    assert struct.unpack("<I", frame[1:5])[0] == 1788917678
    assert len(frame) == 5


def test_set_device_time_defaults_to_now(monkeypatch):
    from openhop_core.companion import constants as companion_constants

    import openhop_txmesh.companion as companion_mod

    monkeypatch.setattr(companion_mod.time, "time", lambda: 1700000000.5)

    client = CompanionClient("127.0.0.1", 1)
    sent = []

    async def fake_write(payload): sent.append(payload)
    client._write = fake_write

    async def run():
        await client._responses.put(bytes([companion_constants.RESP_CODE_OK]))
        return await client.set_device_time()

    assert asyncio.run(run()) is True
    assert struct.unpack("<I", sent[0][1:5])[0] == 1700000000


def test_clock_sync_runs_first_on_every_connect():
    """A companion identity has no RTC of its own: without an explicit sync
    on every connect, messages it composes carry a stale/epoch-0 timestamp
    forever, even though the host process already knows the correct time."""
    calls = []

    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True
        async def set_device_time(self): calls.append("time"); return True
        async def set_path_hash_mode(self, b): calls.append("hash"); return True
        async def set_channel(self, idx, name, secret): calls.append("chan"); return True
        async def get_channel(self, idx): return None

    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n",
                                          "channels": [{"idx": 1, "name": "#bot"}]})
    asyncio.run(pub.on_companion_connected())
    assert calls[0] == "time"


def test_clock_sync_failure_is_not_fatal():
    """A fake client that has no set_device_time at all (older test doubles,
    or a companion firmware that predates this command) must not break the
    rest of the connect sequence."""
    calls = []

    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True
        async def set_path_hash_mode(self, b): calls.append("hash"); return True
        async def get_channel(self, idx): return None

    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n"})
    asyncio.run(pub.on_companion_connected())  # must not raise
    assert calls == ["hash"]


# ---------------------------------------------------------------- path hash width


def test_set_path_hash_mode_wire_layout():
    from openhop_core.companion.constants import CMD_SET_PATH_HASH_MODE, RESP_CODE_OK

    client = CompanionClient("127.0.0.1", 1)
    sent = []

    async def fake_write(payload): sent.append(payload)
    client._write = fake_write

    async def run():
        await client._responses.put(bytes([RESP_CODE_OK]))
        return await client.set_path_hash_mode(2)

    assert asyncio.run(run()) is True
    assert sent[0] == bytes([CMD_SET_PATH_HASH_MODE, 0, 1])  # 2-byte == mode 1
    with pytest.raises(ValueError):
        asyncio.run(client.set_path_hash_mode(4))


def test_path_hash_defaults_to_two_bytes_and_precedes_channels():
    calls = []

    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True
        async def set_path_hash_mode(self, b): calls.append(("hash", b)); return True
        async def set_channel(self, idx, name, secret): calls.append(("chan", idx)); return True
        async def get_channel(self, idx): return None

    pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n",
                                          "channels": [{"idx": 1, "name": "#bot"}]})
    asyncio.run(pub.on_companion_connected())
    assert calls[0] == ("hash", 2)      # default, and first
    assert calls[1] == ("chan", 1)


def test_path_hash_bad_config_is_ignored_not_fatal():
    calls = []

    class FakeClient:
        on_connected = on_message = on_advert = on_raw_frame = None
        def is_connected(self): return True
        async def set_path_hash_mode(self, b): calls.append(b); return True
        async def get_channel(self, idx): return None

    for bad in ("abc", 7, None):
        pub = ObserverPublisher(FakeClient(), {"host": "", "username": "u", "node_name": "n", "path_hash_bytes": bad})
        asyncio.run(pub.on_companion_connected())  # must not raise
    assert calls == []



# ---------------------------------------------------------------- keygen


def test_keygen_produces_repeater_compatible_identity():
    """The 64-byte identity_key must load in openhop_core and derive the same
    public key we printed -- otherwise the pasted key is a different node."""
    from openhop_core.protocol import LocalIdentity
    from openhop_txmesh.main import generate_identity

    pub, key = generate_identity()
    assert len(pub) == 32 and len(key) == 64
    assert LocalIdentity(key).get_public_key() == pub
    # A fresh call is a fresh identity.
    assert generate_identity()[0] != pub


def test_keygen_cli(capsys):
    import sys
    from openhop_txmesh.main import main

    sys.argv = ["openhop-txmesh", "keygen", "--json"]
    assert main() == 0
    out = json.loads(capsys.readouterr().out)
    assert len(out["identity_key"]) == 128 and len(out["public_key"]) == 64



# ---------------------------------------------------------------- node_name required


def test_node_name_has_no_default_and_is_required(monkeypatch):
    """A default node_name would put every install on the mesh under the same
    name. Startup must refuse instead."""
    from openhop_txmesh.main import _run, load_config

    for k in ("OPENHOP_TXMESH_NODE_NAME", "OPENHOP_TXMESH_HOST"):
        monkeypatch.delenv(k, raising=False)
    assert load_config(None).get("node_name") == ""
    rc = asyncio.run(_run({"host": "broker.example", "node_name": ""}))
    assert rc == 2
    rc = asyncio.run(_run({"host": "broker.example", "node_name": "   "}))
    assert rc == 2
