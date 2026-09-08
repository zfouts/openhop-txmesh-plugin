"""
Contract tests for the observer MQTT payload/topic builders.

These assert the invariants MeshCore's examples/observer_node/MQTT.md makes
consumers rely on, not merely that the code runs. The ones that matter most:
optional keys are OMITTED rather than null (§4), status is a bare string not
JSON (§7.3), and a send-bridge miss transmits nothing (§6.4).
"""

import time

import pytest

from openhop_txmesh.format import (
    MAX_TEXT_LEN,
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
    contact_type,
    fleet_prefix,
    hops_fields,
    most_recent,
    normalise_advert,
    parse_send_topic,
    pk8,
    resolve_channel,
    resolve_prefix,
    sanitise_segment,
)


# ---------------------------------------------------------------- §3.1 prefix


def test_prefix_precedence_topic_beats_everything():
    broker = {"base_topic": "custom/thing", "iata": "AUS", "username": "mesh"}
    assert resolve_prefix(broker, "Observer-1") == "custom/thing"


def test_prefix_iata_beats_username():
    broker = {"iata": "aus", "username": "mesh"}
    assert resolve_prefix(broker, "Observer-1") == "meshcore/AUS/Observer-1"


def test_prefix_username_is_acl_aligned_default():
    assert resolve_prefix({"username": "obs-zach"}, "txmesh_bot") == "meshcore/obs-zach/txmesh_bot"


def test_prefix_bare_when_nothing_configured():
    assert resolve_prefix({}, "Observer-1") == "meshcore/Observer-1"


@pytest.mark.parametrize(
    "raw,expected",
    [("a#b", "a-b"), ("a+b", "a-b"), ("a/b", "a-b"), ("a b", "a-b"), ("trail/", "trail")],
)
def test_topic_hygiene_on_free_form_segments(raw, expected):
    assert sanitise_segment(raw) == expected


def test_base_topic_override_is_not_sanitised():
    """§3.1: the mqtt.topic override is used exactly as given."""
    assert resolve_prefix({"base_topic": "weird/but+deliberate"}, "n") == "weird/but+deliberate"


def test_fleet_topic_derives_from_username_only():
    assert fleet_prefix({"username": "mesh"}) == "meshcore/mesh/all"
    # §6.3: a node on the per-region layout has no fleet subscription.
    assert fleet_prefix({"username": "mesh", "iata": "AUS"}) is None
    assert fleet_prefix({"base_topic": "x"}) is None
    assert fleet_prefix({}) is None


# ---------------------------------------------------------------- §3.2 / §4


def test_pk8_is_first_four_bytes_lowercase():
    assert pk8("6EF177D4AABBCCDD") == "6ef177d4"
    assert pk8(bytes.fromhex("6ef177d4aabb")) == "6ef177d4"


def test_hops_omitted_when_heard_direct():
    assert hops_fields(None) == {"hops_n": 0}
    assert hops_fields(b"") == {"hops_n": 0}


def test_hops_encodes_travel_order():
    assert hops_fields(b"\xdd\x17") == {"hops_n": 2, "hops": "dd17"}


@pytest.mark.parametrize(
    "raw,expected",
    [
        (2, "repeater"),
        (1, "chat"),
        (3, "room"),
        (4, "sensor"),
        (0, "unknown"),
        ("Repeater", "repeater"),
        ("Room Server", "room"),
        ("nonsense", "unknown"),
    ],
)
def test_contact_type_accepts_both_representations(raw, expected):
    assert contact_type(raw) == expected


# ---------------------------------------------------------------- §4 omission


def test_optional_keys_are_omitted_not_null():
    """The single most load-bearing convention: consumers test for presence."""
    c = build_contact({"pubkey": "aabbccdd", "node_name": "N", "contact_type": 2})
    assert "lat" not in c and "lon" not in c
    assert not any(v is None for v in c.values())


def test_zero_position_is_treated_as_none():
    c = build_contact(
        {
            "pubkey": "aabbccdd",
            "node_name": "N",
            "contact_type": 2,
            "latitude": 0.0,
            "longitude": 0.0,
        }
    )
    assert "lat" not in c and "lon" not in c


def test_position_present_when_adverted():
    c = build_contact(
        {"pubkey": "aabbccdd", "contact_type": 2, "latitude": 30.26715, "longitude": -97.74306}
    )
    assert c["lat"] == pytest.approx(30.26715)
    assert c["lon"] == pytest.approx(-97.74306)


# ---------------------------------------------------------------- §5


def test_telemetry_omits_fields_a_host_cannot_report():
    t = build_telemetry({"uptime_secs": 86400, "packets_received": 10233, "packets_sent": 0})
    assert t["uptime_s"] == 86400
    assert t["rx"] == 10233
    assert t["relayed"] == 0
    # No battery or heap on a Linux/macOS host unless a sensor provides them.
    assert "batt_mv" not in t and "heap" not in t


def test_sensors_empty_dict_distinguishes_no_sensors_from_offline():
    """§5.2 publishes {} deliberately rather than skipping the topic."""
    assert build_sensors([]) == {}
    assert build_sensors(None) == {}


def test_sensors_channel_suffix_prevents_collisions():
    readings = [
        {"ok": True, "data": {"temperature": 24.5}},
        {"ok": True, "data": {"temperature": 31.0}},
    ]
    out = build_sensors(readings)
    assert out == {"temperature1": 24.5, "temperature2": 31.0}


def test_sensors_skips_failed_reads():
    readings = [
        {"ok": False, "data": {"temperature": 1.0}},
        {"ok": True, "data": {"voltage": 4.15}},
    ]
    assert build_sensors(readings) == {"voltage1": 4.15}


def test_sensors_gps_is_an_object():
    out = build_sensors(
        [{"ok": True, "data": {"latitude": 30.2, "longitude": -97.7, "altitude": 149.0}}]
    )
    assert out["gps1"] == {"lat": pytest.approx(30.2), "lon": pytest.approx(-97.7), "alt": 149.0}


def test_heard_carries_ingress_path():
    h = build_heard(
        {
            "pubkey": "349daaf7",
            "node_name": "South-Gate",
            "timestamp": 1784431091,
            "snr": -3.2,
            "path": b"\xdd\x17",
        }
    )
    assert h["hops_n"] == 2 and h["hops"] == "dd17"
    assert h["pubkey"] == "349daaf7"


def test_dm_and_channel_carry_clock_skew():
    now = 1784504032
    dm = build_dm(
        {
            "contact_name": "alice",
            "message_text": "on my way",
            "timestamp": 1784504030,
            "network_info": {"snr": -7.5},
        },
        rx_ts=now,
    )
    assert dm["from"] == "alice"
    assert dm["skew_s"] == 2
    assert dm["rx_ts"] == now


def test_channel_message_embeds_sender_in_text():
    """§5.5: the on-air group format is '<sender>: <message>'."""
    m = build_channel_message(
        {
            "channel_name": "#bot",
            "sender_name": "KJ5DHR",
            "message_text": "!path",
            "timestamp": 1784504030,
            "network_info": {"snr": 11.8},
        },
        rx_ts=1784504032,
    )
    assert m["text"] == "KJ5DHR: !path"
    assert m["channel"] == "#bot"


def test_channel_message_does_not_double_prefix_sender():
    m = build_channel_message(
        {"channel_name": "x", "sender_name": "bob", "message_text": "bob: hi", "timestamp": 1},
        rx_ts=2,
    )
    assert m["text"] == "bob: hi"


def test_packets_uses_collector_key_casing():
    """§5.8 deliberately uses SNR/RSSI, the collector's casing, not ours."""
    p = build_packet("1102DD17", -7.5, -112)
    assert p == {"raw": "1102dd17", "SNR": -7.5, "RSSI": -112}


def test_advert_reports_clock_skew_and_raw():
    a = build_advert(
        {
            "pubkey": "bbc66b12",
            "timestamp": 1784504030,
            "contact_type": 2,
            "node_name": "Valhalla",
            "snr": 7.0,
            "path": b"\xdd",
            "raw": "1102ff",
        },
        rx_ts=1784504032,
    )
    assert a["adv_ts"] == 1784504030
    assert a["skew_s"] == 2
    assert a["type"] == "repeater"
    assert a["raw"] == "1102ff"


def test_normalise_advert_maps_node_discovered_event():
    event = {
        "public_key": "c04fd4f7f34844996eaf015db71180e6",
        "name": "Hilltop",
        "contact_type": 2,
        "lat": 30.2,
        "lon": -97.7,
        "advert_timestamp": 1784504030,
        "timestamp": 1784504032,
        "snr": -3.5,
        "rssi": -110,
        "inbound_path": b"\xdd\x17",
        "raw_advert_packet": b"\x11\x02",
    }
    a = normalise_advert(event)
    assert a["pubkey"] == "c04fd4f7f34844996eaf015db71180e6"
    assert a["timestamp"] == 1784504030  # advertiser's clock
    assert a["rx_ts"] == 1784504032  # ours
    assert a["raw"] == "1102"
    assert build_heard(a)["hops"] == "dd17"


def test_most_recent_dedupes_by_node_newest_heard_first():
    """Ordering keys on rx_ts (when WE heard it), never on the advertiser's
    timestamp, which an RF peer controls (audit M6)."""
    adverts = [
        {"pubkey": "aaaaaaaa", "timestamp": 30, "rx_ts": 10},
        {"pubkey": "aaaaaaaa", "timestamp": 10, "rx_ts": 30},
        {"pubkey": "bbbbbbbb", "timestamp": 99, "rx_ts": 20},
    ]
    out = most_recent(adverts, limit=16)
    assert len(out) == 2
    assert next(a for a in out if pk8(a["pubkey"]) == "aaaaaaaa")["rx_ts"] == 30
    assert pk8(out[0]["pubkey"]) == "aaaaaaaa"  # heard most recently, despite ts=10


# ---------------------------------------------------------------- §6 bridge


def test_parse_send_topic_extracts_channel():
    prefixes = ["meshcore/mesh/Obs-1", "meshcore/mesh/all"]
    assert parse_send_topic("meshcore/mesh/Obs-1/send/bot", prefixes) == "bot"
    assert parse_send_topic("meshcore/mesh/all/send/0", prefixes) == "0"


def test_parse_send_topic_rejects_non_send_and_nested():
    prefixes = ["meshcore/mesh/Obs-1"]
    assert parse_send_topic("meshcore/mesh/Obs-1/telemetry", prefixes) is None
    assert parse_send_topic("meshcore/mesh/Obs-1/send/a/b", prefixes) is None
    assert parse_send_topic("meshcore/other/Obs-1/send/x", prefixes) is None


def test_parse_send_topic_rejects_overlong_token():
    """§6.2: a channel token over 31 chars is rejected, not truncated."""
    assert parse_send_topic("p/send/" + "x" * 32, ["p"]) is None


class _Chan:
    def __init__(self, name):
        self.name = name


def test_resolve_channel_by_slot_index():
    channels = {0: _Chan("Public"), 3: _Chan("#bot")}
    assert resolve_channel("0", channels) == 0
    assert resolve_channel("3", channels) == 3


def test_resolve_channel_unconfigured_slot_is_a_miss():
    assert resolve_channel("7", {0: _Chan("Public")}) is None


def test_resolve_channel_by_name_ignores_leading_hash():
    """§6.1: MQTT topics cannot contain '#', so it is ignored on the stored name."""
    channels = {0: _Chan("Public"), 3: _Chan("#bot")}
    assert resolve_channel("bot", channels) == 3
    assert resolve_channel("BOT", channels) == 3


def test_resolve_channel_name_miss_sends_nothing():
    """§6.4: a miss must transmit nothing rather than post to an arbitrary slot."""
    assert resolve_channel("nosuch", {0: _Chan("Public")}) is None


def test_clamp_text_truncates_and_drops_empty():
    assert clamp_text(b"") is None
    assert clamp_text(b"   ") is None
    assert len(clamp_text(b"x" * 500)) == MAX_TEXT_LEN
    assert clamp_text(b"hello") == "hello"


def test_rate_limiter_enforces_budget_then_recovers():
    rl = RateLimiter(max_per_min=6)
    t = 1000.0
    assert all(rl.allow(t) for _ in range(6))
    assert rl.allow(t) is False  # over budget
    assert rl.allow(t + 61) is True  # next window
