"""
Behaviour tests for the observer MQTT client and the send bridge.

Covers the parts of MQTT.md that are about *semantics on the wire* rather than
payload shape: the retained last-will pairing, the on-connect sequence and its
bare-string status, and the airtime protections on the inbound bridge.
"""

import asyncio
import threading
import time

import pytest

from openhop_txmesh.mqtt import ObserverMqttClient


class FakeClient:
    """Records what a paho client would have been told to do."""

    def __init__(self):
        self.will = None
        self.published = []
        self.subscribed = []
        self.username = None
        self.tls = False
        self.on_connect = self.on_disconnect = self.on_message = None

    def will_set(self, topic, payload, qos=0, retain=False):
        self.will = (topic, payload, qos, retain)

    def username_pw_set(self, username=None, password=None):
        self.username = username

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))
        return _Result()

    def subscribe(self, topic, qos=0):
        self.subscribed.append(topic)

    def connect(self, *a, **kw):
        pass

    def loop_start(self):
        pass

    def loop_stop(self):
        pass

    def disconnect(self):
        pass


class _Result:
    def wait_for_publish(self, timeout=None):
        return True


@pytest.fixture
def client(monkeypatch):
    """An ObserverMqttClient whose paho Client is a FakeClient.

    Patches the Client *constructor* rather than _build(), so the real _build()
    runs -- it is what arms the last will, and stubbing it would test nothing.
    """
    import openhop_txmesh.mqtt as mqtt_mod

    cfg = {
        "host": "collector.example",
        "port": 443,
        "transport": "websockets",
        "username": "obs-zach",
        "password": "secret",
        "tls": {"enabled": False},
    }
    monkeypatch.setattr(mqtt_mod.mqtt, "Client", lambda **kw: FakeClient())
    c = ObserverMqttClient(cfg, "txmesh_bot")
    fake = c._build()
    c._client = fake
    return c, fake


# ---------------------------------------------------------------- §2.4 LWT


def test_last_will_is_retained_offline_on_status(client):
    c, fake = client
    assert fake.will == (f"{c.prefix}/status", "offline", 1, True)


def test_prefix_is_user_scoped(client):
    c, _ = client
    assert c.prefix == "meshcore/obs-zach/txmesh_bot"
    assert c.fleet == "meshcore/obs-zach/all"


# ---------------------------------------------------------------- §2.5


def test_on_connect_publishes_bare_string_online(client):
    """§7.3: status is a bare string. A consumer must not JSON.parse it."""
    c, fake = client
    c._on_connect(fake, None, None, 0)

    topic, payload, qos, retain = fake.published[0]
    assert topic == f"{c.prefix}/status"
    assert payload == "online"  # not '"online"', not '{"status": ...}'
    assert (qos, retain) == (1, True)


def test_on_connect_subscribes_node_and_fleet_send(client):
    c, fake = client
    c._on_connect(fake, None, None, 0)
    assert f"{c.prefix}/send/+" in fake.subscribed
    assert f"{c.fleet}/send/+" in fake.subscribed


def test_fleet_send_skipped_on_region_layout():
    """§6.3: an iata-scoped node builds no fleet subscription."""
    cfg = {"host": "h", "iata": "AUS", "username": "mesh", "tls": {}}
    c = ObserverMqttClient(cfg, "Obs-1")
    fake = FakeClient()
    c._on_connect(fake, None, None, 0)
    assert c.fleet is None
    assert fake.subscribed == [f"{c.prefix}/send/+"]


def test_refused_connack_climbs_the_ladder(client):
    c, fake = client
    assert c._rung == 0
    c._on_connect(fake, None, None, 5)  # not authorized
    assert c._rung == 1
    assert not c.is_connected()


# ---------------------------------------------------------------- §2.4 ladder


def test_short_session_keeps_its_rung(client):
    """CONNACK alone doesn't prove the link is usable; a session that dies
    inside one keepalive must not reset the backoff."""
    c, fake = client
    c._rung = 2
    c._on_connect(fake, None, None, 0)
    c._connect_time = time.monotonic() - 5  # held only 5s
    c._on_disconnect(fake, None, 1)
    assert c._rung == 3  # climbed, not reset


def test_stable_session_resets_the_ladder(client):
    c, fake = client
    c._rung = 3
    c._on_connect(fake, None, None, 0)
    c._connect_time = time.monotonic() - 300  # held 5 min
    c._on_disconnect(fake, None, 1)
    assert c._rung == 0


def test_breaker_trips_after_repeated_top_rung_failures(client):
    c, _ = client
    from openhop_txmesh.mqtt import _LADDER

    c._rung = len(_LADDER) - 1
    for _ in range(3):
        c._climb()
    assert c._breaker_tripped is True


def test_publish_is_a_noop_while_disconnected(client):
    c, fake = client
    c._connected = False
    assert c.publish("telemetry", "{}") is None
    assert fake.published == []


def test_publish_prefixes_the_subtopic(client):
    c, fake = client
    c._connected = True
    c.publish("telemetry", '{"rx":1}')
    assert fake.published[-1][0] == f"{c.prefix}/telemetry"
