"""
The observer companion's own MQTT client.

Deliberately separate from ``repeater/data_acquisition/mqtt_handler.py``. That
handler speaks the MC2MQTT family (``meshcore/{IATA}/{PUBKEY}/...``, JSON
status, no last-will, no inbound subscription) and fans one payload out to
every broker. The observer contract disagrees on all four points, so sharing
the client would mean publishing malformed data into an MC2MQTT namespace.

Implements MQTT.md §2: transport, auth, the reconnect ladder with its
stability guard and circuit breaker, the last-will, and the on-connect
sequence. Everything here is transport bookkeeping -- payload shapes live in
``format.py`` and the mesh-facing behaviour in ``publisher.py``.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, List, Mapping, Optional

import paho.mqtt.client as mqtt

from .format import fleet_prefix, resolve_prefix

logger = logging.getLogger("ObserverMqtt")

# §2.4 reconnect ladder. One rung per failed attempt.
_LADDER = (10, 30, 60, 120, 300)

# CONNACK alone proves the handshake worked, not that the link is usable. The
# ladder resets only after a connection has held this long, so a session that
# dies inside one keepalive keeps its earned rung instead of hammering TLS.
_STABLE_RESET_S = 120

# After this many failures at the top rung (~15 min) routine retries stop and
# the bridge is probed once every _BREAKER_PROBE_S instead.
_MAX_FAILS_AT_TOP = 3
_BREAKER_PROBE_S = 1800


class ObserverMqttClient:
    """One MQTT connection speaking the observer contract.

    Args:
        config: the ``observer:`` config block (host, port, transport, tls,
            username, password, and optionally base_topic / iata).
        node_name: trailing topic segment -- the companion's name.
        on_send: called with ``(topic, payload_bytes)`` for every inbound
            message on a subscribed ``send/+`` topic.
    """

    def __init__(
        self,
        config: Mapping,
        node_name: str,
        on_send: Optional[Callable[[str, bytes], None]] = None,
    ):
        self.config = config
        self.node_name = node_name
        self.on_send = on_send

        self.prefix = resolve_prefix(config, node_name)
        self.fleet = fleet_prefix(config)

        self.host = config.get("host", "")
        self.port = int(config.get("port", 443))
        self.transport = config.get("transport", "websockets")
        # Default ON. If the shipped defaults are missing and the operator's
        # config omits tls_*, an empty dict here silently sent credentials in
        # plaintext (audit L5). Opting OUT of TLS must be explicit.
        self.tls = config.get("tls") or {"enabled": True, "insecure": False}

        # Keepalive is sized against the ingress, not the broker: Cloudflare and
        # most HTTPS proxies close an idle WebSocket at ~100 s and that timeout
        # is not configurable, so the PINGREQ has to sit safely under it.
        self.keepalive = int(config.get("keepalive", 45))

        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._connect_time = 0.0
        self._rung = 0
        self._fails_at_top = 0
        self._breaker_tripped = False
        self._next_attempt = 0.0
        self._last_error = ""

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ----------------------------------------------------------------

    def _build(self) -> mqtt.Client:
        """Build a fresh client. Called on first connect and on every re-init."""
        kwargs = {
            "client_id": f"observer_{self.node_name}_{int(time.time())}",
            "transport": "websockets" if self.transport == "websockets" else "tcp",
        }
        api = getattr(mqtt, "CallbackAPIVersion", None)
        if api is not None and hasattr(api, "VERSION2"):
            kwargs["callback_api_version"] = api.VERSION2
        # Our _loop owns every retry. paho's own reconnect must never run
        # underneath the ladder -- it did on a TCP drop before CONNACK, where
        # _on_disconnect's not-yet-connected early return left it going
        # (second audit #5).
        kwargs["reconnect_on_failure"] = False
        try:
            client = mqtt.Client(**kwargs)
        except TypeError:
            kwargs.pop("callback_api_version", None)
            client = mqtt.Client(**kwargs)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        username = self.config.get("username")
        password = self.config.get("password")
        if username:
            client.username_pw_set(username=username, password=password)

        if self.tls.get("enabled", False):
            import ssl

            client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
            # paho's tls_insecure_set(True) disables HOSTNAME checking only;
            # the chain is still verified against the system roots
            # (CERT_REQUIRED). It does not enable a private CA -- there is no
            # ca_certs option here -- it only tolerates a name mismatch, e.g.
            # a broker reached by IP. Still weakens MITM protection.
            if self.tls.get("insecure", False):
                client.tls_insecure_set(True)

        # §2.4 last will: retained `offline`, so subscribers see liveness with
        # no polling -- including after an ungraceful death. Must be armed
        # before connect(); paho sends the will inside the CONNECT packet.
        client.will_set(f"{self.prefix}/status", "offline", qos=1, retain=True)
        return client

    # ----------------------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        if not self.host:
            logger.info("Observer MQTT disabled (no host configured)")
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="observer-mqtt", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Publish a graceful `offline` and tear the connection down.

        A clean shutdown never fires the LWT, so without this a deliberately
        stopped node reads as online on the broker forever.
        """
        self._stop.set()
        with self._lock:
            client, connected = self._client, self._connected
        if client is not None and connected:
            try:
                client.publish(
                    f"{self.prefix}/status", "offline", qos=1, retain=True
                ).wait_for_publish(timeout=2)
            except Exception as exc:
                logger.debug(f"offline publish on shutdown failed: {exc}")
        if client is not None:
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    # ----------------------------------------------------------------

    def _loop(self) -> None:
        """Own the retry schedule. paho's auto-reconnect is not used: it has no
        concept of the stability guard or the breaker."""
        while not self._stop.is_set():
            with self._lock:
                connected = self._connected
            if not connected and time.monotonic() >= self._next_attempt:
                self._attempt()
            self._stop.wait(1.0)

    def _attempt(self) -> None:
        delay = _LADDER[min(self._rung, len(_LADDER) - 1)]
        if self._breaker_tripped:
            delay = _BREAKER_PROBE_S

        try:
            # At the top rung (and past the breaker) a retry is a full client
            # re-init rather than a reconnect on a client whose state may be
            # the thing that's wrong.
            if self._client is None or self._rung >= len(_LADDER) - 1:
                if self._client is not None:
                    try:
                        self._client.loop_stop()
                        self._client.disconnect()
                    except Exception:
                        pass
                self._client = self._build()

            logger.info(
                f"Observer MQTT connecting to {self.host}:{self.port} "
                f"(rung {self._rung}, prefix {self.prefix})"
            )
            self._client.connect(self.host, self.port, keepalive=self.keepalive)
            # Push the next attempt out BEFORE the network thread starts: on a
            # fast link CONNACK (and _on_connect's ladder scheduling) can land
            # before this line, and writing it after loop_start() overwrote the
            # ladder's delay (second audit #5). Without it at all, _loop saw
            # _connected=False 1s later and redialled every second (audit M4).
            self._next_attempt = time.monotonic() + max(10.0, self.keepalive)
            self._client.loop_start()
        except Exception as exc:
            self._last_error = str(exc)
            self._climb()
            logger.warning(f"Observer MQTT connect failed ({exc}); next attempt in {delay}s")
            self._next_attempt = time.monotonic() + delay

    def _climb(self) -> None:
        if self._rung >= len(_LADDER) - 1:
            self._fails_at_top += 1
            if self._fails_at_top >= _MAX_FAILS_AT_TOP and not self._breaker_tripped:
                self._breaker_tripped = True
                logger.error(
                    f"Observer MQTT breaker tripped after {self._fails_at_top} failures "
                    f"at the top rung; probing every {_BREAKER_PROBE_S // 60} min"
                )
        else:
            self._rung += 1

    # ----------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        rc_value = int(getattr(rc, "value", rc)) if rc is not None else -1
        if rc_value != 0:
            self._last_error = f"CONNACK rc={rc_value}"
            self._climb()
            delay = _BREAKER_PROBE_S if self._breaker_tripped else _LADDER[min(self._rung, len(_LADDER) - 1)]
            self._next_attempt = time.monotonic() + delay
            logger.error(f"Observer MQTT refused: rc={rc_value}")
            # Without this paho's loop keeps reconnecting on its own 1/2/4/8s
            # schedule underneath our ladder, so a wrong password hammered the
            # broker four times in 12s while the ladder climbed to the top
            # (audit M3). Our _loop owns retries; paho's thread must not.
            try:
                client.loop_stop()
            except Exception:
                pass
            return

        with self._lock:
            self._connected = True
            self._connect_time = time.monotonic()
        self._breaker_tripped = False
        self._fails_at_top = 0
        self._last_error = ""
        logger.info(f"Observer MQTT connected: {self.prefix}")

        # §2.5 on-connect sequence, in order.
        try:
            # 1. `online`, QoS 1, retained -- pairs with the LWT. Published as a
            #    BARE STRING, not JSON: §7.3 requires consumers to special-case
            #    this topic and NOT JSON.parse it.
            client.publish(f"{self.prefix}/status", "online", qos=1, retain=True)

            # 2. per-node send bridge
            client.subscribe(f"{self.prefix}/send/+", qos=0)
            logger.info(f"Send bridge subscribed: {self.prefix}/send/+")

            # 3. fleet send bridge, skipped when it would duplicate the node prefix
            if self.fleet and self.fleet != self.prefix:
                client.subscribe(f"{self.fleet}/send/+", qos=0)
                logger.info(f"Fleet send subscribed: {self.fleet}/send/+")
        except Exception as exc:
            logger.error(f"on-connect sequence failed: {exc}", exc_info=True)

    def _on_disconnect(self, client, userdata, rc, *extra):
        if not isinstance(rc, (int, float)) and extra:
            rc = extra[0]
        rc_value = int(getattr(rc, "value", rc)) if rc is not None else -1

        with self._lock:
            was_connected = self._connected
            held = time.monotonic() - self._connect_time
            self._connected = False

        if not was_connected or self._stop.is_set():
            return  # deliberate shutdown: no retry, no spurious "retry in 10s"

        try:
            client.loop_stop()
        except Exception:
            pass

        # The ladder resets only after a connection has held long enough to
        # prove the link is actually usable.
        if held >= _STABLE_RESET_S:
            self._rung = 0
            self._fails_at_top = 0
        else:
            self._climb()

        delay = _LADDER[min(self._rung, len(_LADDER) - 1)]
        self._next_attempt = time.monotonic() + delay
        logger.warning(
            f"Observer MQTT disconnected (rc={rc_value}, held {held:.0f}s); retry in {delay}s"
        )

    def _on_message(self, client, userdata, msg):
        """Inbound send-bridge message. Parses nothing -- hands straight to the
        publisher, which queues it for the mesh context (§6.5)."""
        if self.on_send is None:
            return
        # A retained send/ message would be re-delivered on every SUBSCRIBE, i.e.
        # every reconnect -- transmitting it on air forever, long after whoever
        # published it lost broker access (audit M1). The bridge is an event
        # stream; retained state is never a legitimate send.
        if getattr(msg, "retain", False):
            logger.warning("ignoring retained message on %s", msg.topic)
            return
        try:
            self.on_send(msg.topic, msg.payload)
        except Exception as exc:
            logger.error(f"inbound send handler failed: {exc}", exc_info=True)

    # ----------------------------------------------------------------

    def send_prefixes(self) -> List[str]:
        """Prefixes whose ``send/+`` we are subscribed to (§6)."""
        prefixes = [self.prefix]
        if self.fleet and self.fleet != self.prefix:
            prefixes.append(self.fleet)
        return prefixes

    def is_connected(self) -> bool:
        with self._lock:
            return self._connected

    def status(self) -> str:
        """Mirrors the firmware's `get mqtt` string, for the web UI / logs."""
        if not self.host:
            return "off"
        if self.is_connected():
            return "connected"
        if self._breaker_tripped:
            return f"breaker {self._last_error}".strip()
        return f"connecting r{self._rung} {self._last_error}".strip()

    def publish(self, subtopic: str, payload: str, retain: bool = False, qos: int = 0):
        """Publish one payload under our prefix. Never raises."""
        with self._lock:
            client, connected = self._client, self._connected
        if client is None or not connected:
            return None
        try:
            return client.publish(f"{self.prefix}/{subtopic}", payload, qos=qos, retain=retain)
        except Exception as exc:
            logger.debug(f"publish to {subtopic} failed: {exc}")
            return None
