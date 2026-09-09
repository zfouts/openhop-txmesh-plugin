"""
Plugin entrypoint.

Runs as a supervised child process of the openHop Repeater plugin manager, or
standalone for development. Config is layered, lowest precedence first:

    config.default.json  (shipped)
 -> --config FILE        (operator's; under the plugin manager this defaults to
                          $OPENHOP_PLUGIN_DATA/config.json, which the Plugins
                          page edits)
 -> OPENHOP_TXMESH_*     (environment)

The plugin manager starts the entrypoint with no arguments and sets
OPENHOP_PLUGIN_DATA to the plugin's persistent data directory. That directory
is also the default state_dir, so the boot counter survives restarts.

The MQTT password is deliberately readable from the environment so an operator
can keep it out of a config file the plugin manager may rewrite.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import site
import sys
from pathlib import Path
from typing import Any, Dict

from .companion import CompanionClient
from .publisher import ObserverPublisher

logger = logging.getLogger("txmesh")

PLUGIN_ID = "txmesh.observer"
_ENV_PREFIX = "OPENHOP_TXMESH_"

# Keys that must not be coerced from their string environment form.
_INT_KEYS = {"companion_port", "port", "keepalive"}
# tls_* MUST be here: bool("false") is True, so leaving them out turns an
# operator's explicit TLS_INSECURE=false into insecure=True (audit H1).
_BOOL_KEYS = {"advert_dump", "packets", "enabled", "tls_enabled", "tls_insecure"}


# Where setuptools data-files land, which differs per install layout:
# a venv/system install uses sys.prefix, `pip install --user` uses site.USER_BASE,
# and a source checkout has the file next to the package.
_DATA_REL = Path("share/openhop/plugins") / PLUGIN_ID / "config.default.json"


def _default_paths() -> list[Path]:
    candidates = [Path(__file__).resolve().parent.parent / "config.default.json"]
    for base in (sys.prefix, getattr(sys, "base_prefix", None), site.USER_BASE):
        if base:
            candidates.append(Path(base) / _DATA_REL)
    return candidates


def _defaults() -> Dict[str, Any]:
    """Load the shipped defaults, whichever install layout put them there."""
    for candidate in _default_paths():
        try:
            return json.loads(candidate.read_text())
        except (OSError, ValueError):
            continue
    logger.warning("shipped config.default.json not found; starting from an empty config")
    return {}


_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off")


def _from_env(config: Dict[str, Any]) -> Dict[str, Any]:
    for raw, value in os.environ.items():
        if not raw.startswith(_ENV_PREFIX):
            continue
        key = raw[len(_ENV_PREFIX) :].lower()
        if key in _INT_KEYS:
            with contextlib.suppress(ValueError):
                config[key] = int(value)
        elif key in _BOOL_KEYS:
            # An empty or unrecognised value is treated as UNSET, not False: a
            # templated `TLS_ENABLED=${UNSET}` must not silently downgrade the
            # connection to plaintext (second audit #4). Ints already behave
            # this way; bools were the odd one out.
            v = value.strip().lower()
            if v in _TRUE:
                config[key] = True
            elif v in _FALSE:
                config[key] = False
            else:
                logger.warning("ignoring %s=%r: not a recognised boolean", raw, value)
        else:
            config[key] = value
    return config


def _fold_tls(layer: Dict[str, Any], tls: Dict[str, Any]) -> Dict[str, Any]:
    """Merge one config layer's TLS settings into ``tls``, nested then flat.

    Applied per layer (defaults, file, env) so the shipped flat defaults can no
    longer clobber a nested ``tls`` dict from the operator's file, which is
    what left second-audit #3 only partially closed.
    """
    nested = layer.get("tls")
    if isinstance(nested, dict):
        tls.update({k: v for k, v in nested.items() if k in ("enabled", "insecure")})
    elif nested is not None:
        logger.warning("ignoring tls=%r: expected a mapping", nested)
    if "tls_enabled" in layer:
        tls["enabled"] = bool(layer["tls_enabled"])
    if "tls_insecure" in layer:
        tls["insecure"] = bool(layer["tls_insecure"])
    return tls


def _plugin_data_dir() -> Path | None:
    """The plugin manager's per-plugin data directory, if we run under it."""
    data = os.environ.get("OPENHOP_PLUGIN_DATA", "").strip()
    return Path(data).expanduser() if data else None


def _manager_config_path() -> str | None:
    """$OPENHOP_PLUGIN_DATA/config.json when the manager has written one."""
    data = _plugin_data_dir()
    if data is None:
        return None
    candidate = data / "config.json"
    return str(candidate) if candidate.is_file() else None


def load_config(path: str | None) -> Dict[str, Any]:
    """Layer defaults -> file -> env, folding TLS settings at each layer.

    With no explicit path, the plugin manager's config.json is used when
    present, so the settings saved from the repeater's Plugins page apply.
    """
    tls: Dict[str, Any] = {"enabled": True, "insecure": False}

    config = _defaults()
    _fold_tls(config, tls)

    if path is None:
        path = _manager_config_path()
        if path:
            logger.info("using plugin manager config %s", path)

    if path:
        try:
            layer = json.loads(Path(path).read_text())
            if not isinstance(layer, dict):
                raise ValueError("top level must be an object")
            _fold_tls(layer, tls)
            config.update(layer)
        except (OSError, ValueError) as exc:
            logger.error("could not read %s: %s", path, exc)

    env_layer = _from_env({})
    _fold_tls(env_layer, tls)
    config.update(env_layer)

    data_dir = _plugin_data_dir()
    if not config.get("state_dir") and data_dir is not None:
        config["state_dir"] = str(data_dir)

    config["tls"] = tls
    return config


def generate_identity() -> tuple[bytes, bytes]:
    """Generate a MeshCore-compatible Ed25519 keypair.

    Returns (public_key[32], identity_key[64]). The 64-byte form is what the
    repeater's `identity_key` expects: the clamped scalar followed by the
    upper half of sha512(seed) -- the same construction MeshCore firmware and
    the repeater's own keygen use, so the key is portable between them.
    """
    import hashlib
    import secrets

    from nacl.bindings import crypto_scalarmult_ed25519_base_noclamp

    seed = secrets.token_bytes(32)
    digest = hashlib.sha512(seed).digest()
    scalar = bytearray(digest[:32])
    scalar[0] &= 248
    scalar[31] &= 63
    scalar[31] |= 64
    public_key = crypto_scalarmult_ed25519_base_noclamp(bytes(scalar))
    return public_key, bytes(scalar) + digest[32:64]


def _cmd_keygen(args) -> int:
    pub, key = generate_identity()
    if args.json:
        print(json.dumps({"public_key": pub.hex(), "identity_key": key.hex()}, indent=2))
        return 0
    print("public_key   " + pub.hex())
    print("identity_key " + key.hex())
    print()
    print("Paste identity_key into the repeater's config.yaml under identities.companions.")
    print("It is the node's PRIVATE key: keep it, and keep it secret.")
    return 0


async def _run(config: Dict[str, Any]) -> int:
    host = config.get("companion_host", "127.0.0.1")
    port = int(config.get("companion_port", 5001))

    if not config.get("host"):
        logger.error("no MQTT host configured; nothing to do")
        return 2
    # No default on purpose: a default would put every install on the mesh
    # under the same name. It must match the companion's node_name.
    if not str(config.get("node_name") or "").strip():
        logger.error(
            "node_name is not set. Pick a unique name for your node (the same one you "
            "gave the companion in the repeater's config.yaml) and set it in the config."
        )
        return 2

    client = CompanionClient(host, port)
    publisher = ObserverPublisher(client, config)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    tasks = [
        asyncio.create_task(client.run(), name="companion"),
        asyncio.create_task(publisher.run(), name="publisher"),
    ]
    logger.info(
        "txmesh observer started: companion %s:%s -> %s", host, port, publisher.mqtt.prefix
    )

    await stop.wait()
    logger.info("shutting down")

    # Publisher first: it publishes a retained `offline`, and a clean shutdown
    # never fires the last will.
    await publisher.stop()
    await client.stop()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="openhop-txmesh", description=__doc__)
    parser.add_argument("--config", help="path to plugin config JSON")
    parser.add_argument("--log-level", default=None)
    sub = parser.add_subparsers(dest="command")
    kg = sub.add_parser("keygen", help="generate a companion identity key for the repeater's config.yaml")
    kg.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args()

    if args.command == "keygen":
        return _cmd_keygen(args)

    config = load_config(args.config)
    logging.basicConfig(
        level=(args.log_level or config.get("log_level", "INFO")).upper(),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    try:
        return asyncio.run(_run(config))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
