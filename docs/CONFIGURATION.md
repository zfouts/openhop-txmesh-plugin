# Configuration reference

## Where settings come from

Settings are read in this order. Later ones win, key by key.

1. Built-in defaults, shipped inside the plugin.
2. Your config file: `openhop-txmesh --config /path/to/config.json`.
3. Environment variables: `OPENHOP_TXMESH_<KEY>`, with the key name in upper
   case. For example, `OPENHOP_TXMESH_PASSWORD=secret` sets `password`.

A minimal file plus one environment variable for the password is a common
setup.

## A complete example

Every setting, with the value it takes if you leave it out. Copy this and
delete what you do not need.

```json
{
  "companion_host": "127.0.0.1",
  "companion_port": 5001,
  "node_name": "YOUR-NODE-NAME",
  "path_hash_bytes": 2,
  "state_dir": "",

  "host": "collector.txme.sh",
  "port": 443,
  "transport": "websockets",
  "username": "YOUR-USERNAME",
  "password": "YOUR-PASSWORD",
  "keepalive": 45,
  "tls_enabled": true,
  "tls_insecure": false,

  "channels": [
    { "idx": 1, "name": "#bot" }
  ],

  "battery_mv": null,
  "advert_dump": false,
  "packets": false,
  "log_level": "INFO"
}
```

## The minimum that works

```json
{
  "node_name": "YOUR-NODE-NAME",
  "host": "collector.txme.sh",
  "username": "YOUR-USERNAME",
  "password": "YOUR-PASSWORD",
  "channels": [{ "idx": 1, "name": "#bot" }]
}
```

Everything else takes its default.

---

## Connecting to the repeater

| Key | Default | Env var | What it is |
|---|---|---|---|
| `companion_host` | `127.0.0.1` | `OPENHOP_TXMESH_COMPANION_HOST` | Where the repeater is. Leave it alone when the plugin runs on the same machine, which it should. |
| `companion_port` | `5001` | `OPENHOP_TXMESH_COMPANION_PORT` | The `tcp_port` you gave the companion in the repeater's `config.yaml`. |
| `node_name` | none, required | `OPENHOP_TXMESH_NODE_NAME` | The name other nodes see on the mesh. It has to be unique to you, and it has to match the companion's `node_name` in the repeater's `config.yaml`. It is the last part of every topic your node publishes. The plugin refuses to start without it. |
| `path_hash_bytes` | `2` | `OPENHOP_TXMESH_PATH_HASH_BYTES` | How many bytes identify each hop in paths your node builds: `1`, `2` or `3`. `2` avoids collisions on a busy mesh. You almost never need to change this. |
| `state_dir` | `""` | `OPENHOP_TXMESH_STATE_DIR` | Where the restart counter is stored. Empty means `~/.local/state/openhop-txmesh`, or `$XDG_STATE_HOME/openhop-txmesh` if that is set. Under systemd, use `StateDirectory=` in the unit instead. See [SETUP.md](SETUP.md#5b-run-it-as-a-service). |

### `channels`

The mesh channels your node joins. It can only read, and send on, channels in
this list.

```json
"channels": [
  { "idx": 1, "name": "#bot" },
  { "idx": 2, "name": "#test" },
  { "idx": 3, "name": "our-private-channel", "secret": "0123456789abcdef0123456789abcdef" }
]
```

| Field | What to put |
|---|---|
| `idx` | A slot number from `1` to `39`. Each channel needs its own. Do not use `0`. That is the Public channel, and an entry with `idx: 0` would replace its key. |
| `name` | The channel name, up to 32 characters. |
| `secret` | Leave it out for names starting with `#`. Those are public hashtag channels and the key comes from the name itself, which is how any node can join `#bot` without being given anything. For a private channel, put its key here as 32 hex characters. |

Channels are sent to the repeater every time the plugin connects and are saved
by the repeater, so they survive restarts on either side. If an entry is
rejected, the log names it by `idx` and `name` only. The secret is never
logged.

---

## Connecting to the collector

| Key | Default | Env var | What it is |
|---|---|---|---|
| `host` | `""` | `OPENHOP_TXMESH_HOST` | The collector's hostname: `collector.txme.sh`. If this is empty the plugin exits with `no MQTT host configured`. |
| `port` | `443` | `OPENHOP_TXMESH_PORT` | `443` for txme.sh. |
| `transport` | `websockets` | `OPENHOP_TXMESH_TRANSPORT` | `websockets` for txme.sh. `tcp` is only for a plain MQTT broker you run yourself. |
| `username` | `""` | `OPENHOP_TXMESH_USERNAME` | Your txme.sh username. It is also your namespace: your node publishes under `meshcore/USERNAME/NODE_NAME/`. |
| `password` | `""` | `OPENHOP_TXMESH_PASSWORD` | Your txme.sh password. Never written to the log. Use the env var if other tools read the config file. |
| `keepalive` | `45` | `OPENHOP_TXMESH_KEEPALIVE` | Seconds between keep-alive pings. `45` is tuned for HTTPS proxies that close idle connections at about 100 s. Leave it. |
| `tls_enabled` | `true` | `OPENHOP_TXMESH_TLS_ENABLED` | Encrypt the connection. Always on for txme.sh. Turning it off has to be explicit. |
| `tls_insecure` | `false` | `OPENHOP_TXMESH_TLS_INSECURE` | Skip the hostname check on the certificate. The certificate itself is still verified. Only useful for reaching a broker by IP address. Not needed for txme.sh. |

### Advanced: topic prefix

You do not need these for txme.sh. They exist for other collectors.

| Key | Env var | What it does |
|---|---|---|
| `base_topic` | `OPENHOP_TXMESH_BASE_TOPIC` | Use this exact string as the topic prefix instead of building one. Used as-is, with no character cleanup. |
| `iata` | `OPENHOP_TXMESH_IATA` | A 3-letter region code. The prefix becomes `meshcore/IATA/NODE_NAME` instead of `meshcore/USERNAME/NODE_NAME`. This also disables the fleet send topic. |

The prefix is chosen by the first of these that is set:

1. `base_topic`, used verbatim
2. `iata`, giving `meshcore/IATA/NODE_NAME`
3. `username`, giving `meshcore/USERNAME/NODE_NAME`. This is the txme.sh case.
4. none of them, giving `meshcore/NODE_NAME`

In `USERNAME` and `NODE_NAME`, the characters `#`, `+`, `/` and space become
`-`.

---

## Telemetry

| Key | Default | Env var | What it is |
|---|---|---|---|
| `battery_mv` | unset | `OPENHOP_TXMESH_BATTERY_MV` | Your node has no battery, so by default no battery fields are published. If a dashboard insists on one, set a number here, for example `4600`, and it is published as `batt_mv` with `batt_pct: 100`. |

Two telemetry fields are always published and are real: `heap`, the memory
used by the plugin process in bytes, and `boots`, how many times the plugin
has started.

---

## Extra feeds, off by default

| Key | Default | Env var | What it is |
|---|---|---|---|
| `advert_dump` | `false` | `OPENHOP_TXMESH_ADVERT_DUMP` | Publish every node announcement your radio hears, including the raw bytes. Useful for diagnosing nodes with a wrong clock. |
| `packets` | `false` | `OPENHOP_TXMESH_PACKETS` | Publish every single frame your radio hears. This is by far the most traffic the plugin can generate. Turn it on only if you know you want it. |

---

## Logging

| Key | Default | Env var / flag |
|---|---|---|
| `log_level` | `INFO` | `OPENHOP_TXMESH_LOG_LEVEL`, or `--log-level DEBUG` on the command line |

`DEBUG` shows every dropped send-bridge message and why it was dropped.

---

## Notes on values

True/false in environment variables, for `TLS_ENABLED`, `TLS_INSECURE`,
`ADVERT_DUMP` and `PACKETS`:

- true: `1`, `true`, `yes`, `on`
- false: `0`, `false`, `no`, `off`
- anything else, including an empty value, is ignored with a warning and the
  default applies. An accidentally empty `TLS_ENABLED` can never switch
  encryption off.

True/false in the JSON file: use real JSON booleans, `true` or `false` without
quotes. `"false"` in quotes is a string, and strings count as true.
