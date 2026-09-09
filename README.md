# openhop-txmesh-plugin

The node side of [txme.sh](https://txme.sh), packaged as an
[openHop](https://openhop.dev) Repeater plugin.

txme.sh is the infrastructure: the website, the dashboards, and the bot
framework that actually runs the bots. This plugin is the piece that runs on
your node and talks to it. It does the job the observer firmware used to do.
A node running it is a gateway on the txme.sh network. It does two things:

It reports. Who is on the mesh, how packets are getting around, what is being
said on the channels it has keys for, and how the node itself is doing. All of
that goes to txme.sh under your own namespace.

It answers. When someone on the mesh sends a `!command`, your gateway hears it
and txme.sh works out the reply. Core commands like `!nws`, `!flood` and
`!mpath` work everywhere. Bots you create on the txme.sh site are yours and
answer through your node. txme.sh publishes the reply to your gateway's
`send/` topic and your node transmits it. The bot logic, cooldowns and
de-duplication all live on the txme.sh backend. The plugin is just the two
ends of the pipe, and the airtime limits below sit on the transmitting end.

The reach comes from the community. Thanks to
[MeshTexas.org](https://meshtexas.org), txme.sh runs on nodes operated by
people all over the network instead of only the ones it could deploy itself.
That is far more coverage than running every node in-house.

It runs on the openHop Repeater you already have, on the same radio, as one
more identity. No dedicated hardware, nothing to flash or keep updated.

| | |
|---|---|
| [docs/PLUGIN-MANAGER.md](docs/PLUGIN-MANAGER.md) | Start here on openHop 1.1.4 or newer. Install and run it from the repeater's Plugins page. |
| [docs/SETUP.md](docs/SETUP.md) | Manual install for older repeaters or a separate machine. Copy-and-paste setup from beginning to end, with what you should see after each step. |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Every setting, its default, and its environment variable. |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | What it publishes, privacy, transmit limits, troubleshooting, and how to shut a node down cleanly. |

## How it fits

openHop can host any number of independent mesh identities on one radio. The
gateway gets its own: a companion, which is a full chat node with its own
keypair, contacts and channel keys. That is what lets it decrypt what it hears
and transmit replies. The repeater identity cannot do either; it forwards
frames it has no keys for.

```
Heltec / modem       one radio
      |
openhop_repeater     owns the radio
      |-- repeater identity        forwards packets
      |-- companion "alice-bot"    :5001   this plugin, under your own name
      `-- (optional) more companions on other ports, for example one for a phone app
```

The plugin is a separate process. It connects to its companion over the
repeater's companion port the same way a phone app does, and touches nothing
inside the repeater. That is why it ships as a wheel against a stock openHop
install. Other nodes see it as an ordinary node under the name you give it;
they cannot tell it shares a radio.

A companion allows one client at a time. While the plugin is connected, a
phone app cannot use that companion. Give the gateway its own.

## Install

This is the short version. [docs/SETUP.md](docs/SETUP.md) has the full
walkthrough.

On openHop Repeater 1.1.4 or newer, use the Plugins page in the web UI:
upload the wheel from the
[latest release](https://github.com/zfouts/openhop-txmesh-plugin/releases),
fill in the settings, and enable it. The plugin manager runs it, restarts it,
keeps its settings, and shows its log. The walkthrough is
[docs/PLUGIN-MANAGER.md](docs/PLUGIN-MANAGER.md). What follows is the manual
install for older repeaters or for running the plugin on another machine.

```bash
python3 -m venv /opt/openhop_txmesh
/opt/openhop_txmesh/bin/pip install openhop-txmesh-plugin
/opt/openhop_txmesh/bin/openhop-txmesh keygen        # prints your node's identity_key
```

Add a companion to the repeater's `config.yaml` and paste in the
`identity_key`. `YOUR-NODE-NAME` is the name the mesh will see. Pick one that
is unique to you, like `alice-bot` or `lhtx-obs-1`, not a generic one.

```yaml
identities:
  companions:
    - name: "YOUR-NODE-NAME"
      identity_key: "PASTE-IDENTITY-KEY-HERE"
      settings:
        node_name: "YOUR-NODE-NAME"
        tcp_port: 5001
        bind_address: "127.0.0.1"
```

Configure the plugin. `channels` is the list of mesh channels it joins. A name
starting with `#` needs no secret because the key comes from the name; any
other channel needs `secret` as hex. Channels are sent to the repeater on every
connect and the repeater saves them. Slot `0` is Public, and an entry with
`idx: 0` replaces its key.

```json
{
  "companion_host": "127.0.0.1",
  "companion_port": 5001,
  "node_name": "YOUR-NODE-NAME",
  "host": "collector.txme.sh",
  "port": 443,
  "transport": "websockets",
  "username": "YOUR-USERNAME",
  "password": "YOUR-PASSWORD",
  "tls_enabled": true,
  "channels": [
    { "idx": 1, "name": "#bot" }
  ]
}
```

Run it: `/opt/openhop_txmesh/bin/openhop-txmesh --config config.json`. Any
setting can also come from the environment as `OPENHOP_TXMESH_<KEY>`, which is
the usual way to keep the password out of a file.

Your node publishes under `meshcore/YOUR-USERNAME/YOUR-NODE-NAME/`. Every
setting is described in [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## What it publishes

| Topic | Retained | Cadence / source |
|---|---|---|
| `status` | yes, plus last-will | the bare string `online` or `offline`. Not JSON, do not parse it |
| `telemetry` | no | every 60 s: uptime, frames heard, last SNR/RSSI, process memory, restart count |
| `sensors` | no | every 60 s, always `{}` (a plugin has no sensor bus; `{}` still says "alive") |
| `contact/<pk8>` | yes | every 300 s: the companion's contact table, published in slices |
| `heard/<pk8>` | yes | every 180 s: the 16 most recently heard nodes, with SNR and hop path |
| `msg/dm`, `msg/channel` | no | as they arrive: every message the companion can read |
| `advert` | no | opt-in: one per advert heard, with the raw frame |
| `packets` | no | opt-in: one per frame heard, by far the highest volume |
| `send/<idx\|name>` | n/a | subscribed. Publish text here and the node transmits it |

`<pk8>` is the first 4 bytes of a node's public key as 8 hex characters.
Optional fields are left out rather than set to null, so test for presence.
Anything the plugin cannot source honestly is left out rather than made up.

### Where the data comes from

Adverts are decoded from raw frames and signature-checked. The companion
protocol's advert push only carries a public key, so `heard/` is built by
decoding the raw frame the radio heard, which also gives the SNR and the hop
path. That frame arrives before the repeater has verified it, so the plugin
checks the Ed25519 signature itself and discards forgeries. Nobody on air can
plant a fake name or position under a real node's key.

`msg/*` carries `hops_n` but not `hops`. The message frame has the hop count,
not the hop hashes. It leaves out `snr` when the frame's SNR byte is zero,
because that is indistinguishable from "not set".

`packets` raw frames are capped at 170 bytes by the companion frame protocol.

`telemetry` maps the embedded-node fields onto the host. `heap` is this
process's resident memory and `boots` is a persistent restart counter.
`batt_mv` is only published if you set `battery_mv`; a virtual node has no
battery.

## The send bridge

Publishing to `<prefix>/send/<channel>` makes the node transmit. Anyone with
write access to your broker namespace is spending your mesh's airtime, so
there are limits:

- 6 messages per minute, fixed window. Over-budget messages wait in a 4-deep
  queue; beyond that they are dropped.
- Text is clamped to 160 bytes on air and never split in the middle of a
  character. The channel token is at most 31 characters.
- Retained `send/` messages are refused. They would re-transmit on every
  reconnect, forever.
- An unknown channel or an empty payload is dropped silently. An error reply
  would itself be airtime.

The fleet topic `meshcore/<username>/all/send/<channel>` reaches every node you
run at once. Secure the broker with authentication and an ACL on `+/send/#`.

## Privacy

This mirrors every readable message, including private channels the companion
has keys for. Decrypted mesh traffic lands on your broker. That is your call
to make when you point a node at one, and if other people share the mesh they
should know an observer is on it. It can only read channels you list in
`channels`.

## Development

```bash
pip install -e ".[dev]"
pytest -q
```

The tests pin the things consumers rely on: optional keys left out rather than
null, `status` as a bare string, a bad `send/` transmitting nothing, the
reconnect ladder's stability guard, forged adverts rejected. Frame layouts are
taken from `openhop_core`'s own encoder rather than inferred.

Verified against an existing observer already publishing to the same broker:
identical schemas on `status`, `contact/` and `heard/`.
