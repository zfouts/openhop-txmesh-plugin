# Setup guide

This takes you from a running openHop Repeater to a node publishing to txme.sh,
one step at a time. Every command is meant to be copied and pasted. Plan on
about 20 minutes.

## Before you start

You need three things.

1. An openHop Repeater, version 0.3.0 or newer, that is already hearing your
   mesh. Open its web UI; if packets are arriving you are good. If you do not
   have one yet, set that up first at [openhop.dev](https://openhop.dev) and
   come back.

2. Your txme.sh login: a username and password for the collector. Your
   username is also your namespace. Everything your node publishes lands under
   `meshcore/YOUR-USERNAME/`.

3. Python 3.10 or newer on the same machine as the repeater. Check with:

   ```bash
   python3 --version
   ```

   You should see `Python 3.10.x` or higher.

### Placeholders used in this guide

Every time you see one of these, replace it with your own value.

| Placeholder | Replace with |
|---|---|
| `YOUR-USERNAME` | your txme.sh username, for example `obs-alice` |
| `YOUR-PASSWORD` | your txme.sh password |
| `PASTE-IDENTITY-KEY-HERE` | the `identity_key` you generate in step 2 |
| `YOUR-NODE-NAME` | a name for your node, see below |

Choosing `YOUR-NODE-NAME`: this is the name every other node on the mesh will
see, so it has to be unique to you. Do not use the examples from this guide.
Pick something that says who or where, and keep it short.

- Good: `alice-bot`, `lhtx-obs-1`, `hillcountry-gw`
- Bad: `txmesh_bot`, `observer`, `bot`. Hundreds of people would pick these.

Letters, digits, `-` and `_`. Keep it under 24 characters. You will type it in
exactly two places, the repeater's `config.yaml` in step 3 and the plugin's
config in step 4, and they must match.

Port `5001` is used throughout. Keep it unless something else is already
using it.

---

## Step 1. Install the plugin

```bash
python3 -m venv /opt/openhop_txmesh
/opt/openhop_txmesh/bin/pip install openhop-txmesh-plugin
```

Check that it installed:

```bash
/opt/openhop_txmesh/bin/openhop-txmesh --help
```

You should see the usage text, including a `keygen` command.

Installing from a downloaded wheel instead? Replace the second line with
`/opt/openhop_txmesh/bin/pip install ./openhop_txmesh_plugin-*.whl`.

Using the openHop plugin manager? Once the plugin is in the openHop catalogue
you can install it from there and skip the systemd part of step 5, because the
plugin manager runs it for you. The rest of this guide is the same.

---

## Step 2. Generate your node's identity

Your node is a real mesh node with its own keypair. This creates it:

```bash
/opt/openhop_txmesh/bin/openhop-txmesh keygen
```

You should see:

```
public_key   3f9c...                                              (64 characters)
identity_key a81e...                                              (128 characters)

Paste identity_key into the repeater's config.yaml under identities.companions.
It is the node's PRIVATE key: keep it, and keep it secret.
```

Copy the `identity_key` line somewhere safe. This is your node's private key.
If you lose it, your node comes back on the mesh as a completely new node with
a new public key, and every other node's memory of it is gone.

---

## Step 3. Add the node to the repeater

### 3a. Find the repeater's config file

| How you installed the repeater | Where `config.yaml` is |
|---|---|
| systemd or the install script | `/etc/openhop_repeater/config.yaml` |
| Docker | inside the config volume. `docker inspect openhop-repeater` shows the mount, usually mapped to `/etc/openhop_repeater` in the container |
| Not sure | `sudo find / -name config.yaml -path '*openhop*' 2>/dev/null` |

### 3b. Add a companion

Open the file and find the `identities:` section. It has a `companions:` list
under it, probably with everything commented out. Add this entry, pasting your
`identity_key` from step 2 where marked:

```yaml
identities:
  companions:
    - name: "YOUR-NODE-NAME"
      identity_key: "PASTE-IDENTITY-KEY-HERE"
      settings:
        node_name: "YOUR-NODE-NAME"
        tcp_port: 5001
        bind_address: "127.0.0.1"
        tcp_timeout: 0
```

If `identities:` or `companions:` already exists, add just the `- name:` block
to the existing list. Indentation matters in YAML: the `-` goes four spaces
in, and everything under it lines up as shown.

What these mean:

- `name` and `node_name` are both `YOUR-NODE-NAME`. `node_name` is what other
  nodes see on the mesh. `name` is just the repeater's label for this entry.
- `tcp_port` is where the plugin connects. Keep `5001` unless it is taken.
- `bind_address: 127.0.0.1` means only this machine can connect. Keep it.
- `tcp_timeout: 0` means the repeater never disconnects the plugin for being
  idle.

### 3c. Restart the repeater

```bash
sudo systemctl restart openhop-repeater
```

Docker: `docker restart openhop-repeater`.

### 3d. Check it came up

```bash
sudo journalctl -u openhop-repeater -n 50 | grep -i "companion frame server"
```

Docker: `docker logs openhop-repeater 2>&1 | grep -i "companion frame server"`.

You should see:

```
Companion frame server listening on 127.0.0.1:5001
```

If you do not, the YAML is probably mis-indented. The repeater log will say so
a few lines above. Fix it and restart again.

One client per companion: while the plugin is connected, a phone app cannot
use this companion. If you also want to chat from your phone, add a second
`- name:` entry with a different `node_name` and `tcp_port` (say `5002`) for
that.

---

## Step 4. Configure the plugin

Create the config directory and file:

```bash
sudo mkdir -p /etc/openhop_txmesh
sudo tee /etc/openhop_txmesh/config.json > /dev/null <<'EOF'
{
  "companion_host": "127.0.0.1",
  "companion_port": 5001,
  "node_name": "YOUR-NODE-NAME",

  "host": "collector.txme.sh",
  "port": 443,
  "transport": "websockets",
  "tls_enabled": true,
  "username": "YOUR-USERNAME",
  "password": "YOUR-PASSWORD",

  "channels": [
    { "idx": 1, "name": "#bot" }
  ]
}
EOF
sudo chmod 600 /etc/openhop_txmesh/config.json
```

Now edit it and replace `YOUR-NODE-NAME`, `YOUR-USERNAME` and
`YOUR-PASSWORD`:

```bash
sudo nano /etc/openhop_txmesh/config.json
```

What the important lines mean:

- `node_name` must be exactly the `node_name` you put in the repeater config
  in step 3, same spelling and same case. It becomes the last part of your
  topics: `meshcore/YOUR-USERNAME/YOUR-NODE-NAME/...`
- `channels` is the list of mesh channels your node joins. `#bot` is where
  bots talk on the txme.sh network. You can add more later; see
  [CONFIGURATION.md](CONFIGURATION.md#channels).

Everything else can stay as it is. Every setting is explained in
[CONFIGURATION.md](CONFIGURATION.md).

---

## Step 5. Run it

### 5a. First run, in the foreground

Run it once by hand so you can watch it start:

```bash
/opt/openhop_txmesh/bin/openhop-txmesh --config /etc/openhop_txmesh/config.json
```

Within a few seconds you should see these lines, in this order:

```
txmesh observer started: companion 127.0.0.1:5001 -> meshcore/YOUR-USERNAME/YOUR-NODE-NAME
Connected to companion 127.0.0.1:5001
path hash: 2-byte -> ok
channel slot 1 = '#bot' -> ok
Observer MQTT connecting to collector.txme.sh:443 (rung 0, ...)
Observer MQTT connected: meshcore/YOUR-USERNAME/YOUR-NODE-NAME
Send bridge subscribed: meshcore/YOUR-USERNAME/YOUR-NODE-NAME/send/+
```

Each line is a milestone:

| Line | Means |
|---|---|
| `Connected to companion` | the repeater accepted the plugin, so step 3 worked |
| `channel slot 1 = '#bot' -> ok` | your node joined `#bot` |
| `Observer MQTT connected` | your login worked and you are publishing, so step 4 worked |

If something is wrong, the line before the first error tells you which step to
go back to. The two most common: `Companion connection lost` means step 3
(repeater not restarted, or wrong port). `Observer MQTT refused: rc=5` means a
wrong username or password in step 4. There is more in
[OPERATIONS.md, Troubleshooting](OPERATIONS.md#troubleshooting).

Stop it with `Ctrl-C` once you have seen `Observer MQTT connected`.

### 5b. Run it as a service

So it starts on boot and restarts if it crashes:

```bash
sudo tee /etc/systemd/system/openhop-txmesh.service > /dev/null <<'EOF'
[Unit]
Description=txme.sh node (openhop-txmesh-plugin)
After=network-online.target openhop-repeater.service
Wants=network-online.target

[Service]
Type=simple
User=repeater
ExecStart=/opt/openhop_txmesh/bin/openhop-txmesh --config /etc/openhop_txmesh/config.json
Restart=on-failure
RestartSec=5
StateDirectory=openhop-txmesh
Environment=XDG_STATE_HOME=/var/lib

[Install]
WantedBy=multi-user.target
EOF
sudo chown repeater:repeater /etc/openhop_txmesh/config.json
sudo systemctl daemon-reload
sudo systemctl enable --now openhop-txmesh
```

`User=repeater` is the user the openHop install script creates. If your
repeater runs as a different user, use that one. It needs to be able to read
the config file.

Check that it is running:

```bash
sudo systemctl status openhop-txmesh --no-pager
sudo journalctl -u openhop-txmesh -f
```

You should see `active (running)` and the same startup lines as in 5a. Press
`Ctrl-C` to stop following the log; the service keeps running.

---

## Step 6. Confirm it is publishing

This checks the whole path, from the radio to txme.sh. The plugin's own Python
already has the MQTT library, so use that:

```bash
/opt/openhop_txmesh/bin/python -c "
import paho.mqtt.client as m, ssl
c = m.Client(transport='websockets', callback_api_version=m.CallbackAPIVersion.VERSION2)
c.username_pw_set('YOUR-USERNAME', 'YOUR-PASSWORD')
c.tls_set(cert_reqs=ssl.CERT_REQUIRED)
c.on_message = lambda c, u, x: print(x.topic.split('/', 3)[-1], '=', x.payload[:100].decode(errors='replace'))
c.connect('collector.txme.sh', 443)
c.subscribe('meshcore/YOUR-USERNAME/YOUR-NODE-NAME/#')
print('listening... (Ctrl-C to stop)')
c.loop_forever()
"
```

Within about a minute you should see something like:

```
listening... (Ctrl-C to stop)
status = online
telemetry = {"uptime_s":62,"rx":14,"relayed":0,"snr":11.5,"rssi":-40,"boot":"plugin-start","boots":1,...}
sensors = {}
```

and, as the mesh talks, lines like:

```
msg/channel = {"channel":"#bot","text":"SomeBot: !ping","hops_n":3,...}
heard/82ab8d34 = {"pubkey":"82ab8d34","name":"Mueller Repeater","snr":12.0,"hops_n":8,"hops":"2818a360b01e17ca",...}
contact/82ab8d34 = {"pubkey":"82ab8d34","name":"Mueller Repeater","type":"repeater","lat":30.29873,"lon":-97.69856,...}
```

What to look for:

- `status = online` means you are connected.
- `rx` in `telemetry` should go up each minute. That means your radio is
  hearing the mesh. If it stays at `0` the plugin is fine but the repeater's
  radio settings do not match your mesh. See
  [OPERATIONS.md](OPERATIONS.md#troubleshooting).
- `heard/` and `contact/` appear as other nodes announce themselves. On a
  quiet mesh this can take a while. That is normal.

---

## Step 7. Test the transmit path (optional)

txme.sh sends bot replies to your node by publishing to
`meshcore/YOUR-USERNAME/YOUR-NODE-NAME/send/CHANNEL`. You can prove that path
works by publishing to it yourself. Use a quiet test channel for this, not
`#bot` or Public. Everyone on the channel hears it.

First add a test channel to `/etc/openhop_txmesh/config.json`:

```json
  "channels": [
    { "idx": 1, "name": "#bot" },
    { "idx": 2, "name": "#test" }
  ]
```

Restart the plugin with `sudo systemctl restart openhop-txmesh`, then:

```bash
/opt/openhop_txmesh/bin/python -c "
import paho.mqtt.client as m, ssl
c = m.Client(transport='websockets', callback_api_version=m.CallbackAPIVersion.VERSION2)
c.username_pw_set('YOUR-USERNAME', 'YOUR-PASSWORD')
c.tls_set(cert_reqs=ssl.CERT_REQUIRED)
c.connect('collector.txme.sh', 443); c.loop_start()
c.publish('meshcore/YOUR-USERNAME/YOUR-NODE-NAME/send/test', 'hello from my node', qos=1).wait_for_publish()
print('sent')
"
```

In the plugin log you should see:

```
send bridge: channel 2 ('test') -> True
```

and the repeater log shows a `TX ... GRP_TXT`. Anyone on `#test` just received
your message.

If nothing appears in the log, the channel name did not match anything in your
`channels` list. That is dropped silently on purpose, because a "no such
channel" reply would itself use airtime.

---

## You're done

Your node is a gateway on the txme.sh network. It reports what it hears, and
txme.sh answers `!commands` through it. To create bots of your own, use the
txme.sh site. Before you go further, read [OPERATIONS.md](OPERATIONS.md). It
covers what gets mirrored (every message your node can read, including any
private channel you add), the transmit limits that protect the mesh, and how to
shut a node down cleanly.
