# Operating your node

## Quick health check

Three commands, in order. If all three look right, the node is fine.

```bash
# 1. Is the service up?
sudo systemctl status openhop-txmesh --no-pager | head -5

# 2. Did it connect to both sides?
sudo journalctl -u openhop-txmesh -n 100 | grep -E "Connected to companion|MQTT connected|WARN|ERROR"

# 3. Is it publishing? (replace YOUR-USERNAME, YOUR-PASSWORD and YOUR-NODE-NAME)
/opt/openhop_txmesh/bin/python -c "
import paho.mqtt.client as m, ssl, time
seen = {}
c = m.Client(transport='websockets', callback_api_version=m.CallbackAPIVersion.VERSION2)
c.username_pw_set('YOUR-USERNAME', 'YOUR-PASSWORD'); c.tls_set(cert_reqs=ssl.CERT_REQUIRED)
c.on_message = lambda c, u, x: seen.__setitem__(x.topic.split('/', 3)[-1], x.payload[:80].decode(errors='replace'))
c.connect('collector.txme.sh', 443); c.subscribe('meshcore/YOUR-USERNAME/YOUR-NODE-NAME/#'); c.loop_start()
time.sleep(75); c.loop_stop()
for k in sorted(seen): print(f'{k:22} {seen[k]}')
"
```

Healthy output from the third command, after 75 seconds:

```
sensors                {}
status                 online
telemetry              {"uptime_s":...,"rx":57,...}
```

plus `heard/...` and `contact/...` lines once the mesh has announced itself.

---

## What it publishes, and what it costs

Everything lands under `meshcore/YOUR-USERNAME/YOUR-NODE-NAME/`.

| Topic | Kept by the broker? | How often | How much |
|---|---|---|---|
| `status` | yes | on connect, and when the node drops | tiny |
| `telemetry` | no | every 60 s | tiny |
| `sensors` | no | every 60 s | tiny, always `{}` |
| `contact/<id>` | yes | every 5 min | one saved message per node your node knows |
| `heard/<id>` | yes | every 3 min | up to 16 saved messages |
| `msg/dm`, `msg/channel` | no | as messages arrive | one per message your node can read |
| `advert` | no | as announcements arrive | off by default |
| `packets` | no | every frame | off by default, and the big one |

`<id>` is the first 8 hex characters of a node's public key.

"Kept by the broker" (retained, in MQTT terms) means the collector remembers
the last value even after your node goes offline. That is what lets a
dashboard show your node list right away. It also means stale entries stay
until you clear them. See [Shutting a node down](#shutting-a-node-down).

---

## Privacy: read this before adding channels

Your node mirrors every message it can read to the collector, including any
private channel you put in `channels`. If you add a private channel, the
messages on it leave the mesh and land on the collector. That is your choice
to make, and if other people use that channel they should know an observer is
on it.

Your node can only read channels in its `channels` list. It has no key for
anything else.

---

## Transmitting: the limits

Bot replies reach your node as messages on
`meshcore/YOUR-USERNAME/YOUR-NODE-NAME/send/CHANNEL`, and your radio transmits
them. Airtime is shared with everyone on the mesh, so the plugin enforces
limits on that path no matter who publishes to it.

| Limit | Value |
|---|---|
| Rate | 6 messages per minute |
| Queue | over-budget messages wait in a queue of 4. Anything beyond that is dropped. |
| Length | 160 bytes on air. Longer text is cut, never in the middle of a character. |
| Channel name | up to 31 characters |
| Retained messages | refused. A retained `send/` message would re-transmit on every reconnect, forever. |
| Unknown channel, empty text | dropped silently. An error reply would itself be airtime. |

There is also a fleet topic, `meshcore/YOUR-USERNAME/all/send/CHANNEL`, which
reaches every node you run at once. If two of your nodes are on the same mesh
they both transmit, so only use it when your nodes are on different meshes.

Anyone who can publish to your `send/` topics can key up your radio. Your
collector password is what protects that. Keep it secret.

---

## Troubleshooting

Find your symptom, then follow the fix.

### The plugin exits right away: `no MQTT host configured; nothing to do`

`host` is empty in the config. Set `"host": "collector.txme.sh"`.

### The plugin exits right away: `node_name is not set`

`node_name` is missing from the config. Set it to the same name you gave the
companion in the repeater's `config.yaml`.

### `Companion connection lost (...); retry in Ns`, repeating

The plugin cannot reach the repeater's companion port. One of these:

- The repeater was not restarted after you added the companion. Restart it.
- Wrong port. `companion_port` in the plugin config has to equal `tcp_port` in
  the repeater's companion entry.
- Something else is connected. A companion allows one client at a time. If a
  phone app is attached to this companion, disconnect it, or give the phone
  its own companion entry.

Check with:

```bash
sudo journalctl -u openhop-repeater -n 200 | grep -i "companion frame server"
```

You should see `listening on 127.0.0.1:5001`.

### `Observer MQTT refused: rc=5`

Wrong username or password. Fix them in the config and restart. `rc=4` means
the credentials are malformed and `rc=2` means the client ID was rejected.

After repeated failures the plugin backs off: 10 s, 30 s, 60 s, 2 min, 5 min.
After three failures at 5 min it only retries every 30 min. Restarting the
plugin resets that.

### Connected, but `rx` in `telemetry` stays at `0`

The plugin is fine. Your radio is not hearing anything. This is nearly always
the repeater's radio settings (frequency, bandwidth or spreading factor) not
matching your mesh. Check them in the repeater's config or web UI against what
your mesh uses. The plugin cannot fix this. It only reports what the repeater
hears.

### `heard/` and `contact/` are empty

Give it time. Nodes announce themselves every few minutes to every few hours.
`contact/` fills as your node learns them, and `heard/` needs announcements to
arrive. Both publish on a timer, 3 and 5 minutes. Ten minutes of nothing on a
mesh you know is active points back to the `rx: 0` problem above.

### `msg/channel` shows `"channel": "?"`

A message arrived on a channel slot with no name. Usually that is a channel
you hold a key for but have not listed in `channels`. Add it with a name.

### I published to `send/...` and nothing happened

No log line at all means it was dropped silently. The usual causes, most
common first:

1. The channel name is not in your `channels` list, or the slot number is
   wrong.
2. The message was published as retained. Those are always refused.
3. The text was empty.

Run with `--log-level DEBUG` to see `send bridge: no channel matching '...';
dropped`.

### `boots` is missing from telemetry

The plugin cannot write its state directory. Under systemd, the unit in
[SETUP.md](SETUP.md#5b-run-it-as-a-service) handles this with
`StateDirectory=`. Otherwise set `state_dir` to a directory the plugin's user
can write.

### Messages have no `snr` field

This happens when the radio reported an SNR of exactly zero. The plugin cannot
tell that apart from "not reported", so it leaves the field out rather than
publish a possibly fake `0.0`.

---

## Upgrading

```bash
/opt/openhop_txmesh/bin/pip install -U openhop-txmesh-plugin
sudo systemctl restart openhop-txmesh
```

Or upgrade through the openHop plugin manager, if that is how you installed.

Your config keeps working. New versions only ever add settings and never
change what an existing one means. Your node's identity, its channels and its
restart counter all live outside the plugin and survive upgrades.

---

## Shutting a node down

If you are just restarting or pausing, `sudo systemctl stop openhop-txmesh` is
enough. The node announces `offline` on the way out.

If you are retiring the node, also clear what the collector still remembers
about it. Otherwise dashboards keep showing a node that no longer exists.

```bash
/opt/openhop_txmesh/bin/python -c "
import paho.mqtt.client as m, ssl, time
topics = set()
c = m.Client(transport='websockets', callback_api_version=m.CallbackAPIVersion.VERSION2)
c.username_pw_set('YOUR-USERNAME', 'YOUR-PASSWORD'); c.tls_set(cert_reqs=ssl.CERT_REQUIRED)
c.on_message = lambda c, u, x: x.retain and topics.add(x.topic)
c.connect('collector.txme.sh', 443); c.subscribe('meshcore/YOUR-USERNAME/YOUR-NODE-NAME/#'); c.loop_start()
time.sleep(10)
for t in sorted(topics):
    c.publish(t, payload=None, qos=1, retain=True); print('cleared', t)
time.sleep(3); c.loop_stop()
print(len(topics), 'retained topics cleared')
"
```

Then remove the companion entry from the repeater's `config.yaml` and restart
the repeater, if you no longer want the identity on the mesh at all.
