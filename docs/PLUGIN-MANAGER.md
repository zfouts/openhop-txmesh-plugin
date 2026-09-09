# Installing through the openHop plugin manager

openHop Repeater 1.1.4 and newer ship a plugin manager with a **Plugins** page
in the web UI. It installs this plugin from its wheel, runs it as a supervised
process, keeps its settings and state in a directory it owns, restarts it if
it crashes, and starts it again after the repeater reboots. There is nothing
to install by hand and no systemd unit to write. This is the recommended way
to run the plugin.

If your repeater is older than 1.1.4, or you want to run the plugin on a
different machine from the repeater, use [SETUP.md](SETUP.md) instead.

The placeholders are the same as in [SETUP.md](SETUP.md#placeholders-used-in-this-guide):
`YOUR-USERNAME` and `YOUR-PASSWORD` are your txme.sh login, `YOUR-NODE-NAME`
is the name the mesh will see for your node.

## What you need

- An openHop Repeater 1.1.4 or newer that already hears your mesh. Open its
  web UI and check that the **Plugins** page is there.
- The plugin wheel from the
  [latest release](https://github.com/zfouts/openhop-txmesh-plugin/releases):
  the file named `openhop_txmesh_plugin-<version>-py3-none-any.whl`.
- Internet access from the repeater host. Installing creates a Python
  environment for the plugin and downloads its dependencies.
- Your txme.sh username and password.

## Step 1. Install the wheel

On the **Plugins** page, choose **Install** and upload the wheel. The page
shows the install progress; the "Creating environment" step takes a minute
or two on a Raspberry Pi. When it finishes, **txmesh Observer** appears in
the list with state `DISABLED`. Leave it disabled for now; it has nothing to
connect to yet.

## Step 2. Generate your node's identity

Your node is a real mesh node with its own keypair. The installed plugin can
generate one. Run this on the repeater host:

```bash
/var/lib/openhop_repeater/plugins/txmesh.observer/current/venv/bin/openhop-txmesh keygen
```

If the repeater runs in Docker, run it inside the container:

```bash
docker exec openhop-repeater \
  /var/lib/openhop_repeater/plugins/txmesh.observer/current/venv/bin/openhop-txmesh keygen
```

Copy the `identity_key` line somewhere safe. It is your node's private key.
Lose it and your node comes back on the mesh as a brand new node.

## Step 3. Add the companion to the repeater

Add a companion to the repeater's `config.yaml`, exactly as in
[SETUP.md step 3](SETUP.md#step-3-add-the-node-to-the-repeater). The short
version:

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

Restart the repeater and check its log for
`Loaded companion 'YOUR-NODE-NAME': hash=0x.., port=5001, bind=127.0.0.1`.
The plugin and the repeater share a host, so `127.0.0.1` is right.

## Step 4. Fill in the settings

Back on the **Plugins** page, open **txmesh Observer** and its **Settings**.
The form is pre-filled with defaults. Set these four and leave the rest:

| Setting | Value |
|---|---|
| `node_name` | `YOUR-NODE-NAME`, exactly as in `config.yaml` |
| `username` | `YOUR-USERNAME` |
| `password` | `YOUR-PASSWORD` |
| `channels` | `[{"idx": 1, "name": "#bot"}]` |

`host` is already `collector.txme.sh` and `companion_host` / `companion_port`
already point at `127.0.0.1:5001`. Every setting is described in
[CONFIGURATION.md](CONFIGURATION.md). Save.

The settings are written to
`/var/lib/openhop_repeater/plugins/txmesh.observer/data/config.json`. The
plugin reads that file whenever the manager starts it. Editing the file by
hand works too; restart the plugin afterwards.

## Step 5. Enable it

Press **Enable**. The manager starts the plugin and the state changes to
`RUNNING`. Open its **Logs**. Within a few seconds you should see:

```
txmesh observer started: companion 127.0.0.1:5001 -> meshcore/YOUR-USERNAME/YOUR-NODE-NAME
Connected to companion 127.0.0.1:5001
device clock sync -> ok
path hash: 2-byte -> ok
channel slot 1 = '#bot' -> ok
Observer MQTT connected: meshcore/YOUR-USERNAME/YOUR-NODE-NAME
Send bridge subscribed: meshcore/YOUR-USERNAME/YOUR-NODE-NAME/send/+
```

The repeater's own log shows `Companion client connected (port=5001)` at the
same moment.

If you see `Companion connection lost ... retry in 2s` repeating, the
companion is not listening: check step 3, and that `node_name` in the
settings matches `config.yaml`. If MQTT never connects, check the username and
password.

## Step 6. Confirm it is publishing

Same as [SETUP.md step 6](SETUP.md#step-6-confirm-it-is-publishing). The
plugin's environment has the MQTT library, so the Python path in that command
becomes `/var/lib/openhop_repeater/plugins/txmesh.observer/current/venv/bin/python`.

## Day to day

- **Logs**: the Logs button on the Plugins page, or
  `/var/lib/openhop_repeater/plugins/txmesh.observer/logs/plugin.log`.
- **Restart**: the Restart button. The manager also restarts the plugin if it
  exits on its own. Five unexpected exits within a minute put it in `FAILED`;
  fix the cause, then press Start.
- **Reboots**: an enabled plugin starts again with the repeater. Nothing to do.
- **Upgrading**: upload the new wheel on the Plugins page. Settings and state
  live in `data/` and survive the upgrade.
- **Uninstalling**: Uninstall on the Plugins page. It asks whether to delete
  `data/` too. The companion in `config.yaml` is yours to remove.
- **`boots` in telemetry** counts plugin starts. Its counter lives in `data/`.

## The password

The password is stored in plain text in `data/config.json`, readable by
anyone who can administer the repeater. If you would rather keep it out of
that file, leave `password` empty in the settings and set
`OPENHOP_TXMESH_PASSWORD` in the repeater's environment instead; the manager
passes its environment on to the plugin. In Docker that is one line under
`environment:` in the compose file.

## Doing it from the command line

Everything the Plugins page does is a call to the repeater's API, which is
handy on a headless box. Log in first; the token lasts an hour.

```bash
R=http://127.0.0.1:8000
TOKEN=$(curl -s -X POST $R/auth/login -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"REPEATER-ADMIN-PASSWORD","client_id":"cli"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
AUTH="Authorization: Bearer $TOKEN"

# install from a wheel on the repeater host
curl -s -X POST $R/api/plugins/install -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"wheel_path":"/tmp/openhop_txmesh_plugin-0.1.1-py3-none-any.whl"}'

# settings
curl -s -X POST $R/api/plugins/settings -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"id":"txmesh.observer","config":{"node_name":"YOUR-NODE-NAME","username":"YOUR-USERNAME","password":"YOUR-PASSWORD","channels":[{"idx":1,"name":"#bot"}]}}'

# enable (also starts it), then read the log
curl -s -X POST $R/api/plugins/enable -H "$AUTH" -H 'Content-Type: application/json' -d '{"id":"txmesh.observer"}'
curl -s "$R/api/plugins/logs?id=txmesh.observer&tail=30" -H "$AUTH"
```

Settings sent this way replace the whole file, so include every key you want
set. Keys you leave out take their defaults.
