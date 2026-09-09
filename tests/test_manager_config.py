"""Config discovery under the openHop plugin manager."""

import json

from openhop_txmesh.main import load_config


def test_manager_config_json_is_used_when_no_path_given(tmp_path, monkeypatch):
    (tmp_path / "config.json").write_text(json.dumps({"node_name": "from-manager", "port": 8883}))
    monkeypatch.setenv("OPENHOP_PLUGIN_DATA", str(tmp_path))
    cfg = load_config(None)
    assert cfg["node_name"] == "from-manager"
    assert cfg["port"] == 8883
    # the data dir doubles as the default state_dir
    assert cfg["state_dir"] == str(tmp_path)


def test_explicit_path_wins_over_manager_config(tmp_path, monkeypatch):
    (tmp_path / "config.json").write_text(json.dumps({"node_name": "from-manager"}))
    mine = tmp_path / "mine.json"
    mine.write_text(json.dumps({"node_name": "explicit"}))
    monkeypatch.setenv("OPENHOP_PLUGIN_DATA", str(tmp_path))
    assert load_config(str(mine))["node_name"] == "explicit"


def test_missing_manager_config_falls_back_to_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENHOP_PLUGIN_DATA", str(tmp_path))
    cfg = load_config(None)
    assert cfg["companion_port"] == 5001
    assert cfg["state_dir"] == str(tmp_path)


def test_env_state_dir_is_respected(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENHOP_PLUGIN_DATA", str(tmp_path))
    monkeypatch.setenv("OPENHOP_TXMESH_STATE_DIR", "/elsewhere")
    assert load_config(None)["state_dir"] == "/elsewhere"
