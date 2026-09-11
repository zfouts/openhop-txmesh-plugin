#!/usr/bin/env python3
"""Emit the openHop plugin catalogue entry for a built wheel.

Prints a JSON object in the shape the openhop-plugin-catalogue repository
expects (schema 2): identity from openhop-plugin.json and pyproject.toml,
the exact GitHub Release wheel URL, and the wheel's SHA-256 digest.

    python scripts/catalogue_entry.py dist/openhop_txmesh_plugin-0.1.2-py3-none-any.whl

The source revision defaults to GITHUB_SHA, then to `git rev-parse HEAD`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REPOSITORY = "zfouts/openhop-txmesh-plugin"
CATEGORY = "integration"
LOGO = f"https://raw.githubusercontent.com/{REPOSITORY}/main/assets/logo.png"
TAGS = ["mqtt", "observer", "txmesh", "companion"]


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_revision() -> str:
    rev = os.environ.get("GITHUB_SHA")
    if not rev:
        rev = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if len(rev) != 40:
        raise SystemExit(f"source revision is not a 40-character SHA: {rev!r}")
    return rev


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("wheel", type=Path, help="path to the built wheel")
    parser.add_argument("--revision", help="source commit (default: GITHUB_SHA or git HEAD)")
    args = parser.parse_args()

    manifest = json.loads((ROOT / "openhop-plugin.json").read_text(encoding="utf-8"))
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    version = project["version"]
    if manifest["version"] != version:
        raise SystemExit(
            f"openhop-plugin.json version {manifest['version']} != pyproject.toml {version}"
        )

    wheel = args.wheel
    expected = f"{project['name'].replace('-', '_')}-{version}-py3-none-any.whl"
    if wheel.name != expected:
        raise SystemExit(f"wheel filename {wheel.name} != expected {expected}")

    entry = {
        "id": manifest["id"],
        "name": manifest["name"],
        "description": manifest["description"],
        "repository": REPOSITORY,
        "category": CATEGORY,
        "logo": LOGO,
        "distribution": project["name"],
        "source_revision": args.revision or source_revision(),
        "version": version,
        "wheel_url": f"https://github.com/{REPOSITORY}/releases/download/v{version}/{wheel.name}",
        "sha256": sha256_of(wheel),
        "min_repeater_version": manifest["min_repeater_version"],
        "homepage": project["urls"]["Homepage"],
        "tags": TAGS,
    }
    json.dump(entry, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
