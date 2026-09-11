# Releasing to the openHop plugin catalogue

The [openHop plugin catalogue](https://github.com/openhop-dev/openhop-plugin-catalogue)
is metadata only. It records one approved version per plugin, the commit it
was built from, the exact GitHub Release wheel URL and the wheel's SHA-256.
Repeaters read the published catalogue and download the wheel straight from
this repository's GitHub Release, so the release layout has to match what the
catalogue validator checks.

## What a release must look like

| Requirement | How this repository meets it |
|---|---|
| Tag `v<version>` | `git tag -a v0.1.3` on the commit to build |
| `version` in `pyproject.toml` and `openhop-plugin.json` equal the tag | CI fails the tag build if they differ |
| Release asset `openhop_txmesh_plugin-<version>-py3-none-any.whl` | built by `python -m build --wheel` and attached by CI |
| Lowercase SHA-256 of that exact asset | CI attaches `<wheel>.sha256` next to the wheel |
| 40-character `source_revision` | the tagged commit, recorded in `catalogue-entry.json` |
| HTTPS `logo` URL | `assets/logo.png`, served from `raw.githubusercontent.com` on `main` |
| `category` | `integration` |

## Cutting a release

1. Bump `version` in both `pyproject.toml` and `openhop-plugin.json`.
2. Commit, then tag and push:

   ```bash
   git tag -a v0.1.3 -m "v0.1.3"
   git push origin main v0.1.3
   ```

3. The **Build Wheel** workflow runs the tests, builds the wheel, checks the
   version against the tag, and creates the GitHub Release with three assets:
   the wheel, its `.sha256`, and `catalogue-entry.json`. The same entry is
   printed in the workflow's step summary.

Pushing a tag is the only supported way to publish. Do not upload a locally
built wheel by hand: the digest in the catalogue pins the exact bytes clients
accept, and a rebuilt wheel will not match.

## Proposing the catalogue update

1. Download `catalogue-entry.json` from the release, or copy it from the step
   summary. It already carries the source revision, wheel URL and digest.
2. Confirm the digest against the published asset:

   ```bash
   curl -sLO https://github.com/zfouts/openhop-txmesh-plugin/releases/download/v0.1.3/openhop_txmesh_plugin-0.1.3-py3-none-any.whl
   shasum -a 256 openhop_txmesh_plugin-0.1.3-py3-none-any.whl
   ```

3. In a fork of the catalogue, replace this plugin's object in
   `catalogue.json` with the entry (or add it if this is the first listing),
   run the catalogue's local validation, and open a PR. A catalogue maintainer
   reviews and merges; publishing a release here does not approve it.

## Regenerating the entry locally

`scripts/catalogue_entry.py` produces the same JSON CI attaches. It reads the
manifest and `pyproject.toml`, hashes the wheel you pass it, and uses
`GITHUB_SHA` or the current `git HEAD` as the source revision:

```bash
python -m build --wheel
python scripts/catalogue_entry.py dist/openhop_txmesh_plugin-0.1.3-py3-none-any.whl
```

Only an entry generated from the CI-built wheel should be submitted, for the
reason above.
