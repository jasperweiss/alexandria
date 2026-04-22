# Alexandria

Local SQLite index and BitTorrent downloader for **Anna's Archive** metadata. You keep a copy of the torrent list and record dump on disk, then search and fetch files offline from your own index.

## What you need

1. **Rust** (current stable) and a normal C toolchain for dependencies.
2. **Torrent list** — the full Anna's Archive torrents JSON (from the **Torrents** section of the site, `/torrents`). This lists magnets and display names so records can be joined to swarms.
3. **Derived mirror metadata** — the latest **`aa_derived_mirror_metadata`** dataset. You only need the **`elasticsearch`** directory inside it (many `aarecords__<n>.json.gz` shards). That tree is on the order of **~155 GB**; plan disk space for the download, the SQLite database, and headroom.
4. **Network** when downloading: BitTorrent is used to retrieve files.

Ingest is heavy I/O and parsing. Expect **several hours** (or longer on slow disks) for a full elastic ingest even after filtering.

## Build

```bash
cargo build --release
```

The binary is `target/release/alexandria`.

## One-time database setup

Pick a path for the database (default `alexandria.db`):

```bash
alexandria db-init --path alexandria.db
```

`ingest-torrents` and `ingest-elastic` also ensure the schema exists, so this step is optional if you go straight to ingest.

## Ingest torrents

Point at the torrents JSON you obtained from Anna's Archive:

```bash
alexandria ingest-torrents --db alexandria.db --from /path/to/torrents_full.json
```

This fills the `torrents` table (magnets, BTIH, etc.) used when resolving downloads.

## Ingest Elasticsearch shards

Pass the **directory** that contains only the shard files (`aarecords__0.json.gz`, `aarecords__1.json.gz`, …). The ingestor walks that directory in order:

```bash
alexandria ingest-elastic --db alexandria.db --from /path/to/elasticsearch
```

Format is auto-detected per file. For a full mirror, **filtering is strongly recommended** so the database stays smaller and ingest finishes sooner:

```bash
alexandria ingest-elastic --db alexandria.db --from /path/to/elasticsearch \
  -x pdf -x epub -l en
```

- **`-x` / `--extension`** — keep only records whose `extension_best` matches (repeatable; comma-separated values in one flag are allowed).
- **`-l` / `--language`** — keep only records that include a language code in `language_codes` (repeatable; lowercase, e.g. `en`, `fr`).

Other useful flags:

- **`--ingest-batch-size N`** — SQLite transaction batch size (default `500`; larger values can help on bulk inserts).
- **`--no-ingest-fast-pragmas`** — disable temporary “fast ingest” PRAGMA tweaks if you prefer stricter durability settings during ingest.

## Searching

Full-text search (FTS5) over **title** and **author**:

```bash
alexandria search --db alexandria.db --limit 20 your words
```

Plain output is TSV-like lines: `id`, size, extension, author, title. **`--json`** prints structured hits.

If the query is a **single ISBN-10 or ISBN-13** (hyphens optional), search resolves via stored ISBNs. If it is a **numeric OCLC** control number, search resolves via stored OCLC ids.

## Inspecting a record

```bash
alexandria show --db alexandria.db <id>
```

Use the record id from search (with or without an `md5:` prefix). JSON includes metadata, ISBN-13s, OCLC numbers, and `record_files` rows (torrent display name, path inside the torrent, magnet when joined).

## Downloading

Downloads use **BitTorrent** (via librqbit). You need a `record_files` row with a non-empty `path_in_torrent` and a magnet linked from the torrents ingest.

```bash
alexandria download --db alexandria.db <id> --output-dir /path/to/out
```

- **`--timeout-secs`** — cap wait time for the transfer (default 300).
- Files are saved under `output_dir` preserving torrent-relative paths; when the record has a preferred extension from metadata, the final file is renamed to **`{record_id}.{ext}`** (normalized id, without a leading `md5:` in the filename).

**Nested archives** (file lives inside a `.tar` in the torrent): the CLI will explain that you need a place to download the container. Use **`--archive-dir DIR`** and, for non-interactive use, **`--yes`**. See `--help` on `download` for details.

After a successful download, if the record id is a 32-character hex md5, Alexandria verifies the file **MD5** against that id and errors on mismatch.