use crate::db::{any_records_exist, normalize_record_id, FastIngestPragmas};
use crate::isbn::record_isbn13_set_from_file_unified;
use crate::oclc::record_oclc_set_from_identifiers_unified;
use anyhow::Context;
use rusqlite::Connection;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default)]
pub enum ElasticInputFormat {
    #[default]
    Auto,
    Lines,
    SingleJson,
}

#[derive(Default, Debug)]
pub struct ElasticIngestStats {
    pub lines_seen: u64,
    pub documents: u64,
    pub inserted: u64,
    pub skipped_existing: u64,
    pub skipped_extension: u64,
    pub skipped_language: u64,
    pub parse_errors: u64,
    pub join_torrent_hits: u64,
    pub join_torrent_misses: u64,
}

/// Tuning for bulk Elasticsearch → SQLite ingest.
#[derive(Clone, Debug)]
pub struct ElasticIngestOptions {
    /// Commit after this many successful record upserts per transaction.
    pub batch_size: usize,
    /// Use faster PRAGMA settings during ingest (restored after each shard file).
    pub fast_pragmas: bool,
}

impl Default for ElasticIngestOptions {
    fn default() -> Self {
        Self {
            batch_size: 500,
            fast_pragmas: true,
        }
    }
}

fn record_exists_tx(tx: &rusqlite::Transaction<'_>, id: &str) -> anyhow::Result<bool> {
    let mut stmt = tx.prepare_cached("SELECT 1 FROM records WHERE id = ?1 LIMIT 1")?;
    Ok(stmt.exists([id])?)
}

fn record_exists_conn(conn: &Connection, id: &str) -> anyhow::Result<bool> {
    let id = normalize_record_id(id);
    let mut stmt = conn.prepare_cached("SELECT 1 FROM records WHERE id = ?1 LIMIT 1")?;
    Ok(stmt.exists([&id])?)
}

fn lookup_torrent_pair_cached(
    tx: &rusqlite::Transaction<'_>,
    cache: &mut HashMap<String, (Option<String>, Option<String>)>,
    display_name: &str,
) -> anyhow::Result<(Option<String>, Option<String>)> {
    if let Some(p) = cache.get(display_name) {
        return Ok(p.clone());
    }
    let mut stmt =
        tx.prepare_cached("SELECT btih, magnet_link FROM torrents WHERE display_name = ?1")?;
    let pair = match stmt.query_row([display_name], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
    }) {
        Ok((btih, mag)) => (Some(btih), mag),
        Err(rusqlite::Error::QueryReturnedNoRows) => (None, None),
        Err(e) => return Err(e.into()),
    };
    cache.insert(display_name.to_string(), pair.clone());
    Ok(pair)
}

/// Build a set of allowed `extension_best` values from CLI args (`-x pdf -x epub` or `-x pdf,epub`).
/// Returns `None` when there is no filter (ingest all extensions).
pub fn extension_filter_from_args(args: &[String]) -> Option<HashSet<String>> {
    let mut set = HashSet::new();
    for arg in args {
        for part in arg.split(',') {
            let e = part.trim().trim_start_matches('.').to_lowercase();
            if !e.is_empty() {
                set.insert(e);
            }
        }
    }
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

/// Build a set of allowed `language_codes` values from CLI args (`-l en -l fr` or `-l en,fr`).
/// Returns `None` when there is no filter (ingest all languages).
pub fn language_filter_from_args(args: &[String]) -> Option<HashSet<String>> {
    let mut set = HashSet::new();
    for arg in args {
        for part in arg.split(',') {
            let c = part.trim().trim_start_matches('.').to_lowercase();
            if !c.is_empty() {
                set.insert(c);
            }
        }
    }
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

pub fn torrent_display_name_from_es_path(path: &str) -> Option<String> {
    let name = path.rsplit('/').next()?.trim();
    if name.ends_with(".torrent") {
        Some(name.to_string())
    } else {
        None
    }
}

/// Last path segment of a server-side path (bulk dumps use this as the torrent root file or folder name).
pub fn basename_posix(path: &str) -> String {
    path.replace('\\', "/")
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

/// True when `server_path` belongs to the batch named by `torrent_display_name` (substring match on the batch key, or `/275000/`-style paths for `s_275000.torrent`).
pub fn server_path_matches_torrent(server_path: &str, torrent_display_name: &str) -> bool {
    let key = torrent_display_name.trim_end_matches(".torrent");
    let sp = server_path.replace('\\', "/");
    if sp.contains(key) {
        return true;
    }
    if let Some(num) = key.strip_prefix("s_") {
        if !num.is_empty() && num.chars().all(|c| c.is_ascii_digit()) {
            if sp.contains(&format!("/{num}/"))
                || sp.ends_with(&format!("/{num}"))
                || sp.contains(&format!("/repository/{num}/"))
            {
                return true;
            }
        }
    }
    false
}

/// `aarecords__<n>.json.gz` (Elasticsearch-style export shards); excludes `.mapping.json.gz`, etc.
pub fn shard_id_from_aarecords_filename(name: &str) -> Option<u64> {
    let lower = name.to_ascii_lowercase();
    let rest = lower.strip_prefix("aarecords__")?;
    let num_str = rest.strip_suffix(".json.gz")?;
    if num_str.is_empty() || !num_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    num_str.parse().ok()
}

/// Effective `--skip-existing`: off when `--no-skip-existing` is set, when the DB has no rows yet,
/// or when `skip_existing` is false. Avoids per-row `record_exists` when skipping cannot trigger.
fn effective_skip_existing(
    skip_existing: bool,
    no_skip_existing: bool,
    conn: &Connection,
) -> anyhow::Result<bool> {
    if no_skip_existing || !any_records_exist(conn).map_err(|e| anyhow::anyhow!("{e}"))? {
        return Ok(false);
    }
    Ok(skip_existing)
}

/// Ingest one dump file, or a directory of `aarecords__*.json.gz` shards (numeric suffix only).
pub fn ingest_elastic_path(
    conn: &Connection,
    path: &Path,
    format: ElasticInputFormat,
    skip_existing: bool,
    no_skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    options: ElasticIngestOptions,
) -> anyhow::Result<ElasticIngestStats> {
    if path.is_dir() {
        return ingest_elastic_directory(
            conn,
            path,
            format,
            skip_existing,
            no_skip_existing,
            replace,
            extension_filter,
            language_filter,
            options,
        );
    }
    println!("elastic ingest: {}", path.display());
    let mut torrent_cache = HashMap::new();
    ingest_elastic_one_file(
        conn,
        path,
        format,
        skip_existing,
        no_skip_existing,
        replace,
        extension_filter,
        language_filter,
        &options,
        &mut torrent_cache,
    )
}

fn ingest_elastic_directory(
    conn: &Connection,
    dir: &Path,
    format: ElasticInputFormat,
    skip_existing: bool,
    no_skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    options: ElasticIngestOptions,
) -> anyhow::Result<ElasticIngestStats> {
    let mut shards: Vec<(u64, PathBuf)> = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("read directory {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(id) = shard_id_from_aarecords_filename(&name_str) {
            shards.push((id, path));
        }
    }
    shards.sort_by_key(|(id, _)| *id);
    if shards.is_empty() {
        anyhow::bail!(
            "no aarecords__<n>.json.gz shard files found in {}",
            dir.display()
        );
    }

    let n = shards.len();
    println!(
        "elastic ingest: {} shard file(s) under {}",
        n,
        dir.display()
    );

    let mut total = ElasticIngestStats::default();
    let mut torrent_cache = HashMap::new();
    for (i, (shard_id, path)) in shards.into_iter().enumerate() {
        let fname = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string());
        println!(
            "elastic ingest [{}/{}] shard {} — {}",
            i + 1,
            n,
            shard_id,
            fname
        );
        let file_stats = ingest_elastic_one_file(
            conn,
            &path,
            format,
            skip_existing,
            no_skip_existing,
            replace,
            extension_filter,
            language_filter,
            &options,
            &mut torrent_cache,
        )
        .with_context(|| format!("ingest {} (shard {})", path.display(), shard_id))?;
        total.lines_seen += file_stats.lines_seen;
        total.documents += file_stats.documents;
        total.inserted += file_stats.inserted;
        total.skipped_existing += file_stats.skipped_existing;
        total.skipped_extension += file_stats.skipped_extension;
        total.skipped_language += file_stats.skipped_language;
        total.parse_errors += file_stats.parse_errors;
        total.join_torrent_hits += file_stats.join_torrent_hits;
        total.join_torrent_misses += file_stats.join_torrent_misses;
    }
    Ok(total)
}

fn ingest_elastic_one_file(
    conn: &Connection,
    path: &Path,
    format: ElasticInputFormat,
    skip_existing: bool,
    no_skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    options: &ElasticIngestOptions,
    torrent_cache: &mut HashMap<String, (Option<String>, Option<String>)>,
) -> anyhow::Result<ElasticIngestStats> {
    let _pragma_guard = if options.fast_pragmas {
        Some(FastIngestPragmas::apply(conn).map_err(|e| anyhow::anyhow!("{e}"))?)
    } else {
        None
    };

    let skip_existing = effective_skip_existing(skip_existing, no_skip_existing, conn)?;

    let mut stats = ElasticIngestStats::default();
    let gzip = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);
    let meta = fs::metadata(path).ok();
    let small_uncompressed = !gzip && meta.map(|m| m.len() < 32 * 1024 * 1024).unwrap_or(false);

    match format {
        ElasticInputFormat::SingleJson => {
            ingest_single_json_file(
                conn,
                path,
                skip_existing,
                replace,
                extension_filter,
                language_filter,
                options,
                torrent_cache,
                &mut stats,
            )?;
        }
        ElasticInputFormat::Lines => {
            ingest_lines_stream(
                conn,
                path,
                skip_existing,
                replace,
                extension_filter,
                language_filter,
                options,
                torrent_cache,
                &mut stats,
            )?;
        }
        ElasticInputFormat::Auto => {
            if small_uncompressed {
                let s = fs::read_to_string(path)
                    .with_context(|| format!("read {}", path.display()))?;
                if let Ok(v) = serde_json::from_str::<Value>(&s) {
                    if v.is_object() {
                        ingest_document(
                            conn,
                            v,
                            skip_existing,
                            replace,
                            extension_filter,
                            language_filter,
                            options,
                            torrent_cache,
                            &mut stats,
                        )?;
                        return Ok(stats);
                    }
                }
                ingest_lines_from_str(
                    conn,
                    &s,
                    skip_existing,
                    replace,
                    extension_filter,
                    language_filter,
                    options,
                    torrent_cache,
                    &mut stats,
                )?;
            } else {
                ingest_lines_stream(
                    conn,
                    path,
                    skip_existing,
                    replace,
                    extension_filter,
                    language_filter,
                    options,
                    torrent_cache,
                    &mut stats,
                )?;
            }
        }
    }
    Ok(stats)
}

fn ingest_single_json_file(
    conn: &Connection,
    path: &Path,
    skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    options: &ElasticIngestOptions,
    torrent_cache: &mut HashMap<String, (Option<String>, Option<String>)>,
    stats: &mut ElasticIngestStats,
) -> anyhow::Result<()> {
    let r = crate::io_util::open_decompressed_reader(path)?;
    let v: Value = serde_json::from_reader(r).context("parse single JSON document")?;
    ingest_document(
        conn,
        v,
        skip_existing,
        replace,
        extension_filter,
        language_filter,
        options,
        torrent_cache,
        stats,
    )?;
    Ok(())
}

fn ingest_lines_stream(
    conn: &Connection,
    path: &Path,
    skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    options: &ElasticIngestOptions,
    torrent_cache: &mut HashMap<String, (Option<String>, Option<String>)>,
    stats: &mut ElasticIngestStats,
) -> anyhow::Result<()> {
    let total = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    let bytes_read = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let progress = spawn_shard_progress_thread(bytes_read.clone(), total, done.clone());

    let r = crate::io_util::open_decompressed_reader_with_byte_counter(path, bytes_read)
        .with_context(|| format!("open {}", path.display()))?;

    let res = ingest_lines_reader(
        conn,
        r,
        skip_existing,
        replace,
        extension_filter,
        language_filter,
        options,
        torrent_cache,
        stats,
    );

    done.store(true, Ordering::Relaxed);
    let _ = progress.join();
    res
}

fn spawn_shard_progress_thread(
    bytes_read: Arc<AtomicU64>,
    total: u64,
    done: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let start = Instant::now();
        while !done.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(250));
            let br = bytes_read.load(Ordering::Relaxed);
            let elapsed = start.elapsed().as_secs_f64();
            print_shard_progress_line(br, total, elapsed);
        }
        let br = bytes_read.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();
        let br_show = br.min(total);
        let pct = if total > 0 {
            (br_show as f64 / total as f64 * 100.0).min(100.0)
        } else {
            100.0
        };
        eprint!(
            "\r  shard I/O: {} / {} ({:.1}%) — {:.1}s{}",
            format_bytes_iec(br_show),
            format_bytes_iec(total),
            pct,
            elapsed,
            " ".repeat(12)
        );
        let _ = std::io::stderr().flush();
        eprintln!();
    })
}

fn print_shard_progress_line(br: u64, total: u64, elapsed_secs: f64) {
    let br_show = if total > 0 { br.min(total) } else { br };
    let (pct, eta) = if total > 0 {
        let pct = (br_show as f64 / total as f64 * 100.0).min(100.0);
        let eta = if br_show > 4096 && elapsed_secs > 0.05 && br_show < total {
            let rate = br_show as f64 / elapsed_secs;
            let rem_s = (total - br_show) as f64 / rate;
            format_eta_seconds(rem_s)
        } else {
            "…".to_string()
        };
        (pct, eta)
    } else {
        (0.0, "—".to_string())
    };
    eprint!(
        "\r  shard I/O: {} / {} ({:.1}%) — ETA {}",
        format_bytes_iec(br_show),
        format_bytes_iec(total),
        pct,
        eta
    );
    let _ = std::io::stderr().flush();
}

fn format_bytes_iec(n: u64) -> String {
    let x = n as f64;
    const KIB: f64 = 1024.0;
    if n < 1024 {
        return format!("{n} B");
    }
    if x < KIB * KIB {
        return format!("{:.2} KiB", x / KIB);
    }
    if x < KIB * KIB * KIB {
        return format!("{:.2} MiB", x / (KIB * KIB));
    }
    if x < KIB * KIB * KIB * KIB {
        return format!("{:.2} GiB", x / (KIB * KIB * KIB));
    }
    format!("{:.2} TiB", x / (KIB * KIB * KIB * KIB))
}

fn format_eta_seconds(seconds: f64) -> String {
    if !seconds.is_finite() || seconds < 0.0 {
        return "—".to_string();
    }
    if seconds > 86400.0 * 365.0 {
        return "—".to_string();
    }
    let s = seconds.round() as u64;
    if s < 60 {
        return format!("{s}s");
    }
    if s < 3600 {
        return format!("{}m {:02}s", s / 60, s % 60);
    }
    format!("{}h {:02}m", s / 3600, (s % 3600) / 60)
}

fn ingest_lines_from_str<'a>(
    conn: &'a Connection,
    s: &str,
    skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    options: &ElasticIngestOptions,
    torrent_cache: &mut HashMap<String, (Option<String>, Option<String>)>,
    stats: &mut ElasticIngestStats,
) -> anyhow::Result<()> {
    ingest_lines_reader(
        conn,
        BufReader::new(s.as_bytes()),
        skip_existing,
        replace,
        extension_filter,
        language_filter,
        options,
        torrent_cache,
        stats,
    )
}

fn ingest_lines_reader<'a>(
    conn: &'a Connection,
    reader: impl BufRead,
    skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    options: &ElasticIngestOptions,
    torrent_cache: &mut HashMap<String, (Option<String>, Option<String>)>,
    stats: &mut ElasticIngestStats,
) -> anyhow::Result<()> {
    let batch_size = options.batch_size.max(1);
    let mut tx: Option<rusqlite::Transaction<'a>> = None;
    let mut rows_in_batch: usize = 0;

    let mut reader = reader;
    let mut line = String::new();
    let mut expect_bulk_doc = false;
    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        stats.lines_seen += 1;
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(t) {
            Ok(v) => v,
            Err(_) => {
                stats.parse_errors += 1;
                continue;
            }
        };

        if expect_bulk_doc {
            ingest_document_with_batch(
                conn,
                &mut tx,
                &mut rows_in_batch,
                torrent_cache,
                batch_size,
                &v,
                skip_existing,
                replace,
                extension_filter,
                language_filter,
                stats,
            )?;
            expect_bulk_doc = false;
            continue;
        }

        if is_bulk_action_line(&v) {
            expect_bulk_doc = true;
            continue;
        }

        ingest_document_with_batch(
            conn,
            &mut tx,
            &mut rows_in_batch,
            torrent_cache,
            batch_size,
            &v,
            skip_existing,
            replace,
            extension_filter,
            language_filter,
            stats,
        )?;
    }

    if let Some(t) = tx.take() {
        t.commit()?;
    }
    Ok(())
}

fn is_bulk_action_line(v: &Value) -> bool {
    let Some(o) = v.as_object() else {
        return false;
    };
    if o.len() != 1 {
        return false;
    }
    o.contains_key("index") || o.contains_key("create") || o.contains_key("update")
}

fn ingest_document<'a>(
    conn: &'a Connection,
    v: Value,
    skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    options: &ElasticIngestOptions,
    torrent_cache: &mut HashMap<String, (Option<String>, Option<String>)>,
    stats: &mut ElasticIngestStats,
) -> anyhow::Result<()> {
    let batch_size = options.batch_size.max(1);
    let mut tx: Option<rusqlite::Transaction<'a>> = None;
    let mut rows_in_batch: usize = 0;
    ingest_document_with_batch(
        conn,
        &mut tx,
        &mut rows_in_batch,
        torrent_cache,
        batch_size,
        &v,
        skip_existing,
        replace,
        extension_filter,
        language_filter,
        stats,
    )?;
    if let Some(t) = tx.take() {
        t.commit()?;
    }
    Ok(())
}

fn ingest_document_with_batch<'a>(
    conn: &'a Connection,
    tx: &mut Option<rusqlite::Transaction<'a>>,
    rows_in_batch: &mut usize,
    torrent_cache: &mut HashMap<String, (Option<String>, Option<String>)>,
    batch_size: usize,
    v: &Value,
    skip_existing: bool,
    replace: bool,
    extension_filter: Option<&HashSet<String>>,
    language_filter: Option<&HashSet<String>>,
    stats: &mut ElasticIngestStats,
) -> anyhow::Result<()> {
    stats.documents += 1;
    let source = v.get("_source").unwrap_or(v);

    let id_raw = source
        .get("id")
        .and_then(|x| x.as_str())
        .or_else(|| v.get("_id").and_then(|x| x.as_str()))
        .ok_or_else(|| anyhow::anyhow!("document missing id"))?;
    let id = normalize_record_id(id_raw);

    let fut = source.pointer("/file_unified_data");
    let ext_raw = fut
        .and_then(|x| x.get("extension_best"))
        .and_then(|x| x.as_str())
        .unwrap_or("");

    if let Some(allowed) = extension_filter {
        let ext = ext_raw.trim().trim_start_matches('.').to_lowercase();
        if !allowed.contains(&ext) {
            stats.skipped_extension += 1;
            return Ok(());
        }
    }

    if let Some(allowed) = language_filter {
        let doc_codes: Vec<String> = fut
            .and_then(|x| x.get("language_codes"))
            .and_then(|x| x.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.trim().to_lowercase()))
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let matches = doc_codes.iter().any(|c| allowed.contains(c));
        if !matches {
            stats.skipped_language += 1;
            return Ok(());
        }
    }

    let already_exists = if skip_existing && !replace {
        if let Some(t) = tx.as_ref() {
            record_exists_tx(t, &id)?
        } else {
            record_exists_conn(conn, &id)?
        }
    } else {
        false
    };
    if skip_existing && !replace && already_exists {
        stats.skipped_existing += 1;
        return Ok(());
    }

    let title = fut
        .and_then(|x| x.get("title_best"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let author = fut
        .and_then(|x| x.get("author_best"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let year = fut
        .and_then(|x| x.get("year_best"))
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();
    let extension = ext_raw.to_string();
    let content_type = fut
        .and_then(|x| x.get("content_type_best"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let filesize = fut
        .and_then(|x| x.get("filesize_best"))
        .and_then(|x| x.as_i64());

    let language = fut
        .and_then(|x| x.get("language_codes"))
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();

    let search_text = String::new();

    let mut torrent_paths: Vec<String> = fut
        .and_then(|x| x.get("classifications_unified"))
        .and_then(|c| c.get("torrent"))
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(std::string::ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    torrent_paths.sort();
    torrent_paths.dedup();

    let mut filepaths: Vec<String> = fut
        .and_then(|x| x.get("identifiers_unified"))
        .and_then(|c| c.get("filepath"))
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(std::string::ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let original_fn = fut
        .and_then(|x| x.get("original_filename_best"))
        .and_then(|x| x.as_str());
    if filepaths.is_empty() {
        if let Some(o) = original_fn {
            filepaths.push(o.to_string());
        }
    }
    filepaths.sort();
    filepaths.dedup();

    let mut server_paths: Vec<String> = fut
        .and_then(|x| x.get("identifiers_unified"))
        .and_then(|c| c.get("server_path"))
        .and_then(|x| x.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(std::string::ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    server_paths.sort();
    server_paths.dedup();

    let isbn13_for_record = record_isbn13_set_from_file_unified(fut);
    let identifiers_unified = fut.and_then(|x| x.get("identifiers_unified"));
    let oclc_for_record = record_oclc_set_from_identifiers_unified(identifiers_unified);

    let mut torrent_file_pairs: Vec<(String, String)> = Vec::new();
    for tpath in &torrent_paths {
        let Some(tdname) = torrent_display_name_from_es_path(tpath.as_str()) else {
            continue;
        };

        let mut paths_for_torrent: Vec<String> = if !server_paths.is_empty() {
            server_paths
                .iter()
                .filter(|sp| server_path_matches_torrent(sp.as_str(), &tdname))
                .map(|sp| basename_posix(sp))
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            Vec::new()
        };
        paths_for_torrent.sort();
        paths_for_torrent.dedup();

        if !paths_for_torrent.is_empty() {
            for p in paths_for_torrent {
                torrent_file_pairs.push((tdname.clone(), p));
            }
        } else if server_paths.is_empty() {
            for fp in &filepaths {
                torrent_file_pairs.push((tdname.clone(), fp.clone()));
            }
        }
    }
    torrent_file_pairs.sort();
    torrent_file_pairs.dedup();

    if tx.is_none() {
        *tx = Some(conn.unchecked_transaction()?);
    }
    {
        let t = tx.as_mut().unwrap();
        let mut ins_rec = t.prepare_cached(
            r#"INSERT INTO records (id, title, author, year, extension, content_type, filesize, language, search_text)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
               ON CONFLICT(id) DO UPDATE SET
                 title = excluded.title,
                 author = excluded.author,
                 year = excluded.year,
                 extension = excluded.extension,
                 content_type = excluded.content_type,
                 filesize = excluded.filesize,
                 language = excluded.language,
                 search_text = excluded.search_text"#,
        )?;
        ins_rec.execute(rusqlite::params![
            &id,
            title,
            author,
            year,
            extension,
            content_type,
            filesize,
            language,
            search_text,
        ])?;

        let mut del_isbn = t.prepare_cached("DELETE FROM record_isbns WHERE record_id = ?1")?;
        del_isbn.execute([&id])?;
        let mut ins_isbn = t.prepare_cached(
            r#"INSERT INTO record_isbns (isbn13, record_id) VALUES (?1, ?2)
               ON CONFLICT(isbn13) DO UPDATE SET record_id = excluded.record_id"#,
        )?;
        for isbn in &isbn13_for_record {
            ins_isbn.execute(rusqlite::params![isbn, &id])?;
        }

        let mut del_oclc = t.prepare_cached("DELETE FROM record_oclc WHERE record_id = ?1")?;
        del_oclc.execute([&id])?;
        let mut ins_oclc = t.prepare_cached(
            r#"INSERT INTO record_oclc (oclc, record_id) VALUES (?1, ?2)
               ON CONFLICT(oclc) DO UPDATE SET record_id = excluded.record_id"#,
        )?;
        for o in &oclc_for_record {
            ins_oclc.execute(rusqlite::params![o, &id])?;
        }

        let mut del_files = t.prepare_cached("DELETE FROM record_files WHERE record_id = ?1")?;
        del_files.execute([&id])?;

        let mut ins_file = t.prepare_cached(
            r#"INSERT INTO record_files (record_id, torrent_display_name, path_in_torrent, filesize_bytes, btih, magnet_link)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
        )?;

        for (tdname, fp) in torrent_file_pairs {
            let (btih, magnet) = lookup_torrent_pair_cached(t, torrent_cache, &tdname)?;
            if btih.is_some() {
                stats.join_torrent_hits += 1;
            } else {
                stats.join_torrent_misses += 1;
            }
            let btih_s = btih.as_deref();
            let mag_s = magnet.as_deref();
            ins_file.execute(rusqlite::params![
                &id,
                tdname,
                fp.as_str(),
                filesize,
                btih_s,
                mag_s
            ])?;
        }

        if torrent_paths.is_empty() && !filepaths.is_empty() {
            for fp in &filepaths {
                ins_file.execute(rusqlite::params![
                    &id,
                    "",
                    fp.as_str(),
                    filesize,
                    None::<String>,
                    None::<String>
                ])?;
            }
        }
    }

    *rows_in_batch += 1;
    if *rows_in_batch >= batch_size {
        let t_owned = tx
            .take()
            .expect("open transaction required for batch commit");
        t_owned.commit()?;
        *rows_in_batch = 0;
    }

    stats.inserted += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::init_schema;
    use crate::search::search_records;

    #[test]
    fn fts_triggers_index_record_on_insert() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        init_schema(&conn).unwrap();
        conn.execute(
            "INSERT INTO records (id, title, author, year, extension, content_type, filesize, language, search_text) \
             VALUES ('r1', 'Hello', 'World', '', '', '', NULL, '', '')",
            [],
        )
        .unwrap();
        let hits = search_records(&conn, "Hello", 10).unwrap();
        assert!(
            hits.iter().any(|h| h.id == "r1"),
            "expected FTS trigger to index title, got {hits:?}"
        );
    }

    #[test]
    fn extension_filter_from_cli_args() {
        assert!(extension_filter_from_args(&[]).is_none());
        assert!(extension_filter_from_args(&["".into(), "  ".into()]).is_none());
        let a = extension_filter_from_args(&["pdf".into(), "epub".into()]).unwrap();
        assert!(a.contains("pdf") && a.contains("epub"));
        let b = extension_filter_from_args(&["PDF,.Epub".into()]).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn language_filter_from_cli_args() {
        assert!(language_filter_from_args(&[]).is_none());
        assert!(language_filter_from_args(&["".into(), "  ".into()]).is_none());
        let a = language_filter_from_args(&["en".into(), "fr".into()]).unwrap();
        assert!(a.contains("en") && a.contains("fr"));
        let b = language_filter_from_args(&["EN,.Fr".into()]).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn aarecords_shard_filename_filter() {
        assert_eq!(
            shard_id_from_aarecords_filename("aarecords__11.json.gz"),
            Some(11)
        );
        assert_eq!(
            shard_id_from_aarecords_filename("aarecords__0.json.gz"),
            Some(0)
        );
        assert_eq!(
            shard_id_from_aarecords_filename("AARECORDS__8.JSON.GZ"),
            Some(8)
        );
        assert_eq!(shard_id_from_aarecords_filename("aarecords__11.mapping.json.gz"), None);
        assert_eq!(shard_id_from_aarecords_filename("aarecords__11.json"), None);
        assert_eq!(shard_id_from_aarecords_filename("other.json.gz"), None);
    }

    #[test]
    fn server_path_matches_numeric_bucket_torrent() {
        let sp = "gi/fake_vendor/standarts/repository/275000/deadbeefcafebabe1234567890abcd12";
        assert!(server_path_matches_torrent(sp, "s_275000.torrent"));
        assert_eq!(basename_posix(sp), "deadbeefcafebabe1234567890abcd12");
    }

    #[test]
    fn server_path_matches_batch_folder_in_path() {
        let sp = "g3/fixture_files/20200101/fixture_org__fixture_batch__20200101T000000Z--20200101T000001Z/aacid__fixture_inner__20200101T000000Z__00feedface00";
        let t = "fixture_org__fixture_batch__20200101T000000Z--20200101T000001Z.torrent";
        assert!(server_path_matches_torrent(sp, t));
        assert_eq!(
            basename_posix(sp),
            "aacid__fixture_inner__20200101T000000Z__00feedface00"
        );
    }
}
