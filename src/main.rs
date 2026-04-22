use alexandria::{
    download::{
        execute_download_plan, fetch_torrent_download_meta_with_progress, format_byte_size_iec,
        plan_file_selection, resolve_download, verify_download_against_record_md5,
        CTRL_C_INTERRUPT_MESSAGE, DownloadPlan,
    },
    elastic::{
        extension_filter_from_args, ingest_elastic_path, language_filter_from_args,
        ElasticIngestOptions, ElasticInputFormat,
    },
    init_schema, ingest_torrents_path, open_db, search_records, show_record,
};
use anyhow::Context;
use clap::{Parser, Subcommand};
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "alexandria", version, about = "Local Anna's Archive index + torrent download")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create database schema
    DbInit {
        #[arg(short, long, default_value = "alexandria.db")]
        path: PathBuf,
    },
    /// Ingest torrents JSON (Anna's Archive torrent list)
    IngestTorrents {
        #[arg(short, long, default_value = "alexandria.db")]
        db: PathBuf,
        #[arg(short, long)]
        from: PathBuf,
    },
    /// Ingest Elasticsearch-style dump (.json / .json.gz) or a directory of aarecords__N.json.gz shards
    IngestElastic {
        #[arg(short, long, default_value = "alexandria.db")]
        db: PathBuf,
        #[arg(
            short,
            long,
            help = "Dump file (.json / .json.gz) or directory of aarecords__<n>.json.gz shards only"
        )]
        from: PathBuf,
        #[arg(long, value_enum, default_value_t = ElasticInputFormatCli::Auto)]
        format: ElasticInputFormatCli,
        /// Skip documents whose record id is already in the DB. Ignored when `records` is empty (fresh ingest) or with `--no-skip-existing`.
        #[arg(long, default_value_t = false)]
        skip_existing: bool,
        /// Always upsert; never skip existing ids. Overrides `--skip-existing`. When `records` is empty, existence checks are skipped automatically.
        #[arg(long, default_value_t = false)]
        no_skip_existing: bool,
        #[arg(long, default_value_t = false)]
        replace: bool,
        #[arg(
            short = 'x',
            long = "extension",
            value_name = "EXT",
            action = clap::ArgAction::Append,
            help = "Only ingest records with this extension_best (repeatable). Comma-separated in one value is allowed, e.g. -x pdf -x epub or -x pdf,epub"
        )]
        extension: Vec<String>,
        #[arg(
            short = 'l',
            long = "language",
            value_name = "CODE",
            action = clap::ArgAction::Append,
            help = "Only ingest records whose file_unified_data.language_codes contains this code (repeatable). Normalized to lowercase; comma-separated allowed, e.g. -l en -l fr or -l en,fr"
        )]
        language: Vec<String>,
        #[arg(
            long = "ingest-batch-size",
            value_name = "N",
            default_value_t = 500,
            help = "Commit and refresh FTS after this many successful record upserts per transaction"
        )]
        ingest_batch_size: usize,
        #[arg(
            long = "no-ingest-fast-pragmas",
            default_value_t = false,
            help = "Do not apply temporary fast SQLite PRAGMA settings during ingest (restored per shard when enabled)"
        )]
        no_ingest_fast_pragmas: bool,
    },
    /// Full-text search
    Search {
        #[arg(short, long, default_value = "alexandria.db")]
        db: PathBuf,
        #[arg(short, long, default_value_t = 20)]
        limit: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(value_name = "QUERY", num_args = 1..)]
        query: Vec<String>,
    },
    /// Show one record and file rows
    Show {
        #[arg(short, long, default_value = "alexandria.db")]
        db: PathBuf,
        id: String,
    },
    /// Download via BitTorrent (librqbit)
    Download {
        #[arg(short, long, default_value = "alexandria.db")]
        db: PathBuf,
        id: String,
        #[arg(short, long, default_value = ".")]
        output_dir: PathBuf,
        #[arg(long, default_value_t = 300)]
        timeout_secs: u64,
        /// Skip confirmation when downloading a container .tar to extract an inner file (requires --archive-dir).
        #[arg(short = 'y', long, default_value_t = false)]
        yes: bool,
        /// Directory where the container archive (.tar) is downloaded before extraction (nested torrent layouts).
        #[arg(long, value_name = "DIR")]
        archive_dir: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
enum ElasticInputFormatCli {
    #[default]
    Auto,
    Lines,
    SingleJson,
}

impl From<ElasticInputFormatCli> for ElasticInputFormat {
    fn from(v: ElasticInputFormatCli) -> Self {
        match v {
            ElasticInputFormatCli::Auto => ElasticInputFormat::Auto,
            ElasticInputFormatCli::Lines => ElasticInputFormat::Lines,
            ElasticInputFormatCli::SingleJson => ElasticInputFormat::SingleJson,
        }
    }
}

fn user_hit_ctrl_c(e: &anyhow::Error) -> bool {
    e.chain()
        .any(|c| c.to_string().as_str() == CTRL_C_INTERRUPT_MESSAGE)
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,librqbit=warn,librqbit_dht=warn")
            }),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::DbInit { path } => {
            let conn = open_db(&path)?;
            init_schema(&conn)?;
            println!("Initialized {}", path.display());
        }
        Command::IngestTorrents { db, from } => {
            let conn = open_db(&db)?;
            init_schema(&conn)?;
            let n = ingest_torrents_path(&conn, &from)?;
            println!("Ingested {n} torrents from {}", from.display());
        }
        Command::IngestElastic {
            db,
            from,
            format,
            skip_existing,
            no_skip_existing,
            replace,
            extension,
            language,
            ingest_batch_size,
            no_ingest_fast_pragmas,
        } => {
            let conn = open_db(&db)?;
            init_schema(&conn)?;
            let ext_filter = extension_filter_from_args(&extension);
            if let Some(ref set) = ext_filter {
                let mut v: Vec<_> = set.iter().cloned().collect();
                v.sort();
                println!("Elastic ingest: extension filter: {}", v.join(", "));
            }
            let lang_filter = language_filter_from_args(&language);
            if let Some(ref set) = lang_filter {
                let mut v: Vec<_> = set.iter().cloned().collect();
                v.sort();
                println!("Elastic ingest: language filter: {}", v.join(", "));
            }
            let ingest_opts = ElasticIngestOptions {
                batch_size: ingest_batch_size,
                fast_pragmas: !no_ingest_fast_pragmas,
            };
            let stats = ingest_elastic_path(
                &conn,
                &from,
                format.into(),
                skip_existing,
                no_skip_existing,
                replace,
                ext_filter.as_ref(),
                lang_filter.as_ref(),
                ingest_opts,
            )?;
            println!(
                "Elastic ingest: lines={} docs={} inserted={} skipped_existing={} skipped_extension={} skipped_language={} parse_errors={} join_hits={} join_misses={}",
                stats.lines_seen,
                stats.documents,
                stats.inserted,
                stats.skipped_existing,
                stats.skipped_extension,
                stats.skipped_language,
                stats.parse_errors,
                stats.join_torrent_hits,
                stats.join_torrent_misses
            );
        }
        Command::Search {
            db,
            query,
            limit,
            json,
        } => {
            let conn = open_db(&db)?;
            let query = query.join(" ");
            let hits = search_records(&conn, &query, limit)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&hits)?);
            } else {
                for h in &hits {
                    let size = h
                        .filesize
                        .filter(|&n| n >= 0)
                        .map(|n| format_byte_size_iec(n as u64))
                        .unwrap_or_else(|| "—".to_string());
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        h.id, size, h.extension, h.author, h.title
                    );
                }
                println!("Found {} hits", hits.len());
            }
        }
        Command::Show { db, id } => {
            let conn = open_db(&db)?;
            let rec = show_record(&conn, &id)?;
            match rec {
                Some(r) => println!("{}", serde_json::to_string_pretty(&r)?),
                None => eprintln!("Record not found"),
            }
        }
        Command::Download {
            db,
            id,
            output_dir,
            timeout_secs,
            yes,
            archive_dir,
        } => {
            let conn = open_db(&db)?;
            let Some(res) = resolve_download(&conn, &id)? else {
                anyhow::bail!(
                    "no download info for {} — need a record_files row with non-empty path_in_torrent and a magnet (ingest torrents + elastic; path comes from identifiers_unified.server_path when present)",
                    id
                );
            };
            let out = output_dir.as_path();
            if out == Path::new(".") || out == Path::new("./") {
                eprintln!("Downloading {} using torrent", res.record_id);
            } else {
                eprintln!(
                    "Downloading {} using torrent\n  into {}",
                    res.record_id,
                    output_dir.display()
                );
            }
            std::fs::create_dir_all(&output_dir)?;

            let rt = tokio::runtime::Runtime::new()?;
            let meta = match rt.block_on(fetch_torrent_download_meta_with_progress(&res.magnet)) {
                Ok(m) => m,
                Err(e) if user_hit_ctrl_c(&e) => {
                    eprintln!("Cancelled");
                    std::process::exit(130);
                }
                Err(e) => return Err(e),
            };
            let plan = plan_file_selection(
                &meta.info,
                res.path_in_torrent.as_str(),
                Some(res.record_id.as_str()),
            )?;

            let archive_download_dir: Option<PathBuf> = match &plan {
                DownloadPlan::NestedArchive {
                    container_torrent_path,
                    container_size_bytes,
                    inner_path,
                    ..
                } => {
                    eprintln!(
                        "Path {:?} is not a top-level file in the torrent; treating as a member inside archive:\n  {} ({} · {} bytes)",
                        inner_path,
                        container_torrent_path,
                        format_byte_size_iec(*container_size_bytes),
                        container_size_bytes
                    );
                    if yes {
                        let Some(ref p) = archive_dir else {
                            anyhow::bail!(
                                "nested archive download with --yes requires --archive-dir DIR"
                            );
                        };
                        Some(
                            p.canonicalize()
                                .with_context(|| format!("canonicalize {}", p.display()))?,
                        )
                    } else if !io::stdin().is_terminal() {
                        anyhow::bail!(
                            "nested archive download in non-interactive mode requires --yes and --archive-dir DIR"
                        );
                    } else {
                        print!(
                            "Download that archive ({}) and extract {:?} into {}? [y/N] ",
                            format_byte_size_iec(*container_size_bytes),
                            inner_path,
                            output_dir.display()
                        );
                        io::stdout().flush()?;
                        let mut line = String::new();
                        io::stdin().read_line(&mut line)?;
                        if !matches!(line.trim().to_lowercase().as_str(), "y" | "yes") {
                            println!("Cancelled");
                            return Ok(());
                        }
                        let p = if let Some(ref p) = archive_dir {
                            p.clone()
                        } else {
                            print!("Directory for the .tar download: ");
                            io::stdout().flush()?;
                            line.clear();
                            io::stdin().read_line(&mut line)?;
                            PathBuf::from(line.trim())
                        };
                        if p.as_os_str().is_empty() {
                            anyhow::bail!("archive directory path is empty");
                        }
                        std::fs::create_dir_all(&p).with_context(|| format!("{}", p.display()))?;
                        Some(
                            p.canonicalize()
                                .with_context(|| format!("canonicalize {}", p.display()))?,
                        )
                    }
                }
                DownloadPlan::Direct { .. } => {
                    if archive_dir.is_some() {
                        eprintln!("Note: --archive-dir is only used for nested .tar downloads; ignoring");
                    }
                    None
                }
            };

            let paths = match rt.block_on(execute_download_plan(
                meta,
                &output_dir,
                plan,
                res.extension.as_deref(),
                Duration::from_secs(timeout_secs),
                archive_download_dir.as_deref(),
                Some(res.record_id.as_str()),
            )) {
                Ok(p) => p,
                Err(e) if user_hit_ctrl_c(&e) => {
                    eprintln!("Cancelled");
                    std::process::exit(130);
                }
                Err(e) => return Err(e),
            };
            verify_download_against_record_md5(&paths, Some(res.record_id.as_str()))?;
            println!("Download complete");
        }
    }
    Ok(())
}

#[cfg(test)]
mod cli_tests {
    use super::{Cli, Command};
    use clap::Parser;

    #[test]
    fn search_query_accepts_multiple_positional_words() {
        let cli = Cli::try_parse_from(["alexandria", "search", "nineteen", "eighty-four"])
            .expect("parse");
        match cli.command {
            Command::Search { query, .. } => assert_eq!(query.join(" "), "nineteen eighty-four"),
            _ => panic!("expected Search"),
        }
    }

    #[test]
    fn search_query_positional_after_options() {
        let cli = Cli::try_parse_from([
            "alexandria",
            "search",
            "--limit",
            "5",
            "nineteen",
            "eighty-four",
        ])
        .expect("parse");
        match cli.command {
            Command::Search { query, limit, .. } => {
                assert_eq!(limit, 5);
                assert_eq!(query.join(" "), "nineteen eighty-four");
            }
            _ => panic!("expected Search"),
        }
    }

    #[test]
    fn search_query_single_quoted_argv() {
        let cli = Cli::try_parse_from(["alexandria", "search", "nineteen eighty-four"])
            .expect("parse");
        match cli.command {
            Command::Search { query, .. } => assert_eq!(query.join(" "), "nineteen eighty-four"),
            _ => panic!("expected Search"),
        }
    }
}
