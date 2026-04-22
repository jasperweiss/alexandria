use crate::db::upsert_torrent;
use anyhow::Context;
use rusqlite::Connection;
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct TorrentJson {
    pub url: String,
    pub display_name: String,
    pub btih: String,
    pub magnet_link: String,
    #[serde(default)]
    pub data_size: Option<i64>,
    #[serde(default)]
    pub seeders: Option<i64>,
    #[serde(default)]
    pub leechers: Option<i64>,
    #[serde(default)]
    pub num_files: Option<i64>,
    #[serde(default)]
    pub torrent_size: Option<i64>,
}

pub fn ingest_torrents_path(conn: &Connection, path: &Path) -> anyhow::Result<usize> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    let list: Vec<TorrentJson> = serde_json::from_str(&text).context("parse torrents JSON")?;
    let tx = conn.unchecked_transaction()?;
    let mut n = 0usize;
    for t in list {
        upsert_torrent(
            &tx,
            &t.display_name,
            &t.btih,
            Some(t.magnet_link.as_str()),
            Some(t.url.as_str()),
            t.data_size,
            t.seeders,
            t.leechers,
            t.num_files,
            t.torrent_size,
            None,
        )?;
        n += 1;
    }
    tx.commit()?;
    Ok(n)
}
