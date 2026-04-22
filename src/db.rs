use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
}

pub fn open_db(path: &Path) -> Result<Connection, DbError> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;")?;
    Ok(conn)
}

/// Applies faster SQLite settings for bulk ingest; restores previous values on drop.
pub struct FastIngestPragmas<'a> {
    conn: &'a Connection,
    synchronous: i32,
    cache_size: i64,
    temp_store: i64,
}

impl<'a> FastIngestPragmas<'a> {
    pub fn apply(conn: &'a Connection) -> Result<Self, DbError> {
        let synchronous: i32 = conn.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
        let cache_size: i64 = conn.query_row("PRAGMA cache_size", [], |r| r.get(0))?;
        let temp_store: i64 = conn.query_row("PRAGMA temp_store", [], |r| r.get(0))?;
        conn.execute_batch(
            "PRAGMA synchronous=NORMAL; PRAGMA cache_size=-200000; PRAGMA temp_store=MEMORY;",
        )?;
        Ok(Self {
            conn,
            synchronous,
            cache_size,
            temp_store,
        })
    }
}

impl Drop for FastIngestPragmas<'_> {
    fn drop(&mut self) {
        let sql = format!(
            "PRAGMA synchronous={}; PRAGMA cache_size={}; PRAGMA temp_store={};",
            self.synchronous, self.cache_size, self.temp_store
        );
        let _ = self.conn.execute_batch(&sql);
    }
}

/// Strip a leading `md5:` prefix (ASCII case-insensitive) from Anna's Archive record ids.
pub fn normalize_record_id(id: &str) -> String {
    let id = id.trim();
    let prefix = b"md5:";
    let b = id.as_bytes();
    if b.len() > prefix.len() && b[..prefix.len()].eq_ignore_ascii_case(prefix) {
        id[prefix.len()..].to_string()
    } else {
        id.to_string()
    }
}

pub fn init_schema(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS torrents (
            display_name TEXT PRIMARY KEY,
            btih TEXT NOT NULL,
            magnet_link TEXT,
            torrent_url TEXT,
            data_size INTEGER,
            seeders INTEGER,
            leechers INTEGER,
            num_files INTEGER,
            torrent_size INTEGER,
            json_extra TEXT
        );

        CREATE TABLE IF NOT EXISTS records (
            id TEXT PRIMARY KEY,
            title TEXT,
            author TEXT,
            year TEXT,
            extension TEXT,
            content_type TEXT,
            filesize INTEGER,
            language TEXT,
            search_text TEXT
        );

        CREATE TABLE IF NOT EXISTS record_files (
            record_id TEXT NOT NULL,
            torrent_display_name TEXT NOT NULL,
            path_in_torrent TEXT NOT NULL,
            filesize_bytes INTEGER,
            btih TEXT,
            magnet_link TEXT,
            PRIMARY KEY (record_id, torrent_display_name, path_in_torrent),
            FOREIGN KEY (record_id) REFERENCES records(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_record_files_record ON record_files(record_id);

        CREATE TABLE IF NOT EXISTS record_isbns (
            isbn13 TEXT PRIMARY KEY,
            record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_record_isbns_record_id ON record_isbns(record_id);

        CREATE TABLE IF NOT EXISTS record_oclc (
            oclc TEXT PRIMARY KEY,
            record_id TEXT NOT NULL REFERENCES records(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_record_oclc_record_id ON record_oclc(record_id);

        CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(
            title,
            author,
            search_text,
            content='records',
            content_rowid='rowid',
            tokenize='unicode61 remove_diacritics 1'
        );

        CREATE TRIGGER IF NOT EXISTS records_ai_fts AFTER INSERT ON records BEGIN
            INSERT INTO records_fts(rowid, title, author, search_text)
            VALUES (new.rowid, new.title, new.author, new.search_text);
        END;

        CREATE TRIGGER IF NOT EXISTS records_ad_fts AFTER DELETE ON records BEGIN
            INSERT INTO records_fts(records_fts, rowid) VALUES('delete', old.rowid);
        END;

        CREATE TRIGGER IF NOT EXISTS records_au_fts AFTER UPDATE ON records BEGIN
            INSERT INTO records_fts(records_fts, rowid) VALUES('delete', old.rowid);
            INSERT INTO records_fts(rowid, title, author, search_text)
            VALUES (new.rowid, new.title, new.author, new.search_text);
        END;
        "#,
    )?;
    // Drop legacy column if present (SQLite 3.35+). Ignore error on fresh DBs or already migrated.
    let _ = conn.execute("ALTER TABLE records DROP COLUMN raw_json", []);
    Ok(())
}

/// Delete a record and dependent rows (`record_files`, `record_isbns`, `record_oclc` via CASCADE; FTS via trigger).
pub fn delete_record_cascade(conn: &Connection, id: &str) -> Result<(), DbError> {
    let id = normalize_record_id(id);
    conn.execute("DELETE FROM records WHERE id = ?1", [&id])?;
    Ok(())
}

pub fn record_exists(conn: &Connection, id: &str) -> Result<bool, DbError> {
    let id = normalize_record_id(id);
    let n: i64 = conn.query_row(
        "SELECT COUNT(1) FROM records WHERE id = ?1",
        [&id],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

/// True if `records` has at least one row (cheap `LIMIT 1` probe).
pub fn any_records_exist(conn: &Connection) -> Result<bool, DbError> {
    match conn.query_row("SELECT 1 FROM records LIMIT 1", [], |_| Ok(())) {
        Ok(()) => Ok(true),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

pub fn upsert_torrent(
    conn: &Connection,
    display_name: &str,
    btih: &str,
    magnet_link: Option<&str>,
    torrent_url: Option<&str>,
    data_size: Option<i64>,
    seeders: Option<i64>,
    leechers: Option<i64>,
    num_files: Option<i64>,
    torrent_size: Option<i64>,
    json_extra: Option<&str>,
) -> Result<(), DbError> {
    conn.execute(
        r#"
        INSERT INTO torrents (
            display_name, btih, magnet_link, torrent_url, data_size,
            seeders, leechers, num_files, torrent_size, json_extra
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(display_name) DO UPDATE SET
            btih = excluded.btih,
            magnet_link = COALESCE(excluded.magnet_link, torrents.magnet_link),
            torrent_url = COALESCE(excluded.torrent_url, torrents.torrent_url),
            data_size = COALESCE(excluded.data_size, torrents.data_size),
            seeders = COALESCE(excluded.seeders, torrents.seeders),
            leechers = COALESCE(excluded.leechers, torrents.leechers),
            num_files = COALESCE(excluded.num_files, torrents.num_files),
            torrent_size = COALESCE(excluded.torrent_size, torrents.torrent_size),
            json_extra = COALESCE(excluded.json_extra, torrents.json_extra)
        "#,
        params![
            display_name,
            btih,
            magnet_link,
            torrent_url,
            data_size,
            seeders,
            leechers,
            num_files,
            torrent_size,
            json_extra
        ],
    )?;
    Ok(())
}

pub fn lookup_torrent_btih(conn: &Connection, display_name: &str) -> Result<Option<String>, DbError> {
    conn.query_row(
        "SELECT btih FROM torrents WHERE display_name = ?1",
        [display_name],
        |r| r.get::<_, String>(0),
    )
    .optional()
    .map_err(Into::into)
}

pub fn lookup_torrent_magnet(conn: &Connection, display_name: &str) -> Result<Option<String>, DbError> {
    match conn.query_row(
        "SELECT magnet_link FROM torrents WHERE display_name = ?1",
        [display_name],
        |r| r.get::<_, Option<String>>(0),
    ) {
        Ok(v) => Ok(v),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_record_id_strips_md5_prefix() {
        assert_eq!(
            normalize_record_id("md5:a1b2c3d4e5f6789012345678abcdef01"),
            "a1b2c3d4e5f6789012345678abcdef01"
        );
        assert_eq!(
            normalize_record_id("MD5:a1b2c3d4e5f6789012345678abcdef01"),
            "a1b2c3d4e5f6789012345678abcdef01"
        );
        assert_eq!(normalize_record_id("  md5:abc  "), "abc");
        assert_eq!(
            normalize_record_id("a1b2c3d4e5f6789012345678abcdef01"),
            "a1b2c3d4e5f6789012345678abcdef01"
        );
        assert_eq!(normalize_record_id("sha1:deadbeef"), "sha1:deadbeef");
    }
}
