use crate::db::normalize_record_id;
use crate::isbn::try_user_query_as_isbn13_digits;
use crate::oclc::try_user_query_as_oclc;
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    /// Record id (Anna's Archive md5 / normalized id).
    pub id: String,
    /// `records.filesize` in bytes, if present.
    pub filesize: Option<i64>,
    pub extension: String,
    pub author: String,
    pub title: String,
}

/// Build an FTS5 MATCH query: tokenize on whitespace, phrase-quote each token.
pub fn fts_match_query(user_query: &str) -> String {
    let parts: Vec<&str> = user_query.split_whitespace().filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return "*".to_string();
    }
    parts
        .iter()
        .map(|t| format!("\"{}\"", t.replace('\"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" AND ")
}

pub fn search_records(
    conn: &Connection,
    query: &str,
    limit: usize,
) -> anyhow::Result<Vec<SearchHit>> {
    if let Some(isbn13) = try_user_query_as_isbn13_digits(query) {
        let mut stmt = conn.prepare(
            r#"
            SELECT r.id, r.filesize, r.extension, r.author, r.title
            FROM record_isbns ri
            JOIN records r ON r.id = ri.record_id
            WHERE ri.isbn13 = ?1
            ORDER BY r.id
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(rusqlite::params![isbn13, limit as i64], |row| {
            Ok(SearchHit {
                id: row.get(0)?,
                filesize: row.get(1)?,
                extension: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                author: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                title: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        return Ok(out);
    }

    if let Some(oclc) = try_user_query_as_oclc(query) {
        let mut stmt = conn.prepare(
            r#"
            SELECT r.id, r.filesize, r.extension, r.author, r.title
            FROM record_oclc ro
            JOIN records r ON r.id = ro.record_id
            WHERE ro.oclc = ?1
            ORDER BY r.id
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(rusqlite::params![oclc, limit as i64], |row| {
            Ok(SearchHit {
                id: row.get(0)?,
                filesize: row.get(1)?,
                extension: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                author: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                title: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        return Ok(out);
    }

    let fts = fts_match_query(query);
    let mut stmt = conn.prepare(
        r#"
        SELECT r.id, r.filesize, r.extension, r.author, r.title
        FROM records_fts
        JOIN records r ON r.rowid = records_fts.rowid
        WHERE records_fts MATCH ?1
        ORDER BY bm25(records_fts)
        LIMIT ?2
        "#,
    )?;
    let rows = stmt.query_map(rusqlite::params![fts, limit as i64], |row| {
        Ok(SearchHit {
            id: row.get(0)?,
            filesize: row.get(1)?,
            extension: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            author: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
            title: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

pub fn show_record(
    conn: &Connection,
    id: &str,
) -> anyhow::Result<Option<RecordDetail>> {
    let id = normalize_record_id(id);
    let rec = conn
        .query_row(
            r#"SELECT id, title, author, year, extension, content_type, filesize, language
               FROM records WHERE id = ?1"#,
            [&id],
            |row| {
                Ok(RecordDetail {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    author: row.get(2)?,
                    year: row.get(3)?,
                    extension: row.get(4)?,
                    content_type: row.get(5)?,
                    filesize: row.get(6)?,
                    language: row.get(7)?,
                    isbn13s: Vec::new(),
                    oclcs: Vec::new(),
                    files: Vec::new(),
                })
            },
        )
        .optional()?;

    let Some(mut rec) = rec else {
        return Ok(None);
    };

    let mut stmt = conn.prepare(
        r#"SELECT torrent_display_name, path_in_torrent, filesize_bytes, btih, magnet_link
           FROM record_files WHERE record_id = ?1 ORDER BY torrent_display_name, path_in_torrent"#,
    )?;
    let files = stmt.query_map([&id], |row| {
        Ok(RecordFile {
            torrent_display_name: row.get::<_, String>(0)?,
            path_in_torrent: row.get::<_, String>(1)?,
            filesize_bytes: row.get(2)?,
            btih: row.get(3)?,
            magnet_link: row.get(4)?,
        })
    })?;
    for f in files {
        rec.files.push(f?);
    }

    let mut stmt = conn
        .prepare("SELECT isbn13 FROM record_isbns WHERE record_id = ?1 ORDER BY isbn13")?;
    let isbns = stmt.query_map([&id], |row| row.get::<_, String>(0))?;
    for v in isbns {
        rec.isbn13s.push(v?);
    }

    let mut stmt = conn
        .prepare("SELECT oclc FROM record_oclc WHERE record_id = ?1 ORDER BY oclc")?;
    let oclcs = stmt.query_map([&id], |row| row.get::<_, String>(0))?;
    for v in oclcs {
        rec.oclcs.push(v?);
    }

    Ok(Some(rec))
}

#[derive(Debug, Clone, Serialize)]
pub struct RecordFile {
    pub torrent_display_name: String,
    pub path_in_torrent: String,
    pub filesize_bytes: Option<i64>,
    pub btih: Option<String>,
    pub magnet_link: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecordDetail {
    pub id: String,
    pub title: Option<String>,
    pub author: Option<String>,
    pub year: Option<String>,
    pub extension: Option<String>,
    pub content_type: Option<String>,
    pub filesize: Option<i64>,
    pub language: Option<String>,
    /// Canonical 13-digit ISBNs from ingest (and ISBN-10 converted to 978…).
    pub isbn13s: Vec<String>,
    /// OCLC control numbers from `identifiers_unified.oclc`.
    pub oclcs: Vec<String>,
    pub files: Vec<RecordFile>,
}
