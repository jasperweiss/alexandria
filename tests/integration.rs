use alexandria::{
    elastic::{
        ingest_elastic_path, torrent_display_name_from_es_path, ElasticIngestOptions,
        ElasticInputFormat,
    },
    init_schema, ingest_torrents_path, open_db, resolve_download, search_records, show_record,
};
use std::path::Path;

#[test]
fn torrent_display_name_basename() {
    assert_eq!(
        torrent_display_name_from_es_path(
            "managed_by_aa/fixture_bins/fixture_bins__batch_0001-0009.torrent"
        )
        .as_deref(),
        Some("fixture_bins__batch_0001-0009.torrent")
    );
    assert_eq!(torrent_display_name_from_es_path("not-a-torrent"), None);
}

#[test]
fn ingest_sample_joins_torrent_metadata() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("test.db");
    let conn = open_db(&db_path).expect("open");
    init_schema(&conn).expect("schema");

    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    ingest_torrents_path(
        &conn,
        &fixtures.join("torrents_sample.json"),
    )
    .expect("torrents");

    ingest_elastic_path(
        &conn,
        &fixtures.join("elastic_sample.json"),
        ElasticInputFormat::SingleJson,
        false,
        false,
        false,
        None,
        None,
        ElasticIngestOptions::default(),
    )
    .expect("elastic");

    let hits = search_records(&conn, "QuokkaFixture", 5).expect("search");
    assert!(!hits.is_empty(), "expected search hit for fixture title token");

    let isbn_hits = search_records(&conn, "123456789X", 5).expect("isbn search");
    assert!(
        isbn_hits.iter().any(|h| h.id == "a1b2c3d4e5f6789012345678abcdef01"),
        "expected ISBN-10 query to resolve via record_isbns, got {:?}",
        isbn_hits
    );

    let oclc_hits = search_records(&conn, "999888777", 5).expect("oclc search");
    assert!(
        oclc_hits.iter().any(|h| h.id == "a1b2c3d4e5f6789012345678abcdef01"),
        "expected OCLC query to resolve via record_oclc, got {:?}",
        oclc_hits
    );
    let id = &hits[0].id;

    let detail = show_record(&conn, id).expect("show").expect("record");
    assert!(
        detail.oclcs.contains(&"999888777".to_string()),
        "expected OCLC on record, got {:?}",
        detail.oclcs
    );
    assert!(
        detail.files.iter().any(|f| {
            f.path_in_torrent
                .contains("aacid__fixture_inner__20200101T000000Z__00deadbeef00")
        }),
        "expected identifiers_unified.server_path basename, got {:?}",
        detail.files.iter().map(|f| &f.path_in_torrent).collect::<Vec<_>>()
    );
}

#[test]
fn resolve_download_requires_magnet_row() {
    let conn = rusqlite::Connection::open_in_memory().expect("mem");
    init_schema(&conn).expect("schema");
    assert!(resolve_download(&conn, "nope").expect("q").is_none());
}
