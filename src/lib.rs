pub mod db;
pub mod download;
pub mod elastic;
pub mod io_util;
pub mod isbn;
pub mod oclc;
pub mod search;
pub mod torrents;

pub use db::{
    any_records_exist, init_schema, normalize_record_id, open_db, DbError, FastIngestPragmas,
};
pub use download::{
    download_to_dir, execute_download_plan, extract_matching_tar_member,
    fetch_torrent_download_meta, format_byte_size_iec, file_md5_hex, plan_file_selection,
    resolve_download, verify_download_against_record_md5, DownloadPlan, ResolvedDownload,
    TorrentDownloadMeta,
};
pub use elastic::{
    extension_filter_from_args, ingest_elastic_path, language_filter_from_args,
    shard_id_from_aarecords_filename, torrent_display_name_from_es_path, ElasticIngestOptions,
    ElasticIngestStats, ElasticInputFormat,
};
pub use isbn::{
    isbn10_to_isbn13, normalize_isbn13_str, record_isbn13_set_from_file_unified,
    record_isbn13_set_from_identifiers, try_user_query_as_isbn13_digits,
};
pub use oclc::{
    normalize_oclc_token, record_oclc_set_from_identifiers_unified, try_user_query_as_oclc,
};
pub use search::{search_records, show_record, RecordDetail, RecordFile, SearchHit};
pub use torrents::ingest_torrents_path;
