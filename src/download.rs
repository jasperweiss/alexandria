use crate::db::normalize_record_id;
use anyhow::Context;
use bytes::Bytes;
use flate2::read::GzDecoder;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, ByteBufOwned, ListOnlyResponse, Session,
    ValidatedTorrentMetaV1Info,
};
use md5::Context as Md5Context;
use rusqlite::Connection;
use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use tar::Archive;

#[derive(Debug, Clone)]
pub struct ResolvedDownload {
    pub record_id: String,
    pub magnet: String,
    pub torrent_display_name: String,
    pub path_in_torrent: String,
    /// From `records.extension` (e.g. `pdf`); used to rename extensionless downloads.
    pub extension: Option<String>,
}

/// Result of `list_only` fetch: torrent bytes for a second `add_torrent` plus parsed info.
#[derive(Clone)]
pub struct TorrentDownloadMeta {
    pub torrent_bytes: Bytes,
    pub info: ValidatedTorrentMetaV1Info<ByteBufOwned>,
}

/// Whether to download torrent files directly or fetch a container archive and extract one member.
#[derive(Debug, Clone)]
pub enum DownloadPlan {
    Direct { indices: Vec<usize> },
    NestedArchive {
        container_index: usize,
        container_torrent_path: String,
        /// Declared size of the container file in the torrent (for prompts / UI).
        container_size_bytes: u64,
        inner_path: String,
    },
}

/// Human-readable byte size (IEC: KiB, MiB, …) for CLI messages.
pub fn format_byte_size_iec(n: u64) -> String {
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

/// Pick the first record_files row with a magnet link.
pub fn resolve_download(conn: &Connection, record_id: &str) -> anyhow::Result<Option<ResolvedDownload>> {
    let record_id = normalize_record_id(record_id);
    let mut stmt = conn.prepare(
        r#"SELECT rf.torrent_display_name, rf.path_in_torrent, rf.magnet_link, rf.btih, r.extension
           FROM record_files rf
           JOIN records r ON r.id = rf.record_id
           WHERE rf.record_id = ?1
           ORDER BY (rf.magnet_link IS NOT NULL) DESC, rf.torrent_display_name"#,
    )?;
    let mut rows = stmt.query([&record_id])?;
    while let Some(row) = rows.next()? {
        let torrent_display_name: String = row.get(0)?;
        let path_in_torrent: String = row.get(1)?;
        if path_in_torrent.trim().is_empty() {
            continue;
        }
        let magnet_link: Option<String> = row.get(2)?;
        let _btih: Option<String> = row.get(3)?;
        let extension: Option<String> = row.get(4)?;
        let extension = extension
            .map(|s| s.trim().trim_start_matches('.').to_lowercase())
            .filter(|s| !s.is_empty());
        let Some(magnet) = magnet_link else { continue };

        return Ok(Some(ResolvedDownload {
            record_id: record_id.clone(),
            magnet,
            torrent_display_name,
            path_in_torrent,
            extension,
        }));
    }
    Ok(None)
}

/// Lowercase hex MD5 (32 chars) when `id` is an Anna's Archive–style md5 record id.
fn record_id_expected_md5_hex(id: &str) -> Option<String> {
    let h = normalize_record_id(id);
    if h.len() == 32 && h.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(h.to_ascii_lowercase())
    } else {
        None
    }
}

/// Streaming MD5 of a file as lowercase hex.
pub fn file_md5_hex(path: &Path) -> anyhow::Result<String> {
    let mut f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut ctx = Md5Context::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).context("read file for MD5")?;
        if n == 0 {
            break;
        }
        ctx.consume(&buf[..n]);
    }
    Ok(format!("{:x}", ctx.compute()))
}

/// After download, compare file MD5 to the record id when it is a 32-hex md5 id.
/// Skips when `record_id` is missing or not md5-shaped. With multiple delivered files, skips with a warning.
pub fn verify_download_against_record_md5(
    paths: &[PathBuf],
    record_id: Option<&str>,
) -> anyhow::Result<()> {
    let Some(raw) = record_id else {
        return Ok(());
    };
    let Some(expected) = record_id_expected_md5_hex(raw) else {
        return Ok(());
    };
    if paths.is_empty() {
        anyhow::bail!("MD5 verify: no output files recorded");
    }
    if paths.len() > 1 {
        eprintln!(
            "warning: skipping MD5 check ({} files); record id {}",
            paths.len(),
            expected
        );
        return Ok(());
    }
    let path = &paths[0];
    if !path.is_file() {
        anyhow::bail!("MD5 verify: {} is not a file", path.display());
    }
    let got = file_md5_hex(path)?;
    if got != expected {
        anyhow::bail!(
            "MD5 mismatch for {}: got {} expected {} (from record id)",
            path.display(),
            got,
            expected
        );
    }
    println!("MD5 OK ({})", got);
    Ok(())
}

fn pick_file_indices(
    info: &ValidatedTorrentMetaV1Info<ByteBufOwned>,
    want: &str,
    record_id: Option<&str>,
) -> anyhow::Result<Vec<usize>> {
    let want = want.trim();
    if want.is_empty() {
        return Ok(Vec::new());
    }
    let want_norm = want.replace('\\', "/").trim_end_matches('/').to_string();
    let want_lc = want_norm.to_lowercase();
    let basename = Path::new(want)
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(want);
    let basename_lc = basename.to_lowercase();
    let needle = record_id.and_then(|rid| {
        let h = normalize_record_id(rid);
        if h.len() >= 8 {
            Some(h)
        } else {
            None
        }
    });

    let mut exact = Vec::new();
    let mut dir_prefix = Vec::new();
    let mut end_match = Vec::new();
    let mut sub_match = Vec::new();
    let mut md5_match = Vec::new();

    let dir_prefix_pat = format!("{}/", want_norm);
    let dir_prefix_pat_lc = format!("{}/", want_lc);

    for (i, fd) in info.iter_file_details().enumerate() {
        let tp = fd.filename.to_string();
        let tp_norm = tp.replace('\\', "/");
        let tp_lc = tp_norm.to_lowercase();
        if tp == want || tp_norm == want_norm || tp_lc == want_lc {
            exact.push(i);
            continue;
        }
        if tp_norm.starts_with(&dir_prefix_pat) || tp_lc.starts_with(&dir_prefix_pat_lc) {
            dir_prefix.push(i);
            continue;
        }
        if tp_lc.ends_with(&format!("/{basename_lc}")) || tp_lc == basename_lc {
            end_match.push(i);
            continue;
        }
        if tp_lc.contains(&basename_lc) {
            sub_match.push(i);
            continue;
        }
        if let Some(ref n) = needle {
            if tp.contains(n.as_str()) {
                md5_match.push(i);
            }
        }
    }

    if !exact.is_empty() {
        return Ok(exact);
    }
    if !dir_prefix.is_empty() {
        return Ok(dir_prefix);
    }
    for bucket in [&end_match, &sub_match, &md5_match] {
        if let Some(&i) = bucket.first() {
            return Ok(vec![i]);
        }
    }

    anyhow::bail!("no file in torrent metadata matches path {want:?}");
}

fn torrent_path_is_archive_container(tp: &str) -> bool {
    let tp_lc = tp.replace('\\', "/").to_lowercase();
    let base = Path::new(&tp_lc)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    base.ends_with(".tar") || base.ends_with(".tar.gz") || base.ends_with(".tgz")
}

fn collect_archive_container_candidates(
    info: &ValidatedTorrentMetaV1Info<ByteBufOwned>,
) -> Vec<(usize, String)> {
    let mut v = Vec::new();
    for (i, fd) in info.iter_file_details().enumerate() {
        if fd.attrs().padding {
            continue;
        }
        let tp = fd.filename.to_string();
        if torrent_path_is_archive_container(&tp) {
            v.push((i, tp));
        }
    }
    v
}

fn pick_one_archive_candidate(candidates: Vec<(usize, String)>) -> anyhow::Result<(usize, String)> {
    match candidates.len() {
        0 => anyhow::bail!("no archive container in torrent"),
        1 => Ok(candidates.into_iter().next().unwrap()),
        _ => {
            let hints: Vec<_> = candidates
                .iter()
                .filter(|(_, p)| {
                    let l = p.to_lowercase();
                    l.contains("pilimi") || l.contains("zlib")
                })
                .cloned()
                .collect();
            if hints.len() == 1 {
                Ok(hints.into_iter().next().unwrap())
            } else {
                let lines: Vec<_> = candidates.iter().map(|(_, p)| p.as_str()).collect();
                anyhow::bail!(
                    "multiple archive files in torrent; cannot choose automatically:\n{}",
                    lines.join("\n")
                )
            }
        }
    }
}

/// Decide direct file selection vs nested archive (path refers to a member inside a `.tar`).
pub fn plan_file_selection(
    info: &ValidatedTorrentMetaV1Info<ByteBufOwned>,
    path_in_torrent: &str,
    record_id: Option<&str>,
) -> anyhow::Result<DownloadPlan> {
    let p = path_in_torrent.trim();
    if p.is_empty() {
        anyhow::bail!("path_in_torrent is empty");
    }
    match pick_file_indices(info, p, record_id) {
        Ok(indices) if indices.is_empty() => {
            anyhow::bail!("no files selected for path {p:?}")
        }
        Ok(indices) => Ok(DownloadPlan::Direct { indices }),
        Err(e) => {
            let c = collect_archive_container_candidates(info);
            if c.is_empty() {
                return Err(e);
            }
            let (container_index, container_torrent_path) = pick_one_archive_candidate(c)?;
            let container_size_bytes = info
                .iter_file_details()
                .enumerate()
                .find(|(i, _)| *i == container_index)
                .map(|(_, fd)| fd.len)
                .unwrap_or(0);
            Ok(DownloadPlan::NestedArchive {
                container_index,
                container_torrent_path,
                container_size_bytes,
                inner_path: p.to_string(),
            })
        }
    }
}

/// Fetch torrent metadata (`list_only`) from a magnet URI.
pub async fn fetch_torrent_download_meta(magnet: &str) -> anyhow::Result<TorrentDownloadMeta> {
    let tmp = tempfile::tempdir_in(std::env::temp_dir()).context("tempdir for torrent list_only")?;
    let scratch = tmp.path().canonicalize().context("canonicalize list_only scratch")?;
    let session = Session::new(scratch.clone())
        .await
        .context("create librqbit session for list_only")?;

    let list_resp = session
        .add_torrent(
            AddTorrent::from_url(magnet),
            Some(AddTorrentOptions {
                list_only: true,
                ..Default::default()
            }),
        )
        .await
        .context("fetch torrent metadata (list_only)")?;

    session.stop().await;

    let ListOnlyResponse {
        info,
        torrent_bytes,
        ..
    } = match list_resp {
        AddTorrentResponse::ListOnly(l) => l,
        AddTorrentResponse::Added(_, _) | AddTorrentResponse::AlreadyManaged(_, _) => {
            anyhow::bail!("expected list_only response")
        }
    };

    Ok(TorrentDownloadMeta {
        torrent_bytes,
        info,
    })
}

fn is_cross_device_rename(err: &io::Error) -> bool {
    #[cfg(unix)]
    {
        err.raw_os_error() == Some(18) // EXDEV
    }
    #[cfg(windows)]
    {
        err.raw_os_error() == Some(17) // ERROR_NOT_SAME_DEVICE
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = err;
        false
    }
}

fn move_or_copy_file(src: &Path, dest: &Path) -> anyhow::Result<()> {
    match std::fs::rename(src, dest) {
        Ok(()) => Ok(()),
        Err(e) if is_cross_device_rename(&e) => {
            std::fs::copy(src, dest).with_context(|| {
                format!(
                    "copy {} -> {} (cross-filesystem move)",
                    src.display(),
                    dest.display()
                )
            })?;
            std::fs::remove_file(src).with_context(|| {
                format!("remove temp file after copy {}", src.display())
            })?;
            Ok(())
        }
        Err(e) => Err(e).with_context(|| {
            format!("move {} -> {}", src.display(), dest.display())
        }),
    }
}

/// Move completed downloads from `scratch_dir` into `final_output`,
/// preserving torrent-relative paths.
fn move_selected_to_final(
    scratch_dir: &Path,
    final_output: &Path,
    info: &ValidatedTorrentMetaV1Info<ByteBufOwned>,
    selected: &[usize],
) -> anyhow::Result<()> {
    let keep: HashSet<usize> = selected.iter().copied().collect();
    for (idx, fd) in info.iter_file_details().enumerate() {
        if !keep.contains(&idx) || fd.attrs().padding {
            continue;
        }
        let rel = fd.filename.to_pathbuf();
        let src = scratch_dir.join(&rel);
        if !src.is_file() {
            continue;
        }
        let dest = final_output.join(&rel);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("create directory {}", parent.display())
            })?;
        }
        if dest.exists() {
            anyhow::bail!(
                "refusing to overwrite existing {}",
                dest.display()
            );
        }
        move_or_copy_file(&src, &dest)?;
    }
    Ok(())
}

fn normalized_preferred_extension(preferred_extension: Option<&str>) -> Option<String> {
    preferred_extension
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|e| e.trim_start_matches('.').to_lowercase())
        .filter(|s| !s.is_empty())
}

fn apply_preferred_extensions(
    output_dir: &Path,
    info: &ValidatedTorrentMetaV1Info<ByteBufOwned>,
    selected: &[usize],
    preferred_extension: Option<&str>,
    preferred_name_record_id: Option<&str>,
) -> anyhow::Result<Vec<PathBuf>> {
    let keep: HashSet<usize> = selected.iter().copied().collect();

    let mut paths = Vec::new();
    for (idx, fd) in info.iter_file_details().enumerate() {
        if !keep.contains(&idx) || fd.attrs().padding {
            continue;
        }
        let rel = fd.filename.to_pathbuf();
        let abs = output_dir.join(&rel);
        if !abs.is_file() {
            continue;
        }
        let final_path =
            finalize_download_file_name(&abs, preferred_extension, preferred_name_record_id)?;
        paths.push(final_path);
    }
    Ok(paths)
}

/// When `preferred_name_record_id` is set, rename to `{normalize_record_id(id)}[.ext]` in the same
/// directory: use normalized preferred extension if set, otherwise the file's current extension
/// (if any). When id is unset, only apply preferred extension using the original stem (legacy).
fn finalize_download_file_name(
    abs: &Path,
    preferred_extension: Option<&str>,
    preferred_name_record_id: Option<&str>,
) -> anyhow::Result<PathBuf> {
    let parent = abs
        .parent()
        .filter(|p| *p != Path::new(""))
        .unwrap_or_else(|| abs.parent().unwrap_or(Path::new(".")));

    let ext_pref = normalized_preferred_extension(preferred_extension);

    if let Some(id_raw) = preferred_name_record_id.map(str::trim).filter(|s| !s.is_empty()) {
        let id = normalize_record_id(id_raw);
        if !id.is_empty() {
            let ext_slice: &str = if let Some(ref pe) = ext_pref {
                pe.as_str()
            } else {
                abs.extension().and_then(|e| e.to_str()).unwrap_or("")
            };
            let dest = if ext_slice.is_empty() {
                parent.join(&id)
            } else {
                parent.join(format!("{id}.{ext_slice}"))
            };
            if dest == abs {
                return Ok(abs.to_path_buf());
            }
            if dest.exists() {
                anyhow::bail!(
                    "cannot rename {} to {}: destination exists",
                    abs.display(),
                    dest.display()
                );
            }
            std::fs::rename(abs, &dest).with_context(|| {
                format!(
                    "rename {} -> {}",
                    abs.display(),
                    dest.display()
                )
            })?;
            return Ok(dest);
        }
    }

    let Some(ext) = ext_pref else {
        return Ok(abs.to_path_buf());
    };
    let ext = ext.as_str();
    let current = abs
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_lowercase());
    if current.as_deref() == Some(ext) {
        return Ok(abs.to_path_buf());
    }

    let mut new_name = OsString::from(abs.file_stem().unwrap_or_default());
    new_name.push(".");
    new_name.push(ext);
    let dest = parent.join(new_name);
    if dest == abs {
        return Ok(abs.to_path_buf());
    }
    if dest.exists() {
        anyhow::bail!(
            "cannot rename {} to {}: destination exists",
            abs.display(),
            dest.display()
        );
    }
    std::fs::rename(abs, &dest).with_context(|| {
        format!(
            "rename {} -> {}",
            abs.display(),
            dest.display()
        )
    })?;
    Ok(dest)
}

fn tar_entry_path_safe(path: &Path) -> bool {
    for c in path.components() {
        match c {
            Component::ParentDir => return false,
            Component::Prefix(_) | Component::RootDir => return false,
            Component::Normal(_) | Component::CurDir => {}
        }
    }
    true
}

fn tar_path_matches_inner(
    entry_path: &Path,
    inner_path: &str,
    preferred_extension: Option<&str>,
) -> bool {
    let inner_norm = inner_path.replace('\\', "/").trim_start_matches('/').to_string();
    let inner_lc = inner_norm.to_lowercase();
    let entry_s = entry_path.to_string_lossy();
    let entry_norm = entry_s.replace('\\', "/").trim_start_matches('/').to_string();
    let entry_lc = entry_norm.to_lowercase();

    if entry_lc == inner_lc || entry_lc.ends_with(&format!("/{inner_lc}")) {
        return true;
    }

    let inner_file = Path::new(&inner_norm)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&inner_norm);
    let inner_base_lc = inner_file.to_lowercase();

    let entry_file = entry_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&entry_s);
    if entry_file.to_lowercase() == inner_base_lc {
        return true;
    }

    if let Some(pe) = preferred_extension.map(|e| e.trim().trim_start_matches('.').to_lowercase()) {
        if !pe.is_empty() {
            let with_ext = format!("{inner_file}.{pe}").to_lowercase();
            if entry_file.to_lowercase() == with_ext {
                return true;
            }
        }
    }

    false
}

/// Extract the first tar member matching `inner_path` into `dest_dir` (flat: uses member basename).
/// Returns path to the written file.
pub fn extract_matching_tar_member(
    archive_path: &Path,
    inner_path: &str,
    preferred_extension: Option<&str>,
    dest_dir: &Path,
) -> anyhow::Result<PathBuf> {
    let lower = archive_path.to_string_lossy().to_lowercase();
    let is_gz = lower.ends_with(".gz") || lower.ends_with(".tgz");

    let file = std::fs::File::open(archive_path)
        .with_context(|| format!("open archive {}", archive_path.display()))?;

    let tar_reader: Box<dyn Read> = if is_gz {
        Box::new(GzDecoder::new(file))
    } else {
        Box::new(file)
    };

    let mut archive = Archive::new(tar_reader);
    let mut matched: Option<PathBuf> = None;

    for entry in archive.entries().context("read tar entries")? {
        let mut entry = entry.context("tar entry")?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry.path().context("tar entry path")?;
        if !tar_entry_path_safe(&path) {
            continue;
        }
        if !tar_path_matches_inner(&path, inner_path, preferred_extension) {
            continue;
        }

        let name = path
            .file_name()
            .map(PathBuf::from)
            .filter(|n| !n.as_os_str().is_empty())
            .with_context(|| format!("tar member has no file name: {path:?}"))?;
        let out = dest_dir.join(&name);
        if out.exists() {
            anyhow::bail!("refusing to overwrite {}", out.display());
        }
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let mut outf = std::fs::File::create(&out)
            .with_context(|| format!("create {}", out.display()))?;
        std::io::copy(&mut entry, &mut outf)
            .with_context(|| format!("extract to {}", out.display()))?;

        if matched.is_some() {
            anyhow::bail!("multiple tar members matched {inner_path:?}; refusing to guess");
        }
        matched = Some(out);
    }

    matched.with_context(|| {
        format!(
            "no tar member matched inner path {inner_path:?} in {}",
            archive_path.display()
        )
    })
}

/// `preferred_name_record_id`: when set, the downloaded file is renamed to
/// `{normalize_record_id}[.ext]` (preferred extension if set, otherwise the on-disk extension).
pub async fn execute_download_plan(
    meta: TorrentDownloadMeta,
    output_dir: &Path,
    plan: DownloadPlan,
    preferred_extension: Option<&str>,
    complete_timeout: Duration,
    archive_download_dir: Option<&Path>,
    preferred_name_record_id: Option<&str>,
) -> anyhow::Result<Vec<PathBuf>> {
    std::fs::create_dir_all(output_dir).context("create output directory")?;
    let output_dir = output_dir.canonicalize().context("canonicalize output_dir")?;

    match plan {
        DownloadPlan::Direct { indices } => {
            let tmp = tempfile::Builder::new()
                .prefix("alexandria-dl-")
                .tempdir_in(&output_dir)
                .context("create alexandria-dl-* scratch under output")?;
            let scratch = tmp
                .path()
                .canonicalize()
                .context("canonicalize scratch")?;

            let session = Session::new(scratch.clone())
                .await
                .context("create librqbit session")?;

            let work = async {
                let opts = AddTorrentOptions {
                    overwrite: true,
                    list_only: false,
                    output_folder: Some(scratch.as_os_str().to_string_lossy().into_owned()),
                    only_files: Some(indices.clone()),
                    ..Default::default()
                };

                let resp = session
                    .add_torrent(AddTorrent::from_bytes(meta.torrent_bytes), Some(opts))
                    .await
                    .context("add torrent for download")?;

                let handle = match resp {
                    AddTorrentResponse::Added(_, h) | AddTorrentResponse::AlreadyManaged(_, h) => h,
                    AddTorrentResponse::ListOnly(_) => {
                        anyhow::bail!("unexpected list_only on add")
                    }
                };

                handle
                    .wait_until_initialized()
                    .await
                    .context("wait for torrent metadata")?;

                tokio::time::timeout(complete_timeout, handle.wait_until_completed())
                    .await
                    .map_err(|_| anyhow::anyhow!("download timed out after {:?}", complete_timeout))?
                    .context("torrent finished with error")?;

                move_selected_to_final(&scratch, &output_dir, &meta.info, &indices)?;
                let paths = apply_preferred_extensions(
                    &output_dir,
                    &meta.info,
                    &indices,
                    preferred_extension,
                    preferred_name_record_id,
                )?;
                Ok(paths)
            }
            .await;

            session.stop().await;
            work
        }
        DownloadPlan::NestedArchive {
            container_index,
            inner_path,
            ..
        } => {
            let arch_root = archive_download_dir.with_context(|| {
                "nested archive download requires an archive download directory (--archive-dir)"
            })?;
            std::fs::create_dir_all(arch_root).context("create archive download directory")?;
            let arch_root = arch_root
                .canonicalize()
                .context("canonicalize archive download directory")?;

            let tmp = tempfile::Builder::new()
                .prefix("alexandria-arch-")
                .tempdir_in(&arch_root)
                .context("create scratch under archive directory")?;
            let scratch = tmp.path().canonicalize().context("canonicalize arch scratch")?;

            let session = Session::new(scratch.clone())
                .await
                .context("create librqbit session (nested)")?;

            let work = async {
                let opts = AddTorrentOptions {
                    overwrite: true,
                    list_only: false,
                    output_folder: Some(scratch.as_os_str().to_string_lossy().into_owned()),
                    only_files: Some(vec![container_index]),
                    ..Default::default()
                };

                let resp = session
                    .add_torrent(AddTorrent::from_bytes(meta.torrent_bytes), Some(opts))
                    .await
                    .context("add torrent for nested archive download")?;

                let handle = match resp {
                    AddTorrentResponse::Added(_, h) | AddTorrentResponse::AlreadyManaged(_, h) => h,
                    AddTorrentResponse::ListOnly(_) => {
                        anyhow::bail!("unexpected list_only on nested add")
                    }
                };

                handle
                    .wait_until_initialized()
                    .await
                    .context("wait for torrent (nested)")?;

                tokio::time::timeout(complete_timeout, handle.wait_until_completed())
                    .await
                    .map_err(|_| anyhow::anyhow!("download timed out after {:?}", complete_timeout))?
                    .context("torrent finished with error (nested)")?;

                let rel = meta
                    .info
                    .iter_file_details()
                    .enumerate()
                    .find(|(i, _)| *i == container_index)
                    .map(|(_, fd)| fd.filename.to_pathbuf())
                    .context("container file index missing from info")?;
                let tar_path = scratch.join(&rel);
                if !tar_path.is_file() {
                    anyhow::bail!(
                        "expected archive file at {} after download",
                        tar_path.display()
                    );
                }

                let extracted = extract_matching_tar_member(
                    &tar_path,
                    inner_path.trim(),
                    preferred_extension,
                    &output_dir,
                )
                .context("extract inner file from archive")?;

                let final_path = finalize_download_file_name(
                    &extracted,
                    preferred_extension,
                    preferred_name_record_id,
                )?;

                std::fs::remove_file(&tar_path)
                    .with_context(|| format!("remove archive {}", tar_path.display()))?;

                Ok(vec![final_path])
            }
            .await;

            session.stop().await;
            work
        }
    }
}

/// Full download: fetch metadata, plan selection, run BitTorrent + optional tar extract.
///
/// For [`DownloadPlan::NestedArchive`], `archive_download_dir` must be `Some` or this returns an error.
pub async fn download_to_dir(
    output_dir: &Path,
    magnet: &str,
    path_in_torrent: Option<&str>,
    record_id: Option<&str>,
    preferred_extension: Option<&str>,
    complete_timeout: Duration,
    archive_download_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let path_trimmed = path_in_torrent.map(str::trim).filter(|s| !s.is_empty());
    let Some(p) = path_trimmed else {
        anyhow::bail!("refusing full-torrent download: path_in_torrent is missing or empty");
    };

    let meta = fetch_torrent_download_meta(magnet).await?;
    let plan = plan_file_selection(&meta.info, p, record_id)?;

    if matches!(plan, DownloadPlan::NestedArchive { .. }) && archive_download_dir.is_none() {
        anyhow::bail!(
            "path {:?} is not a top-level torrent file; it may be inside a .tar in the torrent. \
             Re-run with --archive-dir DIR to download the archive, or use the library API \
             fetch_torrent_download_meta + plan_file_selection + execute_download_plan.",
            p
        );
    }

    let paths = execute_download_plan(
        meta,
        output_dir,
        plan,
        preferred_extension,
        complete_timeout,
        archive_download_dir,
        record_id,
    )
    .await?;
    verify_download_against_record_md5(&paths, record_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tar_member_match_and_traversal() {
        let tmp = tempfile::tempdir().unwrap();
        let tar_path = tmp.path().join("test.tar");
        {
            let file = std::fs::File::create(&tar_path).unwrap();
            let mut builder = tar::Builder::new(file);
            let mut h = tar::Header::new_gnu();
            h.set_path("fixture_inner_member").unwrap();
            h.set_size(5);
            h.set_cksum();
            builder.append(&h, &b"hello"[..]).unwrap();
            let mut h2 = tar::Header::new_gnu();
            h2.set_path("other.bin").unwrap();
            h2.set_size(3);
            h2.set_cksum();
            builder.append(&h2, &b"bye"[..]).unwrap();
            builder.finish().unwrap();
        }

        let out_dir = tmp.path().join("out");
        std::fs::create_dir_all(&out_dir).unwrap();
        let got =
            extract_matching_tar_member(&tar_path, "fixture_inner_member", Some("epub"), &out_dir)
                .unwrap();
        assert_eq!(got.file_name().unwrap(), "fixture_inner_member");
        assert_eq!(std::fs::read_to_string(&got).unwrap(), "hello");

        assert!(extract_matching_tar_member(&tar_path, "nope", None, &out_dir).is_err());
    }

    #[test]
    fn tar_prefers_extension_member_name() {
        let tmp = tempfile::tempdir().unwrap();
        let tar_path = tmp.path().join("t.tar");
        {
            let file = std::fs::File::create(&tar_path).unwrap();
            let mut builder = tar::Builder::new(file);
            let mut h = tar::Header::new_gnu();
            h.set_path("fixture_inner_member.epub").unwrap();
            h.set_size(4);
            h.set_cksum();
            builder.append(&h, &b"data"[..]).unwrap();
            builder.finish().unwrap();
        }
        let out_dir = tmp.path().join("out2");
        std::fs::create_dir_all(&out_dir).unwrap();
        let got =
            extract_matching_tar_member(&tar_path, "fixture_inner_member", Some("epub"), &out_dir)
                .unwrap();
        assert_eq!(got.file_name().unwrap(), "fixture_inner_member.epub");
    }

    #[test]
    fn tar_path_safe_rejects_parent_components() {
        assert!(!super::tar_entry_path_safe(Path::new("../evil")));
        assert!(!super::tar_entry_path_safe(Path::new("a/../b")));
        assert!(super::tar_entry_path_safe(Path::new("fixture_inner_member")));
        assert!(super::tar_entry_path_safe(Path::new("dir/file")));
    }

    #[test]
    fn file_md5_hex_matches_known_vector() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("t");
        std::fs::write(&p, b"hello").unwrap();
        assert_eq!(
            file_md5_hex(&p).unwrap(),
            "5d41402abc4b2a76b9719d911017c592"
        );
    }

    #[test]
    fn verify_download_skips_non_md5_record_id() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("f");
        std::fs::write(&p, b"x").unwrap();
        verify_download_against_record_md5(&[p], Some("not-a-hex-id")).unwrap();
    }

    #[test]
    fn verify_download_matches_md5_record_id() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("f");
        std::fs::write(&p, b"hello").unwrap();
        verify_download_against_record_md5(
            &[p],
            Some("md5:5d41402abc4b2a76b9719d911017c592"),
        )
        .unwrap();
    }

    #[test]
    fn verify_download_rejects_wrong_md5() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("f");
        std::fs::write(&p, b"hello").unwrap();
        let wrong = format!("md5:{}", "0".repeat(32));
        assert!(verify_download_against_record_md5(&[p], Some(wrong.as_str())).is_err());
    }
}
