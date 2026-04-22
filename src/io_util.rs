use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor, Read};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

struct CountingReader {
    inner: Box<dyn Read + Send>,
    counter: Arc<AtomicU64>,
}

impl Read for CountingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.counter.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

/// Open a buffered reader, decompressing with gzip when the path ends with `.gz`
/// or the file starts with gzip magic bytes (after probing the first two bytes).
pub fn open_decompressed_reader(path: &Path) -> io::Result<Box<dyn BufRead + Send>> {
    open_decompressed_reader_inner(path, None)
}

/// Like [`open_decompressed_reader`], but increments `counter` by every byte read from the
/// underlying file (compressed bytes for `.gz`, i.e. progress through the shard on disk).
pub fn open_decompressed_reader_with_byte_counter(
    path: &Path,
    counter: Arc<AtomicU64>,
) -> io::Result<Box<dyn BufRead + Send>> {
    open_decompressed_reader_inner(path, Some(counter))
}

fn open_decompressed_reader_inner(
    path: &Path,
    byte_counter: Option<Arc<AtomicU64>>,
) -> io::Result<Box<dyn BufRead + Send>> {
    let mut file = File::open(path)?;
    let mut header = [0u8; 2];
    let n = file.read(&mut header)?;
    let gzip_by_ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);
    let gzip_by_magic = n == 2 && header == GZIP_MAGIC;
    let gzip = gzip_by_ext || gzip_by_magic;

    // Count bytes read from the file on disk. The gzip decoder may read the same logical prefix
    // again via `chain(Cursor(peek), file)`; only `file` is wrapped so we do not double-count.
    let file: Box<dyn Read + Send> = match byte_counter {
        Some(c) => {
            if n > 0 {
                c.fetch_add(n as u64, Ordering::Relaxed);
            }
            Box::new(CountingReader {
                inner: Box::new(file),
                counter: c,
            })
        }
        None => Box::new(file),
    };

    let rest: Box<dyn Read + Send> = match n {
        0 => file,
        1 => Box::new(Cursor::new(vec![header[0]]).chain(file)),
        2 => Box::new(Cursor::new(header.to_vec()).chain(file)),
        _ => unreachable!(),
    };

    if gzip {
        Ok(Box::new(BufReader::new(GzDecoder::new(rest))))
    } else {
        Ok(Box::new(BufReader::new(rest)))
    }
}
