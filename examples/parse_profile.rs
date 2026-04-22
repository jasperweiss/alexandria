//! One-off: compare gzip read vs + serde_json parse for NDJSON (same path as ingest).
//! Run: `cargo run --release --example parse_profile -- /path/to/file.json.gz`

use alexandria::io_util::open_decompressed_reader;
use std::env;
use std::io::BufRead;
use std::path::Path;
use std::time::Instant;

fn main() -> anyhow::Result<()> {
    let path = env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: parse_profile <file.json.gz>"))?;
    let path = Path::new(&path);

    let t0 = Instant::now();
    let mut reader = open_decompressed_reader(path)?;
    let mut lines = 0u64;
    let mut buf = String::new();
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        lines += 1;
    }
    let read_only = t0.elapsed();

    let t1 = Instant::now();
    let mut reader = open_decompressed_reader(path)?;
    let mut parsed = 0u64;
    let mut buf = String::new();
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 {
            break;
        }
        let t = buf.trim();
        if t.is_empty() {
            continue;
        }
        let _: serde_json::Value = serde_json::from_str(t)?;
        parsed += 1;
    }
    let read_parse = t1.elapsed();

    println!("File: {}", path.display());
    println!("Lines read (no JSON):     {lines} in {:.3}s", read_only.as_secs_f64());
    println!("Lines parsed (serde_json): {parsed} in {:.3}s", read_parse.as_secs_f64());
    println!(
        "Parse overhead vs read-only: {:.1}%",
        (read_parse.as_secs_f64() / read_only.as_secs_f64() - 1.0) * 100.0
    );
    Ok(())
}
