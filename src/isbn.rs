//! ISBN-10 / ISBN-13 normalization and conversion (978 prefix migration for ISBN-10).

use serde_json::Value;
use std::collections::BTreeSet;

/// Strip spaces and hyphens, keep ASCII digits; for ISBN-10 parsing callers may uppercase and allow trailing `X`.
pub fn compact_isbn_chars(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .collect()
}

/// True if `s` is 10 chars: first nine digits, last digit or `X` (after compacting).
fn looks_like_isbn10_body(compact: &str) -> bool {
    let b: Vec<char> = compact.chars().collect();
    if b.len() != 10 {
        return false;
    }
    for c in &b[..9] {
        if !c.is_ascii_digit() {
            return false;
        }
    }
    let last = b[9].to_ascii_uppercase();
    last.is_ascii_digit() || last == 'X'
}

/// ISBN-10 weighted check: sum (10-i)*d_i for i in 0..9, plus 1*d_10 (X = 10); divisible by 11.
pub fn isbn10_check_valid(compact_upper: &str) -> bool {
    let b: Vec<char> = compact_upper.chars().collect();
    if b.len() != 10 {
        return false;
    }
    let mut sum = 0u32;
    for i in 0..9 {
        let d = match b[i].to_digit(10) {
            Some(d) => d,
            None => return false,
        };
        sum += (10 - i as u32) * d;
    }
    let d10 = match b[9].to_ascii_uppercase() {
        'X' => 10,
        c if c.is_ascii_digit() => c.to_digit(10).unwrap(),
        _ => return false,
    };
    sum += d10;
    sum % 11 == 0
}

/// ISBN-13 check digit on first 12 digits; returns expected 13th digit.
fn isbn13_check_digit(body12: &[u8; 12]) -> u8 {
    let mut sum = 0u32;
    for (i, &d) in body12.iter().enumerate() {
        let weight = if i % 2 == 0 { 1 } else { 3 };
        sum += weight * d as u32;
    }
    ((10 - (sum % 10)) % 10) as u8
}

/// ISBN-13 check over 13 decimal digits (ASCII).
pub fn isbn13_check_valid(digits13: &str) -> bool {
    let b = digits13.as_bytes();
    if b.len() != 13 || !b.iter().all(|x| x.is_ascii_digit()) {
        return false;
    }
    let mut body = [0u8; 12];
    for i in 0..12 {
        body[i] = b[i] - b'0';
    }
    let expected = isbn13_check_digit(&body);
    b[12] - b'0' == expected
}

/// Normalize to 13 ASCII digits; validates check digit. Strips spaces and hyphens from `s`.
pub fn normalize_isbn13_str(s: &str) -> Option<String> {
    let compact: String = s
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();
    if compact.len() != 13 {
        return None;
    }
    if !isbn13_check_valid(&compact) {
        return None;
    }
    Some(compact)
}

/// Convert validated ISBN-10 (compact, uppercase `X` allowed) to ISBN-13 with 978 prefix.
pub fn isbn10_to_isbn13(isbn10: &str) -> Option<String> {
    let compact = compact_isbn_chars(isbn10).to_ascii_uppercase();
    if !looks_like_isbn10_body(&compact) {
        return None;
    }
    if !isbn10_check_valid(&compact) {
        return None;
    }
    let nine: String = compact.chars().take(9).collect();
    let mut body12 = [0u8; 12];
    let prefix = b"978";
    body12[0] = prefix[0] - b'0';
    body12[1] = prefix[1] - b'0';
    body12[2] = prefix[2] - b'0';
    for (i, c) in nine.chars().enumerate() {
        body12[3 + i] = c.to_digit(10)? as u8;
    }
    let check = isbn13_check_digit(&body12);
    let mut out = String::with_capacity(13);
    for d in &body12 {
        out.push((b'0' + d) as char);
    }
    out.push((b'0' + check) as char);
    Some(out)
}

/// If the entire trimmed query is one ISBN-10 or ISBN-13 (hyphens/spaces allowed), return canonical 13-digit string.
pub fn try_user_query_as_isbn13_digits(query: &str) -> Option<String> {
    let t = query.trim();
    if t.is_empty() {
        return None;
    }
    let compact = compact_isbn_chars(t).to_ascii_uppercase();
    if compact.len() == 13 && compact.chars().all(|c| c.is_ascii_digit()) {
        return normalize_isbn13_str(&compact);
    }
    if looks_like_isbn10_body(&compact) {
        return isbn10_to_isbn13(&compact);
    }
    None
}

fn collect_isbns_from_json_array(arr: &[Value], set: &mut BTreeSet<String>) {
    for v in arr {
        if let Some(s) = v.as_str() {
            if let Some(n) = try_user_query_as_isbn13_digits(s) {
                set.insert(n);
            }
        }
    }
}

fn add_isbns_from_identifiers_unified(idents: &serde_json::Map<String, Value>, set: &mut BTreeSet<String>) {
    for key in ["isbn13", "isbn10"] {
        if let Some(arr) = idents.get(key).and_then(|v| v.as_array()) {
            collect_isbns_from_json_array(arr, set);
        }
    }
}

/// Normalized unique ISBN-13 strings from `identifiers_unified` only (`isbn13` + `isbn10` arrays).
/// Each entry is parsed as ISBN-13 or, when valid, converted from ISBN-10 (hyphens/spaces allowed).
pub fn record_isbn13_set_from_identifiers(identifiers: Option<&Value>) -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    if let Some(obj) = identifiers.and_then(|v| v.as_object()) {
        add_isbns_from_identifiers_unified(obj, &mut set);
    }
    set.into_iter().collect()
}

/// Like [`record_isbn13_set_from_identifiers`], but reads the full `_source.file_unified_data` object:
/// `identifiers_unified` (`isbn13`, `isbn10`) plus `search_only_fields.search_isbn13`.
pub fn record_isbn13_set_from_file_unified(file_unified: Option<&Value>) -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    let Some(fud) = file_unified.and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    if let Some(idents) = fud.get("identifiers_unified").and_then(|v| v.as_object()) {
        add_isbns_from_identifiers_unified(idents, &mut set);
    }
    if let Some(so) = fud.get("search_only_fields").and_then(|v| v.as_object()) {
        if let Some(arr) = so.get("search_isbn13").and_then(|v| v.as_array()) {
            collect_isbns_from_json_array(arr, &mut set);
        }
    }
    set.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isbn10_with_x_suffix() {
        assert_eq!(
            isbn10_to_isbn13("123456789X").as_deref(),
            Some("9781234567897")
        );
    }

    #[test]
    fn try_query_hyphenated_isbn13() {
        assert_eq!(
            try_user_query_as_isbn13_digits("978-1-23456789-7").as_deref(),
            Some("9781234567897")
        );
    }

    #[test]
    fn try_query_isbn10() {
        assert_eq!(
            try_user_query_as_isbn13_digits("123456789X").as_deref(),
            Some("9781234567897")
        );
    }

    #[test]
    fn partial_query_not_isbn() {
        assert!(try_user_query_as_isbn13_digits("foo 123456789X").is_none());
    }

    #[test]
    fn record_set_treats_isbn10_in_isbn13_array_as_convertible() {
        let v = serde_json::json!({
            "identifiers_unified": {
                "isbn13": ["123456789X"],
                "isbn10": []
            }
        });
        let got = record_isbn13_set_from_file_unified(Some(&v));
        assert_eq!(got, vec!["9781234567897".to_string()]);
    }

    #[test]
    fn record_set_includes_search_isbn13() {
        let v = serde_json::json!({
            "identifiers_unified": {},
            "search_only_fields": { "search_isbn13": ["123456789X"] }
        });
        let got = record_isbn13_set_from_file_unified(Some(&v));
        assert_eq!(got, vec!["9781234567897".to_string()]);
    }
}
