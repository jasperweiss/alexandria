//! OCLC control numbers (WorldCat) from `identifiers_unified.oclc`.

use serde_json::Value;
use std::collections::BTreeSet;

/// Keep only ASCII digits; returns None if empty or length outside a plausible OCLC range.
pub fn normalize_oclc_token(s: &str) -> Option<String> {
    let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.len() < 4 || digits.len() > 15 {
        return None;
    }
    Some(digits)
}

/// When the trimmed query is one OCLC-shaped token (optional spaces/hyphens), return digits.
pub fn try_user_query_as_oclc(query: &str) -> Option<String> {
    let t = query.trim();
    if t.is_empty() {
        return None;
    }
    normalize_oclc_token(t)
}

pub fn record_oclc_set_from_identifiers_unified(identifiers: Option<&Value>) -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    let Some(obj) = identifiers.and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    if let Some(arr) = obj.get("oclc").and_then(|v| v.as_array()) {
        for v in arr {
            if let Some(s) = v.as_str() {
                if let Some(n) = normalize_oclc_token(s) {
                    set.insert(n);
                }
            }
        }
    }
    set.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_strips_nondigits() {
        assert_eq!(
            normalize_oclc_token("999-888-777").as_deref(),
            Some("999888777")
        );
    }

    #[test]
    fn query_trimmed() {
        assert_eq!(
            try_user_query_as_oclc("  999888777  ").as_deref(),
            Some("999888777")
        );
    }

    #[test]
    fn too_short_rejected() {
        assert!(normalize_oclc_token("123").is_none());
    }
}
