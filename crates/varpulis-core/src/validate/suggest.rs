//! "Did you mean?" suggestions using Levenshtein distance.

/// Compute the Levenshtein edit distance between two strings.
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    let mut prev: Vec<usize> = (0..=b_len).collect();
    let mut curr = vec![0usize; b_len + 1];

    for (i, a_ch) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, b_ch) in b.chars().enumerate() {
            let cost = if a_ch == b_ch { 0 } else { 1 };
            curr[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(curr[j] + 1);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[b_len]
}

/// Maximum edit distance threshold for a suggestion to be considered relevant.
fn max_distance(name_len: usize) -> usize {
    (name_len / 2).clamp(1, 3)
}

/// Find the closest match in `candidates` for `name`.
/// Returns `None` if no candidate is close enough.
pub fn suggest(name: &str, candidates: &[&str]) -> Option<String> {
    let threshold = max_distance(name.len());
    let lower = name.to_lowercase();

    let mut best: Option<(&str, usize)> = None;
    for &candidate in candidates {
        let dist = levenshtein(&lower, &candidate.to_lowercase());
        if dist <= threshold && (best.is_none() || dist < best.unwrap().1) {
            best = Some((candidate, dist));
        }
    }

    best.map(|(s, _)| s.to_string())
}

/// Build a "did you mean 'X'?" suffix string, or empty if no suggestion.
pub fn did_you_mean(name: &str, candidates: &[&str]) -> String {
    match suggest(name, candidates) {
        Some(s) => format!("; did you mean '{}'?", s),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein_identical() {
        assert_eq!(levenshtein("hello", "hello"), 0);
    }

    #[test]
    fn test_levenshtein_one_off() {
        assert_eq!(levenshtein("level", "lvl"), 2);
    }

    #[test]
    fn test_suggest_finds_close_match() {
        let candidates = &["level", "message", "data"];
        assert_eq!(suggest("lvl", candidates), None); // distance 2 but len/2 = 1
        assert_eq!(suggest("leve", candidates), Some("level".to_string()));
        assert_eq!(suggest("mesage", candidates), Some("message".to_string()));
    }

    #[test]
    fn test_suggest_no_match() {
        let candidates = &["level", "message", "data"];
        assert_eq!(suggest("zzzzz", candidates), None);
    }

    #[test]
    fn test_did_you_mean_formatting() {
        let candidates = &["count", "sum", "avg"];
        let s = did_you_mean("cont", candidates);
        assert!(s.contains("count"));
    }
}
