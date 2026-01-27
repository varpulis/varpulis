//! Helper functions for parsing literals in LALRPOP grammar

/// Parse a duration string like "5s", "100ms", "1h" into nanoseconds
pub fn parse_duration(s: &str) -> u64 {
    let len = s.len();
    let (num_str, unit) = if s.ends_with("ns") {
        (&s[..len - 2], "ns")
    } else if s.ends_with("us") {
        (&s[..len - 2], "us")
    } else if s.ends_with("ms") {
        (&s[..len - 2], "ms")
    } else if s.ends_with('s') && !s.ends_with("ns") && !s.ends_with("us") && !s.ends_with("ms") {
        (&s[..len - 1], "s")
    } else if s.ends_with('m') {
        (&s[..len - 1], "m")
    } else if s.ends_with('h') {
        (&s[..len - 1], "h")
    } else if s.ends_with('d') {
        (&s[..len - 1], "d")
    } else {
        return 0;
    };

    let num: u64 = num_str.parse().unwrap_or(0);
    match unit {
        "ns" => num,
        "us" => num * 1_000,
        "ms" => num * 1_000_000,
        "s" => num * 1_000_000_000,
        "m" => num * 60 * 1_000_000_000,
        "h" => num * 3600 * 1_000_000_000,
        "d" => num * 86400 * 1_000_000_000,
        _ => 0,
    }
}

/// Parse an ISO8601 timestamp like "@2024-01-15" or "@2024-01-15T10:30:00Z" into nanoseconds since epoch
pub fn parse_timestamp(s: &str) -> i64 {
    // Remove @ prefix
    let s = s.strip_prefix('@').unwrap_or(s);

    // Parse ISO8601: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS(Z|+HH:MM|-HH:MM)
    let (date_part, time_part) = if let Some(idx) = s.find('T') {
        (&s[..idx], Some(&s[idx + 1..]))
    } else {
        (s, None)
    };

    // Parse date: YYYY-MM-DD
    let parts: Vec<&str> = date_part.split('-').collect();
    if parts.len() != 3 {
        return 0;
    }

    let year: i32 = parts[0].parse().unwrap_or(1970);
    let month: u32 = parts[1].parse().unwrap_or(1);
    let day: u32 = parts[2].parse().unwrap_or(1);

    // Calculate days since epoch (1970-01-01)
    let mut days: i64 = 0;

    // Years
    for y in 1970..year {
        days += if is_leap_year(y) { 366 } else { 365 };
    }
    for y in (year..1970).rev() {
        days -= if is_leap_year(y) { 366 } else { 365 };
    }

    // Months
    let days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..month {
        days += days_in_month[(m - 1) as usize] as i64;
        if m == 2 && is_leap_year(year) {
            days += 1;
        }
    }

    // Days
    days += (day - 1) as i64;

    let mut seconds = days * 86400;

    // Parse time if present
    if let Some(time_str) = time_part {
        // Strip timezone suffix and parse HH:MM:SS
        let time_only = time_str
            .trim_end_matches('Z')
            .split('+')
            .next()
            .unwrap_or(time_str)
            .split('-')
            .next()
            .unwrap_or(time_str);

        let time_parts: Vec<&str> = time_only.split(':').collect();
        if time_parts.len() >= 2 {
            let hours: i64 = time_parts[0].parse().unwrap_or(0);
            let minutes: i64 = time_parts[1].parse().unwrap_or(0);
            let secs: i64 = if time_parts.len() >= 3 {
                time_parts[2].parse().unwrap_or(0)
            } else {
                0
            };
            seconds += hours * 3600 + minutes * 60 + secs;
        }

        // Handle timezone offset (simplified - just adjust hours)
        if let Some(tz_idx) = time_str.find('+') {
            let tz = &time_str[tz_idx + 1..];
            if let Some(colon_idx) = tz.find(':') {
                let tz_hours: i64 = tz[..colon_idx].parse().unwrap_or(0);
                seconds -= tz_hours * 3600;
            }
        } else if let Some(tz_idx) = time_str[1..].find('-') {
            let tz = &time_str[tz_idx + 2..];
            if let Some(colon_idx) = tz.find(':') {
                let tz_hours: i64 = tz[..colon_idx].parse().unwrap_or(0);
                seconds += tz_hours * 3600;
            }
        }
    }

    // Convert to nanoseconds
    seconds * 1_000_000_000
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration("5s"), 5_000_000_000);
        assert_eq!(parse_duration("1s"), 1_000_000_000);
    }

    #[test]
    fn test_parse_duration_milliseconds() {
        assert_eq!(parse_duration("100ms"), 100_000_000);
        assert_eq!(parse_duration("1ms"), 1_000_000);
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration("1m"), 60_000_000_000);
        assert_eq!(parse_duration("5m"), 300_000_000_000);
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration("1h"), 3_600_000_000_000);
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration("1d"), 86_400_000_000_000);
    }

    #[test]
    fn test_parse_timestamp_epoch() {
        assert_eq!(parse_timestamp("@1970-01-01"), 0);
    }

    #[test]
    fn test_parse_timestamp_with_time() {
        let ns = parse_timestamp("@1970-01-01T01:00:00Z");
        assert_eq!(ns, 3_600_000_000_000); // 1 hour in nanoseconds
    }

    #[test]
    fn test_is_leap_year() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2023));
    }
}
