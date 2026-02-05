/// Compile-time expansion of top-level for loops.
///
/// Expands `for VAR in START..END:` at indent level 0 into repeated copies
/// of the body with `{VAR}` replaced by each integer value.
///
/// Runs iteratively until no top-level for loops remain (handles nesting).
pub fn expand_declaration_loops(source: &str) -> String {
    let mut result = source.to_string();
    loop {
        let expanded = expand_one_pass(&result);
        if expanded == result {
            break;
        }
        result = expanded;
    }
    result
}

fn expand_one_pass(source: &str) -> String {
    let lines: Vec<&str> = source.lines().collect();
    let mut result = String::new();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        let indent = line.len() - line.trim_start().len();
        let trimmed = line.trim();

        if indent == 0 && is_declaration_for(trimmed) {
            if let Some((var, start, end)) = parse_for_range(trimmed) {
                // Collect body: lines with indent > 0 until we hit indent 0
                let body_start = i + 1;
                let mut body_end = body_start;
                let mut body_indent: Option<usize> = None;

                while body_end < lines.len() {
                    let bl = lines[body_end];
                    if bl.trim().is_empty() {
                        body_end += 1;
                        continue;
                    }
                    let bi = bl.len() - bl.trim_start().len();
                    if bi == 0 {
                        break;
                    }
                    if body_indent.is_none() {
                        body_indent = Some(bi);
                    }
                    body_end += 1;
                }

                let strip = body_indent.unwrap_or(4);
                let pattern = format!("{{{}}}", var);

                for val in start..end {
                    for bl in &lines[body_start..body_end] {
                        if bl.trim().is_empty() {
                            result.push('\n');
                            continue;
                        }
                        let stripped = if bl.len() >= strip {
                            &bl[strip..]
                        } else {
                            bl.trim_start()
                        };
                        result.push_str(&stripped.replace(&pattern, &val.to_string()));
                        result.push('\n');
                    }
                }

                i = body_end;
                continue;
            }
        }

        result.push_str(line);
        result.push('\n');
        i += 1;
    }
    result
}

/// Check if a trimmed line looks like a compile-time for loop.
/// Must start with `for `, contain `..` (range), and end with `:`.
fn is_declaration_for(trimmed: &str) -> bool {
    trimmed.starts_with("for ") && trimmed.ends_with(':') && trimmed.contains("..")
}

/// Parse `for VAR in START..END:` or `for VAR in START..=END:`
fn parse_for_range(trimmed: &str) -> Option<(&str, i64, i64)> {
    let rest = trimmed.strip_prefix("for ")?.strip_suffix(':')?;
    let (var, range) = rest.split_once(" in ")?;
    let var = var.trim();
    let range = range.trim();

    if let Some((s, e)) = range.split_once("..=") {
        let start: i64 = s.trim().parse().ok()?;
        let end: i64 = e.trim().parse().ok()?;
        Some((var, start, end + 1))
    } else if let Some((s, e)) = range.split_once("..") {
        let start: i64 = s.trim().parse().ok()?;
        let end: i64 = e.trim().parse().ok()?;
        Some((var, start, end))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_expansion() {
        let input = "for i in 0..3:\n    context c{i}\n";
        let output = expand_declaration_loops(input);
        assert_eq!(output.trim(), "context c0\ncontext c1\ncontext c2");
    }

    #[test]
    fn test_nested_expansion() {
        let input = "for r in 0..2:\n    for c in 0..2:\n        context t{r}{c}\n";
        let output = expand_declaration_loops(input);
        assert!(output.contains("context t00"));
        assert!(output.contains("context t01"));
        assert!(output.contains("context t10"));
        assert!(output.contains("context t11"));
        assert_eq!(output.matches("context").count(), 4);
    }

    #[test]
    fn test_inclusive_range() {
        let input = "for i in 0..=2:\n    context c{i}\n";
        let output = expand_declaration_loops(input);
        assert_eq!(output.matches("context").count(), 3);
    }

    #[test]
    fn test_no_expansion_inside_function() {
        let input = "fn foo():\n    for i in 0..3:\n        let x = {i}\n";
        let output = expand_declaration_loops(input);
        // Should be unchanged — for loop is inside a function (indent > 0)
        assert!(output.contains("for i in 0..3:"));
    }

    #[test]
    fn test_expression_substitution() {
        let input = "for i in 0..2:\n    stream S{i} = E{i}\n        .process(f({i} * 10))\n";
        let output = expand_declaration_loops(input);
        assert!(output.contains(".process(f(0 * 10))"));
        assert!(output.contains(".process(f(1 * 10))"));
    }

    #[test]
    fn test_empty_range() {
        let input = "for i in 0..0:\n    context c{i}\n";
        let output = expand_declaration_loops(input);
        assert!(!output.contains("context"));
    }

    #[test]
    fn test_preserves_non_loop_lines() {
        let input = "connector X = mqtt (host: \"localhost\")\n\nfor i in 0..2:\n    context c{i}\n\nfn foo():\n    return 1\n";
        let output = expand_declaration_loops(input);
        assert!(output.contains("connector X"));
        assert!(output.contains("fn foo():"));
        assert!(output.contains("context c0"));
        assert!(output.contains("context c1"));
    }

    #[test]
    fn test_comments_in_loop_body() {
        let input = "for i in 0..2:\n    # tile {i}\n    context c{i}\n";
        let output = expand_declaration_loops(input);
        assert!(output.contains("# tile 0"));
        assert!(output.contains("# tile 1"));
        assert!(output.contains("context c0"));
        assert!(output.contains("context c1"));
    }

    #[test]
    fn test_multiple_sequential_loops() {
        let input = "for i in 0..2:\n    context c{i}\n\nfor j in 0..3:\n    stream S{j} = E{j}\n";
        let output = expand_declaration_loops(input);
        assert_eq!(output.matches("context").count(), 2);
        assert_eq!(output.matches("stream").count(), 3);
        assert!(output.contains("context c0"));
        assert!(output.contains("context c1"));
        assert!(output.contains("stream S0"));
        assert!(output.contains("stream S2"));
    }

    #[test]
    fn test_negative_range() {
        let input = "for i in -1..2:\n    context c{i}\n";
        let output = expand_declaration_loops(input);
        assert!(output.contains("context c-1"));
        assert!(output.contains("context c0"));
        assert!(output.contains("context c1"));
        assert_eq!(output.matches("context").count(), 3);
    }

    #[test]
    fn test_single_iteration() {
        let input = "for i in 5..6:\n    context only{i}\n";
        let output = expand_declaration_loops(input);
        assert_eq!(output.trim(), "context only5");
    }

    #[test]
    fn test_blank_lines_in_body() {
        let input = "for i in 0..2:\n    context c{i}\n\n    stream S{i} = E{i}\n";
        let output = expand_declaration_loops(input);
        assert!(output.contains("context c0"));
        assert!(output.contains("stream S0"));
        assert!(output.contains("context c1"));
        assert!(output.contains("stream S1"));
    }

    #[test]
    fn test_multi_line_stream_body() {
        let input = "for i in 0..2:\n    stream S{i} = E{i}\n        .context(c{i})\n        .where(x > {i})\n        .emit(val: x)\n";
        let output = expand_declaration_loops(input);
        assert!(output.contains("stream S0 = E0"));
        assert!(output.contains("    .context(c0)"));
        assert!(output.contains("    .where(x > 0)"));
        assert!(output.contains("stream S1 = E1"));
        assert!(output.contains("    .context(c1)"));
        assert!(output.contains("    .where(x > 1)"));
    }

    #[test]
    fn test_triple_nested_expansion() {
        let input = "for a in 0..2:\n    for b in 0..2:\n        for c in 0..2:\n            context t{a}{b}{c}\n";
        let output = expand_declaration_loops(input);
        assert_eq!(output.matches("context").count(), 8);
        assert!(output.contains("context t000"));
        assert!(output.contains("context t111"));
        assert!(output.contains("context t010"));
        assert!(output.contains("context t101"));
    }

    #[test]
    fn test_no_substitution_when_no_braces() {
        let input = "for i in 0..2:\n    context plain\n";
        let output = expand_declaration_loops(input);
        // Body has no {i} — should repeat body twice unchanged
        assert_eq!(output.matches("context plain").count(), 2);
    }

    #[test]
    fn test_non_range_for_not_expanded() {
        // A for loop without literal range (uses variable) should not be expanded
        let input = "for i in items:\n    process(i)\n";
        let output = expand_declaration_loops(input);
        assert!(output.contains("for i in items:"));
    }

    #[test]
    fn test_inclusive_range_boundary() {
        let input = "for i in 0..=0:\n    context c{i}\n";
        let output = expand_declaration_loops(input);
        assert_eq!(output.trim(), "context c0");
    }

    #[test]
    fn test_parse_for_range_exclusive() {
        assert_eq!(parse_for_range("for x in 0..5:"), Some(("x", 0, 5)));
    }

    #[test]
    fn test_parse_for_range_inclusive() {
        assert_eq!(parse_for_range("for x in 0..=5:"), Some(("x", 0, 6)));
    }

    #[test]
    fn test_parse_for_range_negative_start() {
        assert_eq!(parse_for_range("for x in -3..3:"), Some(("x", -3, 3)));
    }

    #[test]
    fn test_parse_for_range_invalid_no_in() {
        assert_eq!(parse_for_range("for x 0..5:"), None);
    }

    #[test]
    fn test_parse_for_range_invalid_non_integer() {
        assert_eq!(parse_for_range("for x in a..b:"), None);
    }

    #[test]
    fn test_is_declaration_for_positive() {
        assert!(is_declaration_for("for i in 0..3:"));
        assert!(is_declaration_for("for row in 0..=4:"));
    }

    #[test]
    fn test_is_declaration_for_negative() {
        assert!(!is_declaration_for("for i in items:"));
        assert!(!is_declaration_for("forward(x)"));
        assert!(!is_declaration_for("# for i in 0..3:"));
    }
}
