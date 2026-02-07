//! Indentation preprocessor for VPL
//!
//! Converts Python-style indentation to explicit INDENT/DEDENT tokens
//! so that PEG parsers can handle block structures.
//!
//! Only adds markers for block-introducing keywords: fn, if, elif, else, for, while, config

/// Keywords that introduce indented blocks
const BLOCK_KEYWORDS: &[&str] = &[
    "fn ", "if ", "elif ", "else:", "for ", "while ", "config:", "event ",
];

/// Check if a line ends with a block-introducing pattern
fn is_block_start(line: &str) -> bool {
    let trimmed = line.trim();
    // Must end with colon
    if !trimmed.ends_with(':') {
        return false;
    }
    // Must start with a block keyword
    BLOCK_KEYWORDS.iter().any(|kw| trimmed.starts_with(kw))
}

/// Preprocess source code to add explicit INDENT/DEDENT markers
pub fn preprocess_indentation(source: &str) -> String {
    let mut result = String::new();
    let mut indent_stack: Vec<usize> = vec![0];
    let mut expecting_block = false;
    let mut in_block_comment = false;

    for line in source.lines() {
        // Handle block comments
        if in_block_comment {
            result.push_str(line);
            result.push('\n');
            if line.contains("*/") {
                in_block_comment = false;
            }
            continue;
        }

        if line.trim_start().starts_with("/*") {
            in_block_comment = true;
            result.push_str(line);
            result.push('\n');
            if line.contains("*/") {
                in_block_comment = false;
            }
            continue;
        }

        // Skip empty lines and comments
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            result.push_str(line);
            result.push('\n');
            continue;
        }

        // Calculate indentation level
        let indent = line.len() - line.trim_start().len();
        let current_indent = *indent_stack.last().unwrap();

        if expecting_block && indent > current_indent {
            // New block after block keyword - add INDENT marker
            indent_stack.push(indent);
            result.push_str("«INDENT»");
            result.push_str(trimmed);
            result.push('\n');
            expecting_block = false;
        } else if indent < current_indent && indent_stack.len() > 1 {
            // End of block(s) - add DEDENT marker(s)
            while indent_stack.len() > 1 && *indent_stack.last().unwrap() > indent {
                indent_stack.pop();
                result.push_str("«DEDENT»");
            }
            result.push_str(trimmed);
            result.push('\n');
        } else {
            // Same level or continuation
            expecting_block = false;
            result.push_str(trimmed);
            result.push('\n');
        }

        // Check if this line starts a block
        if is_block_start(trimmed) {
            expecting_block = true;
        }
    }

    // Close any remaining blocks
    while indent_stack.len() > 1 {
        indent_stack.pop();
        result.push_str("«DEDENT»");
    }

    result
}

/// Remove INDENT/DEDENT markers (for clean output)
pub fn remove_indent_markers(source: &str) -> String {
    source.replace("«INDENT»", "").replace("«DEDENT»", "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_function() {
        let input = r#"fn add(a: int, b: int) -> int:
    return a + b
"#;
        let output = preprocess_indentation(input);
        assert!(output.contains("«INDENT»"));
        assert!(output.contains("«DEDENT»"));
    }

    #[test]
    fn test_stream_with_pattern() {
        let input = r#"stream HighVolumeAlert = Trade
    .window(1m)
    .pattern(high_activity: events => events.len() > 3)
    .emit(alert_type: "test")"#;
        let output = preprocess_indentation(input);
        // Should NOT add INDENT/DEDENT for continuation lines
        assert!(
            !output.contains("«INDENT»"),
            "Should not have INDENT: {}",
            output
        );
        // The output should be all on separate lines without indentation markers
        assert!(output.contains(".window(1m)"));
        assert!(output.contains(".pattern("));
    }

    #[test]
    fn test_nested_blocks() {
        let input = r#"fn test():
    if x > 0:
        return x
    else:
        return 0
"#;
        let output = preprocess_indentation(input);
        // Should have multiple INDENT/DEDENT pairs
        assert_eq!(output.matches("«INDENT»").count(), 3);
    }

    #[test]
    fn test_stream_no_indent() {
        let input = "stream A = B.where(x > 0)\n";
        let output = preprocess_indentation(input);
        assert!(!output.contains("«INDENT»"));
    }
}
