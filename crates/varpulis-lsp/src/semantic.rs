//! Semantic tokens and document symbols

use once_cell::sync::Lazy;
use tower_lsp::lsp_types::{
    Location, Position, Range, SemanticToken, SemanticTokenModifier, SemanticTokenType,
    SemanticTokensLegend, SymbolInformation, SymbolKind, Url,
};

/// Semantic token types used by the LSP
pub static SEMANTIC_TOKEN_TYPES: Lazy<Vec<SemanticTokenType>> = Lazy::new(|| {
    vec![
        SemanticTokenType::KEYWORD,
        SemanticTokenType::TYPE,
        SemanticTokenType::FUNCTION,
        SemanticTokenType::VARIABLE,
        SemanticTokenType::STRING,
        SemanticTokenType::NUMBER,
        SemanticTokenType::OPERATOR,
        SemanticTokenType::COMMENT,
        SemanticTokenType::CLASS,     // For event types
        SemanticTokenType::PARAMETER, // For function parameters
        SemanticTokenType::PROPERTY,  // For fields
        SemanticTokenType::NAMESPACE, // For stream names
    ]
});

/// Semantic token modifiers (none used currently)
pub static SEMANTIC_TOKEN_MODIFIERS: Lazy<Vec<SemanticTokenModifier>> = Lazy::new(Vec::new);

/// The semantic tokens legend
pub static SEMANTIC_TOKEN_LEGEND: Lazy<SemanticTokensLegend> = Lazy::new(|| SemanticTokensLegend {
    token_types: SEMANTIC_TOKEN_TYPES.clone(),
    token_modifiers: SEMANTIC_TOKEN_MODIFIERS.clone(),
});

/// Token type indices
const TOKEN_KEYWORD: u32 = 0;
const TOKEN_TYPE: u32 = 1;
const TOKEN_FUNCTION: u32 = 2;
const TOKEN_VARIABLE: u32 = 3;
const TOKEN_STRING: u32 = 4;
const TOKEN_NUMBER: u32 = 5;
const TOKEN_OPERATOR: u32 = 6;
const TOKEN_COMMENT: u32 = 7;
const TOKEN_CLASS: u32 = 8;
#[allow(dead_code)]
const TOKEN_NAMESPACE: u32 = 11;

/// Get semantic tokens for a document
pub fn get_semantic_tokens(text: &str) -> Vec<SemanticToken> {
    let mut tokens = Vec::new();
    let mut prev_line = 0u32;
    let mut prev_char = 0u32;

    for (line_num, line) in text.lines().enumerate() {
        let line_num = line_num as u32;

        // Build char indices for safe slicing
        let char_indices: Vec<(usize, char)> = line.char_indices().collect();
        let mut char_pos = 0usize; // Position in char_indices

        // Handle comments - find the character position of #
        if let Some(comment_char_pos) = char_indices.iter().position(|(_, c)| *c == '#') {
            let _comment_byte_start = char_indices[comment_char_pos].0;
            let comment_char_len = char_indices.len() - comment_char_pos;

            let delta_line = line_num - prev_line;
            let delta_start = if delta_line == 0 {
                comment_char_pos as u32 - prev_char
            } else {
                comment_char_pos as u32
            };

            tokens.push(SemanticToken {
                delta_line,
                delta_start,
                length: comment_char_len as u32,
                token_type: TOKEN_COMMENT,
                token_modifiers_bitset: 0,
            });

            prev_line = line_num;
            prev_char = comment_char_pos as u32;

            // Skip the rest of the line (it's a comment)
            continue;
        }

        // Tokenize the line using character positions
        while char_pos < char_indices.len() {
            let (byte_offset, current_char) = char_indices[char_pos];

            // Skip whitespace
            if current_char.is_whitespace() {
                char_pos += 1;
                continue;
            }

            // Get remaining string safely from byte offset
            let remaining = &line[byte_offset..];

            // Try to match patterns
            if let Some((byte_len, token_type)) = match_token(remaining) {
                // Calculate character length from byte length
                let char_len = remaining[..byte_len].chars().count();

                let delta_line = line_num - prev_line;
                let delta_start = if delta_line == 0 {
                    char_pos as u32 - prev_char
                } else {
                    char_pos as u32
                };

                tokens.push(SemanticToken {
                    delta_line,
                    delta_start,
                    length: char_len as u32,
                    token_type,
                    token_modifiers_bitset: 0,
                });

                prev_line = line_num;
                prev_char = char_pos as u32;
                char_pos += char_len;
            } else {
                char_pos += 1;
            }
        }
    }

    tokens
}

/// Match a token at the start of the string, returning (length, token_type)
fn match_token(s: &str) -> Option<(usize, u32)> {
    // Keywords
    let keywords = [
        "stream", "event", "pattern", "from", "let", "var", "const", "fn", "config", "if", "elif",
        "else", "then", "match", "for", "while", "in", "break", "continue", "return", "and", "or",
        "not", "true", "false", "null", "within", "SEQ", "AND", "OR", "NOT",
    ];

    for kw in keywords {
        if s.starts_with(kw)
            && !s[kw.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_')
        {
            return Some((kw.len(), TOKEN_KEYWORD));
        }
    }

    // Types
    let types = [
        "int",
        "float",
        "bool",
        "str",
        "string",
        "timestamp",
        "duration",
        "list",
        "map",
        "any",
        "Stream",
    ];

    for ty in types {
        if s.starts_with(ty)
            && !s[ty.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_')
        {
            return Some((ty.len(), TOKEN_TYPE));
        }
    }

    // Built-in functions
    let functions = [
        "sum",
        "avg",
        "count",
        "min",
        "max",
        "stddev",
        "variance",
        "first",
        "last",
        "collect",
        "distinct",
        "tumbling",
        "sliding",
        "session_window",
        "now",
        "uuid",
        "abs",
        "sqrt",
        "pow",
        "log",
        "exp",
        "floor",
        "ceil",
        "round",
        "len",
        "map",
        "filter",
        "reduce",
        "flatten",
        "zip",
        "enumerate",
        "range",
        "sort",
        "reverse",
        "join",
        "split",
        "trim",
        "upper",
        "lower",
        "contains",
        "starts_with",
        "ends_with",
        "replace",
        "substring",
        "parse_json",
        "to_json",
        "parse_timestamp",
        "format_timestamp",
        "embedding",
        "linear_regression_slope",
        "baseline_pressure",
        "baseline_energy",
        "baseline_volume",
        "ema",
        "forecast",
        "forecast_probability",
        "forecast_time",
        "forecast_state",
        "forecast_context_depth",
    ];

    // Check if followed by ( to confirm it's a function call
    for func in functions {
        if let Some(after) = s.strip_prefix(func) {
            if !after.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
                // Check if it's followed by ( (possibly with whitespace)
                let trimmed = after.trim_start();
                if trimmed.starts_with('(') {
                    return Some((func.len(), TOKEN_FUNCTION));
                }
            }
        }
    }

    // Stream operations (after .)
    let operations = [
        "where",
        "select",
        "aggregate",
        "window",
        "partition_by",
        "order_by",
        "limit",
        "distinct",
        "emit",
        "to",
        "tap",
        "pattern",
        "concurrent",
        "process",
        "on_error",
        "collect",
        "merge",
        "join",
        "on",
    ];

    for op in operations {
        if s.starts_with(op)
            && !s[op.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_')
        {
            // This would need more context to determine if after a dot
            // For now, just continue
        }
    }

    // Strings
    if let Some(rest) = s.strip_prefix('"') {
        let end = rest.find('"').map(|i| i + 2).unwrap_or(s.len());
        return Some((end, TOKEN_STRING));
    }
    if let Some(rest) = s.strip_prefix('\'') {
        let end = rest.find('\'').map(|i| i + 2).unwrap_or(s.len());
        return Some((end, TOKEN_STRING));
    }

    // Numbers (including durations)
    if s.starts_with(|c: char| c.is_ascii_digit()) {
        let mut len = 0;
        let chars: Vec<char> = s.chars().collect();

        // Integer part
        while len < chars.len() && chars[len].is_ascii_digit() {
            len += 1;
        }

        // Decimal part
        if len < chars.len()
            && chars[len] == '.'
            && len + 1 < chars.len()
            && chars[len + 1].is_ascii_digit()
        {
            len += 1;
            while len < chars.len() && chars[len].is_ascii_digit() {
                len += 1;
            }
        }

        // Duration suffix
        let remaining = &s[len..];
        for suffix in ["ms", "s", "m", "h", "d"] {
            if remaining.starts_with(suffix) {
                len += suffix.len();
                break;
            }
        }

        return Some((len, TOKEN_NUMBER));
    }

    // Timestamp literal @2024-01-01T...
    if s.starts_with('@') {
        let mut len = 1;
        let chars: Vec<char> = s.chars().collect();
        while len < chars.len()
            && (chars[len].is_ascii_alphanumeric()
                || chars[len] == '-'
                || chars[len] == ':'
                || chars[len] == 'T'
                || chars[len] == 'Z'
                || chars[len] == '+'
                || chars[len] == '.')
        {
            len += 1;
        }
        return Some((len, TOKEN_NUMBER));
    }

    // Operators
    let operators = [
        "=>", "->", "..", "..=", "==", "!=", "<=", ">=", "+=", "-=", "*=", "/=", "?.", "??",
    ];
    for op in operators {
        if s.starts_with(op) {
            return Some((op.len(), TOKEN_OPERATOR));
        }
    }

    // Type names (PascalCase)
    if s.starts_with(|c: char| c.is_uppercase()) {
        let len = s
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .count();
        if len > 0 {
            return Some((len, TOKEN_CLASS));
        }
    }

    // Identifiers
    if s.starts_with(|c: char| c.is_alphabetic() || c == '_') {
        let len = s
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .count();
        if len > 0 {
            return Some((len, TOKEN_VARIABLE));
        }
    }

    None
}

/// Get document symbols (outline)
#[allow(deprecated)]
pub fn get_document_symbols(text: &str) -> Vec<SymbolInformation> {
    let mut symbols = Vec::new();

    // Create a dummy URI - the actual URI will be provided by the caller
    let uri = Url::parse("file:///dummy").unwrap();

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();

        // Stream declarations
        if trimmed.starts_with("stream ") {
            if let Some(name) = extract_identifier(trimmed, "stream ") {
                symbols.push(SymbolInformation {
                    name: name.to_string(),
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    location: Location {
                        uri: uri.clone(),
                        range: Range {
                            start: Position {
                                line: line_num as u32,
                                character: 0,
                            },
                            end: Position {
                                line: line_num as u32,
                                character: line.len() as u32,
                            },
                        },
                    },
                    container_name: None,
                });
            }
        }

        // Event declarations
        if trimmed.starts_with("event ") {
            if let Some(name) = extract_identifier(trimmed, "event ") {
                symbols.push(SymbolInformation {
                    name: name.to_string(),
                    kind: SymbolKind::CLASS,
                    tags: None,
                    deprecated: None,
                    location: Location {
                        uri: uri.clone(),
                        range: Range {
                            start: Position {
                                line: line_num as u32,
                                character: 0,
                            },
                            end: Position {
                                line: line_num as u32,
                                character: line.len() as u32,
                            },
                        },
                    },
                    container_name: None,
                });
            }
        }

        // Pattern declarations
        if trimmed.starts_with("pattern ") {
            if let Some(name) = extract_identifier(trimmed, "pattern ") {
                symbols.push(SymbolInformation {
                    name: name.to_string(),
                    kind: SymbolKind::FUNCTION,
                    tags: None,
                    deprecated: None,
                    location: Location {
                        uri: uri.clone(),
                        range: Range {
                            start: Position {
                                line: line_num as u32,
                                character: 0,
                            },
                            end: Position {
                                line: line_num as u32,
                                character: line.len() as u32,
                            },
                        },
                    },
                    container_name: None,
                });
            }
        }

        // Function declarations
        if trimmed.starts_with("fn ") {
            if let Some(name) = extract_identifier(trimmed, "fn ") {
                symbols.push(SymbolInformation {
                    name: name.to_string(),
                    kind: SymbolKind::FUNCTION,
                    tags: None,
                    deprecated: None,
                    location: Location {
                        uri: uri.clone(),
                        range: Range {
                            start: Position {
                                line: line_num as u32,
                                character: 0,
                            },
                            end: Position {
                                line: line_num as u32,
                                character: line.len() as u32,
                            },
                        },
                    },
                    container_name: None,
                });
            }
        }

        // Variable declarations
        if trimmed.starts_with("let ")
            || trimmed.starts_with("var ")
            || trimmed.starts_with("const ")
        {
            let prefix = if trimmed.starts_with("let ") {
                "let "
            } else if trimmed.starts_with("var ") {
                "var "
            } else {
                "const "
            };

            if let Some(name) = extract_identifier(trimmed, prefix) {
                symbols.push(SymbolInformation {
                    name: name.to_string(),
                    kind: SymbolKind::VARIABLE,
                    tags: None,
                    deprecated: None,
                    location: Location {
                        uri: uri.clone(),
                        range: Range {
                            start: Position {
                                line: line_num as u32,
                                character: 0,
                            },
                            end: Position {
                                line: line_num as u32,
                                character: line.len() as u32,
                            },
                        },
                    },
                    container_name: None,
                });
            }
        }
    }

    symbols
}

/// Extract an identifier after a keyword
fn extract_identifier<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    let after_prefix = line.strip_prefix(prefix)?;
    let end = after_prefix
        .find(|c: char| !c.is_alphanumeric() && c != '_')
        .unwrap_or(after_prefix.len());

    if end > 0 {
        Some(&after_prefix[..end])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_identifier() {
        assert_eq!(
            extract_identifier("stream SensorData from", "stream "),
            Some("SensorData")
        );
        assert_eq!(
            extract_identifier("event TempReading {", "event "),
            Some("TempReading")
        );
        assert_eq!(extract_identifier("let x = 5", "let "), Some("x"));
    }

    #[test]
    fn test_get_document_symbols() {
        let text = r#"
stream SensorData from "mqtt://..."
event TempReading { value: float }
let threshold = 25
fn process(x: int) -> int { x * 2 }
"#;
        let symbols = get_document_symbols(text);
        assert_eq!(symbols.len(), 4);
    }
}
