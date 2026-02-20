//! Go-to-definition and find-references for the VPL LSP.

use tower_lsp::lsp_types::{Location, Position, Range, Url};
use varpulis_core::span::Span;
use varpulis_core::validate::scope::SymbolTable;

/// Find the definition of the symbol at the given cursor position.
///
/// Returns a `Location` pointing to the definition span if the word under the
/// cursor is a known symbol (event, stream, function, connector, context,
/// pattern, variable, or type alias).
pub fn get_definition(text: &str, position: Position, uri: &Url) -> Option<Location> {
    let word = word_at_position(text, position)?;
    let symbols = build_symbol_table(text)?;

    let def_span = lookup_definition_span(&symbols, &word)?;
    Some(span_to_location(text, def_span, uri))
}

/// Find all references to the symbol at the given cursor position.
///
/// Returns locations for every occurrence of the identifier, including
/// the declaration itself.
pub fn get_references(text: &str, position: Position, uri: &Url) -> Option<Vec<Location>> {
    let word = word_at_position(text, position)?;
    let symbols = build_symbol_table(text)?;

    // The word must be a known symbol for us to find references.
    let _def_span = lookup_definition_span(&symbols, &word)?;

    // Find all whole-word occurrences of the identifier in the document.
    // We search the full text because the parser's statement-level spans are
    // not always aligned to syntactic boundaries.
    let full_span = Span::new(0, text.len());
    let mut occurrences: Vec<Span> = Vec::new();
    find_name_in_span(text, &word, full_span, &mut occurrences);

    if occurrences.is_empty() {
        return None;
    }

    let locations = occurrences
        .iter()
        .map(|span| span_to_location(text, *span, uri))
        .collect();

    Some(locations)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse the document and run validation to obtain the symbol table.
fn build_symbol_table(text: &str) -> Option<SymbolTable> {
    let program = varpulis_parser::parse(text).ok()?;
    let (_result, symbols) = varpulis_core::validate::validate_with_symbols(text, &program);
    Some(symbols)
}

/// Extract the word (identifier) at the given LSP position (0-indexed line/col).
fn word_at_position(text: &str, position: Position) -> Option<String> {
    let lines: Vec<&str> = text.lines().collect();
    let line = lines.get(position.line as usize)?;
    let col = position.character as usize;

    if col > line.len() {
        return None;
    }

    let chars: Vec<char> = line.chars().collect();

    // Find start of word
    let mut start = col;
    while start > 0 && is_ident_char(chars.get(start - 1).copied()) {
        start -= 1;
    }

    // Find end of word
    let mut end = col;
    while end < chars.len() && is_ident_char(chars.get(end).copied()) {
        end += 1;
    }

    if start == end {
        return None;
    }

    Some(chars[start..end].iter().collect())
}

fn is_ident_char(c: Option<char>) -> bool {
    c.map(|c| c.is_alphanumeric() || c == '_').unwrap_or(false)
}

/// Look up a name in the symbol table and return its definition span.
fn lookup_definition_span(symbols: &SymbolTable, name: &str) -> Option<Span> {
    if let Some(info) = symbols.events.get(name) {
        return Some(info.span);
    }
    if let Some(info) = symbols.streams.get(name) {
        return Some(info.span);
    }
    if let Some(info) = symbols.functions.get(name) {
        return Some(info.span);
    }
    if let Some(info) = symbols.connectors.get(name) {
        return Some(info.span);
    }
    if let Some(info) = symbols.contexts.get(name) {
        return Some(info.span);
    }
    if let Some(info) = symbols.patterns.get(name) {
        return Some(info.span);
    }
    if let Some(info) = symbols.variables.get(name) {
        return Some(info.span);
    }
    if let Some(info) = symbols.types.get(name) {
        return Some(info.span);
    }
    None
}

/// Convert a byte-offset `Span` to an LSP `Location` (0-indexed line/col).
fn span_to_location(text: &str, span: Span, uri: &Url) -> Location {
    let (start_line, start_col) = byte_offset_to_position(text, span.start);
    let (end_line, end_col) = byte_offset_to_position(text, span.end);

    Location {
        uri: uri.clone(),
        range: Range {
            start: Position {
                line: start_line as u32,
                character: start_col as u32,
            },
            end: Position {
                line: end_line as u32,
                character: end_col as u32,
            },
        },
    }
}

/// Convert a byte offset in `source` to 0-indexed (line, column).
fn byte_offset_to_position(source: &str, position: usize) -> (usize, usize) {
    let mut line = 0;
    let mut col = 0;
    let mut pos = 0;

    for ch in source.chars() {
        if pos >= position {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 0;
        } else {
            col += 1;
        }
        pos += ch.len_utf8();
    }

    (line, col)
}

/// Find the byte-offset span of `name` as a whole word within `stmt_span`
/// in the source text and push it into `out`.
///
/// We search for all word-boundary occurrences of `name` within the snippet
/// to handle cases where the same name appears multiple times in a statement.
fn find_name_in_span(text: &str, name: &str, stmt_span: Span, out: &mut Vec<Span>) {
    let end = stmt_span.end.min(text.len());
    let snippet = match text.get(stmt_span.start..end) {
        Some(s) => s,
        None => return,
    };

    let mut search_from = 0;
    while let Some(offset) = snippet[search_from..].find(name) {
        let abs_start = stmt_span.start + search_from + offset;
        let abs_end = abs_start + name.len();

        // Check word boundaries: the character before and after should not be
        // alphanumeric or underscore (i.e., the match must be a whole word).
        let before_ok = abs_start == 0
            || !text
                .as_bytes()
                .get(abs_start - 1)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_');
        let after_ok = abs_end >= text.len()
            || !text
                .as_bytes()
                .get(abs_end)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_');

        if before_ok && after_ok {
            out.push(Span::new(abs_start, abs_end));
        }

        search_from += offset + name.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_word_at_position_simple() {
        let text = "stream SensorData = Sensor";
        assert_eq!(
            word_at_position(
                text,
                Position {
                    line: 0,
                    character: 0
                }
            ),
            Some("stream".to_string())
        );
        assert_eq!(
            word_at_position(
                text,
                Position {
                    line: 0,
                    character: 7
                }
            ),
            Some("SensorData".to_string())
        );
        assert_eq!(
            word_at_position(
                text,
                Position {
                    line: 0,
                    character: 20
                }
            ),
            Some("Sensor".to_string())
        );
    }

    #[test]
    fn test_word_at_position_multiline() {
        let text = "event Trade:\n    price: float";
        assert_eq!(
            word_at_position(
                text,
                Position {
                    line: 0,
                    character: 6
                }
            ),
            Some("Trade".to_string())
        );
        assert_eq!(
            word_at_position(
                text,
                Position {
                    line: 1,
                    character: 4
                }
            ),
            Some("price".to_string())
        );
    }

    #[test]
    fn test_word_at_position_empty() {
        assert_eq!(
            word_at_position(
                "",
                Position {
                    line: 0,
                    character: 0
                }
            ),
            None
        );
    }

    #[test]
    fn test_byte_offset_to_position() {
        let text = "line0\nline1\nline2";
        assert_eq!(byte_offset_to_position(text, 0), (0, 0));
        assert_eq!(byte_offset_to_position(text, 5), (0, 5));
        assert_eq!(byte_offset_to_position(text, 6), (1, 0));
        assert_eq!(byte_offset_to_position(text, 12), (2, 0));
    }

    #[test]
    fn test_find_name_in_span() {
        let text = "stream Filtered = Trade.where(price > 100)";
        let span = Span::new(0, text.len());
        let mut out = Vec::new();
        find_name_in_span(text, "Trade", span, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(&text[out[0].start..out[0].end], "Trade");
    }

    #[test]
    fn test_find_name_word_boundary() {
        let text = "stream Trader = Trade";
        let span = Span::new(0, text.len());
        let mut out = Vec::new();
        find_name_in_span(text, "Trade", span, &mut out);
        // Should find only "Trade" at the end, not the "Trade" substring of "Trader"
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].start, 16);
    }

    #[test]
    fn test_references_event_in_stream() {
        use tower_lsp::lsp_types::Url;
        let code = "event Trade:\n    price: float\n\nstream Filtered = Trade\n    .where(price > 100)\n    .emit()";
        let uri = Url::parse("file:///test.vpl").unwrap();
        let refs = get_references(
            code,
            Position {
                line: 0,
                character: 6,
            },
            &uri,
        );
        assert!(refs.is_some(), "should find references for Trade");
        let refs = refs.unwrap();
        assert!(
            refs.len() >= 2,
            "expected >=2 references for Trade, got {}",
            refs.len()
        );
    }

    #[test]
    fn test_goto_def_in_pattern() {
        use tower_lsp::lsp_types::Url;
        let code = "event Warning:\n    level: int\n\nevent Error:\n    msg: str\n\npattern Alert = SEQ(Warning, Error) within 5m";
        let uri = Url::parse("file:///test.vpl").unwrap();
        let loc = get_definition(
            code,
            Position {
                line: 6,
                character: 20,
            },
            &uri,
        );
        assert!(
            loc.is_some(),
            "should find definition for Warning in pattern"
        );
    }

    #[test]
    fn test_references_followed_by() {
        use tower_lsp::lsp_types::Url;
        let code =
            "event A:\n    val: int\n\nevent B:\n    val: int\n\nstream S = A as a -> B as b .within(5m)\n    .emit()";
        let uri = Url::parse("file:///test.vpl").unwrap();

        let refs = get_references(
            code,
            Position {
                line: 3,
                character: 6,
            },
            &uri,
        );
        assert!(refs.is_some(), "should find references for B");
        let refs = refs.unwrap();
        assert!(
            refs.len() >= 2,
            "expected >=2 refs for B, got {}",
            refs.len()
        );
    }
}
