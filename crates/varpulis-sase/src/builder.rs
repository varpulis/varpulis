//! Builder API for SASE patterns

use super::types::{CompareOp, Predicate, SasePattern};
use std::time::Duration;
use varpulis_core::Value;

/// Builder for SASE patterns
#[derive(Debug)]
pub struct PatternBuilder;

impl PatternBuilder {
    /// Single event pattern
    pub fn event(event_type: &str) -> SasePattern {
        SasePattern::Event {
            event_type: event_type.to_string(),
            predicate: None,
            alias: None,
        }
    }

    /// Event with alias
    pub fn event_as(event_type: &str, alias: &str) -> SasePattern {
        SasePattern::Event {
            event_type: event_type.to_string(),
            predicate: None,
            alias: Some(alias.to_string()),
        }
    }

    /// Event with predicate
    pub fn event_where(event_type: &str, predicate: Predicate) -> SasePattern {
        SasePattern::Event {
            event_type: event_type.to_string(),
            predicate: Some(predicate),
            alias: None,
        }
    }

    /// Sequence pattern
    pub fn seq(patterns: Vec<SasePattern>) -> SasePattern {
        SasePattern::Seq(patterns)
    }

    /// AND pattern
    pub fn and(left: SasePattern, right: SasePattern) -> SasePattern {
        SasePattern::And(Box::new(left), Box::new(right))
    }

    /// OR pattern
    pub fn or(left: SasePattern, right: SasePattern) -> SasePattern {
        SasePattern::Or(Box::new(left), Box::new(right))
    }

    /// NOT pattern
    pub fn not(inner: SasePattern) -> SasePattern {
        SasePattern::Not(Box::new(inner))
    }

    /// Kleene plus (one or more)
    pub fn one_or_more(inner: SasePattern) -> SasePattern {
        SasePattern::KleenePlus(Box::new(inner))
    }

    /// Kleene star (zero or more)
    pub fn zero_or_more(inner: SasePattern) -> SasePattern {
        SasePattern::KleeneStar(Box::new(inner))
    }

    /// Temporal constraint
    pub fn within(inner: SasePattern, duration: Duration) -> SasePattern {
        SasePattern::Within(Box::new(inner), duration)
    }

    /// Field equals value predicate
    pub fn field_eq(field: &str, value: Value) -> Predicate {
        Predicate::Compare {
            field: field.to_string(),
            op: CompareOp::Eq,
            value,
        }
    }

    /// Field reference predicate (compare to captured event)
    pub fn field_ref_eq(field: &str, ref_alias: &str, ref_field: &str) -> Predicate {
        Predicate::CompareRef {
            field: field.to_string(),
            op: CompareOp::Eq,
            ref_alias: ref_alias.to_string(),
            ref_field: ref_field.to_string(),
        }
    }
}
