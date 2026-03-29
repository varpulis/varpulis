//! Pattern analyzer for extracting structural info from SASE patterns
//!
//! Used by the Hamlet integration to build MergedTemplate and QueryRegistration
//! from VPL stream definitions containing `.trend_aggregate()`.

use varpulis_core::ast::{Expr, FollowedByClause, StreamSource, TrendAggItem};

use crate::greta::GretaAggregate;

/// Extracted Kleene info for a Kleene closure in a pattern
pub struct KleeneInfo {
    pub event_type: String,
}

/// Extract all event types from a stream source + followed-by chain.
/// Returns them in sequence order.
pub fn extract_event_types(source: &StreamSource, followed_by: &[FollowedByClause]) -> Vec<String> {
    let mut types = Vec::new();

    // Get the initial event type from the source
    match source {
        StreamSource::Ident(name)
        | StreamSource::IdentWithAlias { name, .. }
        | StreamSource::IdentWithFilterAndAlias { name, .. }
        | StreamSource::AllWithAlias { name, .. } => {
            types.push(name.clone());
        }
        StreamSource::Sequence(decl) => {
            for step in &decl.steps {
                types.push(step.event_type.clone());
            }
        }
        _ => {}
    }

    // Add followed-by event types
    for clause in followed_by {
        if !types.contains(&clause.event_type) {
            types.push(clause.event_type.clone());
        }
    }

    types
}

/// Extract Kleene (match_all) positions from followed-by clauses.
/// A clause with `match_all: true` (the `all` keyword) indicates Kleene+.
pub fn extract_kleene_info(
    source: &StreamSource,
    followed_by: &[FollowedByClause],
) -> Vec<KleeneInfo> {
    let mut kleene = Vec::new();

    // Check if source itself is AllWithAlias
    if let StreamSource::AllWithAlias { name, .. } = source {
        kleene.push(KleeneInfo {
            event_type: name.clone(),
        });
    }

    for clause in followed_by {
        if clause.match_all {
            kleene.push(KleeneInfo {
                event_type: clause.event_type.clone(),
            });
        }
    }

    kleene
}

/// Extract within duration from ops (in milliseconds).
pub fn extract_within_ms(within_expr: Option<&Expr>) -> u64 {
    match within_expr {
        Some(Expr::Duration(ns)) => ns / 1_000_000,
        _ => 60_000, // Default 60 seconds
    }
}

/// Convert a TrendAggItem to a GretaAggregate, using the type_indices map.
pub fn trend_item_to_greta(
    item: &TrendAggItem,
    type_indices: &std::collections::HashMap<String, u16>,
) -> GretaAggregate {
    match item.func.as_str() {
        "count_trends" => GretaAggregate::CountTrends,
        "count_events" => {
            // arg should be an identifier referencing an alias/event type
            let type_idx = item
                .arg
                .as_ref()
                .and_then(|expr| match expr {
                    Expr::Ident(name) => type_indices.get(name).copied(),
                    _ => None,
                })
                .unwrap_or(0);
            GretaAggregate::CountEvents(type_idx)
        }
        "sum_trends" => {
            let (type_idx, field_idx) = extract_field_ref(&item.arg, type_indices);
            GretaAggregate::Sum {
                type_index: type_idx,
                field_index: field_idx,
            }
        }
        "avg_trends" => {
            let (type_idx, field_idx) = extract_field_ref(&item.arg, type_indices);
            GretaAggregate::Avg {
                type_index: type_idx,
                field_index: field_idx,
            }
        }
        "min_trends" => {
            let (type_idx, field_idx) = extract_field_ref(&item.arg, type_indices);
            GretaAggregate::Min {
                type_index: type_idx,
                field_index: field_idx,
            }
        }
        "max_trends" => {
            let (type_idx, field_idx) = extract_field_ref(&item.arg, type_indices);
            GretaAggregate::Max {
                type_index: type_idx,
                field_index: field_idx,
            }
        }
        _ => GretaAggregate::CountTrends, // Fallback
    }
}

/// Extract type_index and field_index from a member expression like `alias.field`.
fn extract_field_ref(
    arg: &Option<Expr>,
    type_indices: &std::collections::HashMap<String, u16>,
) -> (u16, u16) {
    match arg {
        Some(Expr::Member { expr, member: _ }) => {
            if let Expr::Ident(name) = expr.as_ref() {
                let type_idx = type_indices.get(name).copied().unwrap_or(0);
                // Field index is a placeholder - actual field resolution happens at runtime
                (type_idx, 0)
            } else {
                (0, 0)
            }
        }
        _ => (0, 0),
    }
}
