//! # varpulis-zdd
//!
//! Zero-suppressed Decision Diagrams (ZDD) for efficient set manipulation in CEP patterns.
//!
//! This crate provides a compact representation of families of sets (set of sets),
//! which is particularly useful for representing Kleene pattern matches in Complex
//! Event Processing (CEP).
//!
//! ## The Problem
//!
//! In CEP, Kleene patterns like `A+` (one or more A events) can match exponentially
//! many combinations. For n matching events, there are O(2^n) possible subsets.
//! Naively storing all combinations leads to memory explosion.
//!
//! ## The Solution
//!
//! ZDDs represent a family of sets in a compact, canonical form. The key operation
//! is `product_with_optional`, which extends each set with an optional new element
//! without explicitly enumerating all combinations.
//!
//! ## Example
//!
//! ```
//! use varpulis_zdd::Zdd;
//!
//! // Start with the base case: {∅}
//! let mut zdd = Zdd::base();
//!
//! // Simulate Kleene matching: each event can be included or not
//! for event_idx in 0..20 {
//!     zdd = zdd.product_with_optional(event_idx);
//! }
//!
//! // We now have 2^20 = 1,048,576 combinations
//! assert_eq!(zdd.count(), 1_048_576);
//!
//! // But far fewer nodes than 2^20 sets - polynomial not exponential!
//! assert!(zdd.node_count() < 500);
//!
//! // We can check membership efficiently
//! assert!(zdd.contains(&[0, 5, 10, 15, 19]));
//!
//! // Or iterate lazily
//! for (i, combination) in zdd.iter().enumerate() {
//!     if i >= 5 { break; }
//!     println!("Combination {}: {:?}", i, combination);
//! }
//! ```
//!
//! ## Key Operations
//!
//! - [`Zdd::base()`] - Create {∅}, the starting point for Kleene matching
//! - [`Zdd::product_with_optional()`] - Extend each set with optional element
//! - [`Zdd::union()`], [`Zdd::intersection()`], [`Zdd::difference()`] - Set operations
//! - [`Zdd::count()`] - Count sets without enumeration
//! - [`Zdd::iter()`] - Lazy iteration over all sets
//! - [`Zdd::to_dot()`] - Export to Graphviz for visualization

mod debug;
mod iter;
mod node;
mod ops;
mod refs;
mod table;
mod zdd;

pub use iter::ZddIterator;
pub use node::ZddNode;
pub use refs::ZddRef;
pub use table::{TableStats, UniqueTable};
pub use zdd::Zdd;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashSet};

    /// Test equivalence with naive HashSet<BTreeSet> implementation
    #[test]
    fn test_equivalence_with_naive() {
        let mut naive: HashSet<BTreeSet<u32>> = HashSet::new();
        naive.insert(BTreeSet::new()); // {∅}

        let mut zdd = Zdd::base();

        for var in 0..12 {
            // Naive operation
            let mut new_naive = naive.clone();
            for s in &naive {
                let mut extended = s.clone();
                extended.insert(var);
                new_naive.insert(extended);
            }
            naive = new_naive;

            // ZDD operation
            zdd = zdd.product_with_optional(var);

            // Verify count matches
            assert_eq!(zdd.count(), naive.len(), "Count mismatch at var {}", var);

            // Verify all sets match
            for combination in zdd.iter() {
                let set: BTreeSet<u32> = combination.into_iter().collect();
                assert!(
                    naive.contains(&set),
                    "ZDD contains {:?} which is not in naive",
                    set
                );
            }
        }
    }

    /// Test that ZDD operations are canonical
    #[test]
    fn test_canonicity() {
        // Build the same family two different ways
        let a = Zdd::singleton(1).union(&Zdd::singleton(2));
        let b = Zdd::singleton(2).union(&Zdd::singleton(1));

        // Both should have the same structure
        assert_eq!(a.count(), b.count());
        assert_eq!(a.node_count(), b.node_count());
    }

    /// Test set algebra laws
    #[test]
    fn test_algebra_laws() {
        let a = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[3]));
        let b = Zdd::from_set(&[2, 3]).union(&Zdd::from_set(&[4]));
        let c = Zdd::from_set(&[1]).union(&Zdd::from_set(&[5]));

        // Union is commutative
        assert_eq!(a.union(&b).count(), b.union(&a).count());

        // Union is associative
        assert_eq!(a.union(&b).union(&c).count(), a.union(&b.union(&c)).count());

        // Intersection is commutative
        assert_eq!(a.intersection(&b).count(), b.intersection(&a).count());

        // A ∩ A = A
        assert_eq!(a.intersection(&a).count(), a.count());

        // A ∪ A = A
        assert_eq!(a.union(&a).count(), a.count());

        // A \ A = ∅
        assert!(a.difference(&a).is_empty());
    }

    /// Test large-scale efficiency
    #[test]
    fn test_large_scale() {
        let mut zdd = Zdd::base();

        // 25 events = 33 million combinations
        for i in 0..25 {
            zdd = zdd.product_with_optional(i);
        }

        assert_eq!(zdd.count(), 1 << 25); // 33,554,432

        // ZDD nodes grow polynomially, not exponentially
        // For product_with_optional, it's O(n^2) in the worst case
        // but still far better than 2^n sets
        let node_count = zdd.node_count();
        assert!(
            node_count < 1000,
            "Expected fewer than 1000 nodes for 25 variables, got {}",
            node_count
        );

        // Verify a specific combination
        assert!(zdd.contains(&[0, 5, 10, 15, 20, 24]));
        assert!(zdd.contains(&[]));
        assert!(zdd.contains(&(0..25).collect::<Vec<_>>()));
    }

    /// Test Clone implementation
    #[test]
    fn test_clone() {
        let zdd = Zdd::base()
            .product_with_optional(0)
            .product_with_optional(1);
        let cloned = zdd.clone();

        assert_eq!(zdd.count(), cloned.count());
        assert_eq!(zdd.node_count(), cloned.node_count());
    }

    /// Test Default implementation
    #[test]
    fn test_default() {
        let zdd = Zdd::default();
        assert!(zdd.is_empty());
    }

    /// Test Debug implementation
    #[test]
    fn test_debug() {
        let zdd = Zdd::base().product_with_optional(0);
        let debug_str = format!("{:?}", zdd);
        assert!(debug_str.contains("Zdd"));
    }
}
