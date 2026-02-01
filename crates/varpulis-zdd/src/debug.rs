//! ZDD Debug utilities
//!
//! Provides DOT export for visualization and debugging.

use crate::refs::ZddRef;
use crate::zdd::Zdd;
use std::collections::HashSet;
use std::fmt::Write;

impl Zdd {
    /// Export the ZDD to DOT format for visualization with Graphviz.
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// let zdd = Zdd::base().product_with_optional(0).product_with_optional(1);
    /// let dot = zdd.to_dot();
    /// // Save to file and run: dot -Tpng zdd.dot -o zdd.png
    /// ```
    pub fn to_dot(&self) -> String {
        let mut out = String::new();
        let mut visited = HashSet::new();

        writeln!(out, "digraph ZDD {{").unwrap();
        writeln!(out, "  rankdir=TB;").unwrap();
        writeln!(out, "  node [shape=circle];").unwrap();
        writeln!(out).unwrap();

        // Terminal nodes
        writeln!(out, "  // Terminals").unwrap();
        writeln!(
            out,
            "  Empty [shape=box, label=\"⊥\", style=filled, fillcolor=lightgray];"
        )
        .unwrap();
        writeln!(
            out,
            "  Base [shape=box, label=\"⊤\", style=filled, fillcolor=lightgreen];"
        )
        .unwrap();
        writeln!(out).unwrap();

        // Traverse and emit nodes
        writeln!(out, "  // Internal nodes").unwrap();
        self.emit_dot_node(self.root(), &mut out, &mut visited);

        writeln!(out, "}}").unwrap();
        out
    }

    fn emit_dot_node(&self, r: ZddRef, out: &mut String, visited: &mut HashSet<ZddRef>) {
        if visited.contains(&r) {
            return;
        }
        visited.insert(r);

        match r {
            ZddRef::Empty | ZddRef::Base => {
                // Terminals already defined
            }
            ZddRef::Node(id) => {
                let node = self.get_node(id);

                // Define this node
                writeln!(out, "  N{} [label=\"{}\"];", id, node.var).unwrap();

                // LO edge (dashed)
                let lo_name = ref_to_name(node.lo);
                writeln!(out, "  N{} -> {} [style=dashed, label=\"0\"];", id, lo_name).unwrap();

                // HI edge (solid)
                let hi_name = ref_to_name(node.hi);
                writeln!(out, "  N{} -> {} [label=\"1\"];", id, hi_name).unwrap();

                // Recurse
                self.emit_dot_node(node.lo, out, visited);
                self.emit_dot_node(node.hi, out, visited);
            }
        }
    }

    /// Print a text representation of the ZDD structure.
    ///
    /// Useful for debugging small ZDDs.
    pub fn dump(&self) -> String {
        let mut out = String::new();
        writeln!(
            out,
            "ZDD (root={:?}, {} nodes, {} sets)",
            self.root(),
            self.node_count(),
            self.count()
        )
        .unwrap();

        if self.is_empty() {
            writeln!(out, "  <empty>").unwrap();
        } else if self.is_base() {
            writeln!(out, "  {{∅}}").unwrap();
        } else {
            for (i, set) in self.iter().enumerate() {
                if i >= 20 {
                    writeln!(out, "  ... and {} more", self.count() - 20).unwrap();
                    break;
                }
                writeln!(out, "  {:?}", set).unwrap();
            }
        }

        out
    }
}

fn ref_to_name(r: ZddRef) -> String {
    match r {
        ZddRef::Empty => "Empty".to_string(),
        ZddRef::Base => "Base".to_string(),
        ZddRef::Node(id) => format!("N{}", id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_dot_empty() {
        let zdd = Zdd::empty();
        let dot = zdd.to_dot();
        assert!(dot.contains("digraph ZDD"));
        assert!(dot.contains("Empty"));
        assert!(dot.contains("Base"));
    }

    #[test]
    fn test_to_dot_base() {
        let zdd = Zdd::base();
        let dot = zdd.to_dot();
        assert!(dot.contains("digraph ZDD"));
    }

    #[test]
    fn test_to_dot_singleton() {
        let zdd = Zdd::singleton(5);
        let dot = zdd.to_dot();
        assert!(dot.contains("label=\"5\""));
        assert!(dot.contains("-> Base"));
        assert!(dot.contains("-> Empty"));
    }

    #[test]
    fn test_to_dot_product() {
        let zdd = Zdd::base()
            .product_with_optional(0)
            .product_with_optional(1);

        let dot = zdd.to_dot();
        assert!(dot.contains("label=\"0\""));
        assert!(dot.contains("label=\"1\""));
    }

    #[test]
    fn test_dump_empty() {
        let zdd = Zdd::empty();
        let dump = zdd.dump();
        assert!(dump.contains("<empty>"));
    }

    #[test]
    fn test_dump_base() {
        let zdd = Zdd::base();
        let dump = zdd.dump();
        assert!(dump.contains("{∅}"));
    }

    #[test]
    fn test_dump_sets() {
        let zdd = Zdd::singleton(1).union(&Zdd::singleton(2));
        let dump = zdd.dump();
        assert!(dump.contains("[1]") || dump.contains("[2]"));
    }
}
