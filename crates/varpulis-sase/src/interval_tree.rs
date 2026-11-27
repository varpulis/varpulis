//! An interval tree for the predicate index: a treap ordered by
//! `(lo, slot)`, each node knowing which node of its subtree holds the
//! highest upper bound.
//!
//! A stabbing query (every interval holding `x`) descends only into the
//! subtrees whose highest upper bound reaches `x` and stops at the first
//! node starting above `x`, so it costs `O(log n)` expected per interval
//! reported, whatever the spread of the interval lengths. Insertion and
//! removal cost `O(log n)` expected.
//!
//! Nodes live in an arena and refer to each other by index; the subtree
//! maximum is kept as the index of the node that holds it, so no bound is
//! ever cloned to maintain it.

use std::cmp::Ordering;

/// Position of a parked run in the index's slab.
pub(crate) type Slot = u32;

const NIL: u32 = u32::MAX;

#[derive(Debug)]
struct Node<K> {
    lo: K,
    hi: K,
    slot: Slot,
    priority: u32,
    left: u32,
    right: u32,
    /// The node of this subtree with the highest `hi`.
    max: u32,
}

/// Closed intervals `[lo, hi]`, each attached to a slot.
#[derive(Debug)]
pub(crate) struct IntervalTree<K> {
    nodes: Vec<Node<K>>,
    free: Vec<u32>,
    root: u32,
    len: usize,
    /// xorshift32 state, for the treap priorities.
    seed: u32,
}

impl<K> Default for IntervalTree<K> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            free: Vec::new(),
            root: NIL,
            len: 0,
            seed: 0x9E37_79B9,
        }
    }
}

impl<K: Ord> IntervalTree<K> {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// File `[lo, hi]` under `slot`. `(lo, slot)` must not be in the tree.
    pub(crate) fn insert(&mut self, lo: K, hi: K, slot: Slot) {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 17;
        self.seed ^= self.seed << 5;
        let node = Node {
            lo,
            hi,
            slot,
            priority: self.seed,
            left: NIL,
            right: NIL,
            max: NIL,
        };
        let id = if let Some(id) = self.free.pop() {
            self.nodes[id as usize] = node;
            id
        } else {
            self.nodes.push(node);
            u32::try_from(self.nodes.len() - 1).expect("more than u32::MAX intervals")
        };
        self.nodes[id as usize].max = id;
        let (left, right) = self.split(self.root, id);
        let joined = self.merge(left, id);
        self.root = self.merge(joined, right);
        self.len += 1;
    }

    /// Remove the interval filed under `(lo, slot)`; `false` if there is none.
    pub(crate) fn remove(&mut self, lo: &K, slot: Slot) -> bool {
        let (root, removed) = self.remove_in(self.root, lo, slot);
        self.root = root;
        if removed {
            self.len -= 1;
        }
        removed
    }

    /// Append the slot of every interval holding `x` (`lo <= x <= hi`).
    pub(crate) fn stab(&self, x: &K, out: &mut Vec<Slot>) {
        self.stab_in(self.root, x, out);
    }

    fn stab_in(&self, t: u32, x: &K, out: &mut Vec<Slot>) {
        if t == NIL {
            return;
        }
        let node = &self.nodes[t as usize];
        // Nothing below reaches `x`.
        if self.nodes[node.max as usize].hi < *x {
            return;
        }
        self.stab_in(node.left, x, out);
        // This node and everything to its right start above `x`.
        if node.lo > *x {
            return;
        }
        if node.hi >= *x {
            out.push(node.slot);
        }
        self.stab_in(node.right, x, out);
    }

    /// Order of node `a` against the key `(lo, slot)` of node `b`.
    fn cmp_nodes(&self, a: u32, b: u32) -> Ordering {
        let (a, b) = (&self.nodes[a as usize], &self.nodes[b as usize]);
        a.lo.cmp(&b.lo).then(a.slot.cmp(&b.slot))
    }

    /// Recompute the subtree maximum of `t` from its children.
    fn update(&mut self, t: u32) {
        let node = &self.nodes[t as usize];
        let mut max = t;
        for child in [node.left, node.right] {
            if child != NIL {
                let candidate = self.nodes[child as usize].max;
                if self.nodes[candidate as usize].hi > self.nodes[max as usize].hi {
                    max = candidate;
                }
            }
        }
        self.nodes[t as usize].max = max;
    }

    /// Split `t` into the nodes ordered before node `key`, and the others.
    fn split(&mut self, t: u32, key: u32) -> (u32, u32) {
        if t == NIL {
            return (NIL, NIL);
        }
        if self.cmp_nodes(t, key) == Ordering::Less {
            let (left, right) = self.split(self.nodes[t as usize].right, key);
            self.nodes[t as usize].right = left;
            self.update(t);
            (t, right)
        } else {
            let (left, right) = self.split(self.nodes[t as usize].left, key);
            self.nodes[t as usize].left = right;
            self.update(t);
            (left, t)
        }
    }

    /// Join two treaps, every key of `a` ordered before every key of `b`.
    fn merge(&mut self, a: u32, b: u32) -> u32 {
        if a == NIL {
            return b;
        }
        if b == NIL {
            return a;
        }
        if self.nodes[a as usize].priority > self.nodes[b as usize].priority {
            let right = self.merge(self.nodes[a as usize].right, b);
            self.nodes[a as usize].right = right;
            self.update(a);
            a
        } else {
            let left = self.merge(a, self.nodes[b as usize].left);
            self.nodes[b as usize].left = left;
            self.update(b);
            b
        }
    }

    fn remove_in(&mut self, t: u32, lo: &K, slot: Slot) -> (u32, bool) {
        if t == NIL {
            return (NIL, false);
        }
        let node = &self.nodes[t as usize];
        match lo.cmp(&node.lo).then(slot.cmp(&node.slot)) {
            Ordering::Equal => {
                let (left, right) = (node.left, node.right);
                self.free.push(t);
                (self.merge(left, right), true)
            }
            Ordering::Less => {
                let (left, removed) = self.remove_in(node.left, lo, slot);
                self.nodes[t as usize].left = left;
                self.update(t);
                (t, removed)
            }
            Ordering::Greater => {
                let (right, removed) = self.remove_in(node.right, lo, slot);
                self.nodes[t as usize].right = right;
                self.update(t);
                (t, removed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brute(intervals: &[(i64, i64, Slot)], x: i64) -> Vec<Slot> {
        let mut v: Vec<Slot> = intervals
            .iter()
            .filter(|(lo, hi, _)| *lo <= x && x <= *hi)
            .map(|&(_, _, s)| s)
            .collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn stabbing_matches_a_linear_scan() {
        let mut rng: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut tree = IntervalTree::default();
        let mut live: Vec<(i64, i64, Slot)> = Vec::new();
        for slot in 0..4000u32 {
            let lo = (next() % 1000) as i64;
            // Lengths from 0 to the whole domain, and some empty ones.
            let len = match next() % 4 {
                0 => (next() % 1000) as i64,
                1 => -1,
                _ => (next() % 5) as i64,
            };
            tree.insert(lo, lo + len, slot);
            live.push((lo, lo + len, slot));
            if next() % 3 == 0 {
                let i = (next() as usize) % live.len();
                let (lo, _, s) = live.swap_remove(i);
                assert!(tree.remove(&lo, s));
            }
            if slot % 50 == 0 {
                for x in [-1, 0, 7, 500, 999, 1500] {
                    let mut got = Vec::new();
                    tree.stab(&x, &mut got);
                    got.sort_unstable();
                    assert_eq!(got, brute(&live, x), "x = {x}");
                }
            }
        }
        assert_eq!(tree.len(), live.len());
        assert!(!tree.remove(&-5, 0));
        for (lo, _, s) in std::mem::take(&mut live) {
            assert!(tree.remove(&lo, s));
        }
        assert!(tree.is_empty());
    }
}
