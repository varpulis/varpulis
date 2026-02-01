//! ZDD Iterator for lazy enumeration
//!
//! Provides lazy iteration over all sets in a ZDD family.

use crate::refs::ZddRef;
use crate::zdd::Zdd;

/// Iterator over all sets in a ZDD family.
///
/// Each iteration yields a `Vec<u32>` containing the elements of one set.
/// The iterator is lazy - it only computes the next set when `next()` is called.
///
/// # Example
/// ```
/// use varpulis_zdd::Zdd;
///
/// let zdd = Zdd::base()
///     .product_with_optional(0)
///     .product_with_optional(1);
///
/// let sets: Vec<Vec<u32>> = zdd.iter().collect();
/// assert_eq!(sets.len(), 4);
/// ```
pub struct ZddIterator<'a> {
    zdd: &'a Zdd,
    /// Stack of (node_ref, current_path, next_branch)
    /// next_branch: 0 = about to explore LO, 1 = about to explore HI, 2 = done
    stack: Vec<(ZddRef, Vec<u32>, u8)>,
}

impl<'a> ZddIterator<'a> {
    pub(crate) fn new(zdd: &'a Zdd) -> Self {
        let stack = if zdd.is_empty() {
            Vec::new()
        } else {
            vec![(zdd.root(), Vec::new(), 0)]
        };

        Self { zdd, stack }
    }
}

impl<'a> Iterator for ZddIterator<'a> {
    type Item = Vec<u32>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (node, path, branch) = self.stack.pop()?;

            match node {
                ZddRef::Empty => {
                    // Dead end, continue with next item on stack
                    continue;
                }
                ZddRef::Base => {
                    // Found a valid set!
                    return Some(path);
                }
                ZddRef::Node(id) => {
                    let n = self.zdd.get_node(id);

                    match branch {
                        0 => {
                            // First visit: explore LO branch, then come back for HI
                            self.stack.push((node, path.clone(), 1));
                            self.stack.push((n.lo, path, 0));
                        }
                        1 => {
                            // Second visit: explore HI branch (include this variable)
                            let mut hi_path = path;
                            hi_path.push(n.var);
                            self.stack.push((n.hi, hi_path, 0));
                        }
                        _ => {
                            // Done with this node
                            continue;
                        }
                    }
                }
            }
        }
    }
}

impl Zdd {
    /// Returns an iterator over all sets in the family.
    ///
    /// Each set is returned as a sorted `Vec<u32>` of element indices.
    ///
    /// # Example
    /// ```
    /// use varpulis_zdd::Zdd;
    ///
    /// let zdd = Zdd::from_set(&[1, 2]).union(&Zdd::from_set(&[3]));
    ///
    /// for set in zdd.iter() {
    ///     println!("{:?}", set);
    /// }
    /// ```
    pub fn iter(&self) -> ZddIterator<'_> {
        ZddIterator::new(self)
    }

    /// Collect all sets into a Vec.
    ///
    /// This is a convenience method equivalent to `zdd.iter().collect()`.
    pub fn to_sets(&self) -> Vec<Vec<u32>> {
        self.iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_iter_empty() {
        let empty = Zdd::empty();
        let sets: Vec<Vec<u32>> = empty.iter().collect();
        assert!(sets.is_empty());
    }

    #[test]
    fn test_iter_base() {
        let base = Zdd::base();
        let sets: Vec<Vec<u32>> = base.iter().collect();
        assert_eq!(sets.len(), 1);
        assert!(sets[0].is_empty());
    }

    #[test]
    fn test_iter_singleton() {
        let zdd = Zdd::singleton(5);
        let sets: Vec<Vec<u32>> = zdd.iter().collect();
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0], vec![5]);
    }

    #[test]
    fn test_iter_from_set() {
        let zdd = Zdd::from_set(&[1, 3, 5]);
        let sets: Vec<Vec<u32>> = zdd.iter().collect();
        assert_eq!(sets.len(), 1);
        assert_eq!(sets[0], vec![1, 3, 5]);
    }

    #[test]
    fn test_iter_product_with_optional() {
        let zdd = Zdd::base()
            .product_with_optional(0)
            .product_with_optional(1)
            .product_with_optional(2);

        let sets: Vec<Vec<u32>> = zdd.iter().collect();
        assert_eq!(sets.len(), 8);

        // Convert to HashSet of BTreeSet for easy comparison
        let set_of_sets: HashSet<Vec<u32>> = sets.into_iter().collect();

        assert!(set_of_sets.contains(&vec![]));
        assert!(set_of_sets.contains(&vec![0]));
        assert!(set_of_sets.contains(&vec![1]));
        assert!(set_of_sets.contains(&vec![2]));
        assert!(set_of_sets.contains(&vec![0, 1]));
        assert!(set_of_sets.contains(&vec![0, 2]));
        assert!(set_of_sets.contains(&vec![1, 2]));
        assert!(set_of_sets.contains(&vec![0, 1, 2]));
    }

    #[test]
    fn test_iter_union() {
        let a = Zdd::from_set(&[1, 2]);
        let b = Zdd::from_set(&[3, 4]);
        let zdd = a.union(&b);

        let sets: HashSet<Vec<u32>> = zdd.iter().collect();
        assert_eq!(sets.len(), 2);
        assert!(sets.contains(&vec![1, 2]));
        assert!(sets.contains(&vec![3, 4]));
    }

    #[test]
    fn test_iter_count_consistency() {
        let zdd = Zdd::base()
            .product_with_optional(0)
            .product_with_optional(1)
            .product_with_optional(2)
            .product_with_optional(3);

        assert_eq!(zdd.iter().count(), zdd.count());
    }

    #[test]
    fn test_to_sets() {
        let zdd = Zdd::singleton(1).union(&Zdd::singleton(2));
        let sets = zdd.to_sets();

        assert_eq!(sets.len(), 2);
    }

    #[test]
    fn test_iter_large() {
        // Test with larger ZDD to ensure stack doesn't overflow
        let mut zdd = Zdd::base();
        for i in 0..15 {
            zdd = zdd.product_with_optional(i);
        }

        // 2^15 = 32768 sets
        assert_eq!(zdd.count(), 32768);

        // Just iterate a few to make sure it works
        let first_100: Vec<Vec<u32>> = zdd.iter().take(100).collect();
        assert_eq!(first_100.len(), 100);
    }
}
