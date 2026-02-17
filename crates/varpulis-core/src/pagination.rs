//! Pagination utilities for API list endpoints.

use serde::{Deserialize, Serialize};

/// Default number of items per page.
pub const DEFAULT_LIMIT: usize = 50;

/// Maximum allowed items per page.
pub const MAX_LIMIT: usize = 1000;

/// Query parameters for paginated list endpoints.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PaginationParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Pagination metadata included in responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationMeta {
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
    pub has_more: bool,
}

impl PaginationParams {
    /// Resolve the effective limit, clamped to MAX_LIMIT.
    pub fn effective_limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT)
    }

    /// Resolve the effective offset.
    pub fn effective_offset(&self) -> usize {
        self.offset.unwrap_or(0)
    }

    /// Returns true if the requested limit exceeds MAX_LIMIT.
    pub fn exceeds_max(&self) -> bool {
        matches!(self.limit, Some(l) if l > MAX_LIMIT)
    }

    /// Apply pagination to a collected vector, returning the page slice and metadata.
    pub fn paginate<T>(&self, items: Vec<T>) -> (Vec<T>, PaginationMeta) {
        let total = items.len();
        let limit = self.effective_limit();
        let offset = self.effective_offset();

        let page: Vec<T> = items.into_iter().skip(offset).take(limit).collect();
        let has_more = offset + limit < total;

        let meta = PaginationMeta {
            total,
            limit,
            offset,
            has_more,
        };
        (page, meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_pagination() {
        let params = PaginationParams::default();
        assert_eq!(params.effective_limit(), DEFAULT_LIMIT);
        assert_eq!(params.effective_offset(), 0);
        assert!(!params.exceeds_max());
    }

    #[test]
    fn test_custom_pagination() {
        let params = PaginationParams {
            limit: Some(10),
            offset: Some(20),
        };
        assert_eq!(params.effective_limit(), 10);
        assert_eq!(params.effective_offset(), 20);
    }

    #[test]
    fn test_exceeds_max() {
        let params = PaginationParams {
            limit: Some(1001),
            offset: None,
        };
        assert!(params.exceeds_max());
        // effective_limit is clamped
        assert_eq!(params.effective_limit(), MAX_LIMIT);
    }

    #[test]
    fn test_paginate_first_page() {
        let params = PaginationParams {
            limit: Some(2),
            offset: Some(0),
        };
        let items = vec![1, 2, 3, 4, 5];
        let (page, meta) = params.paginate(items);
        assert_eq!(page, vec![1, 2]);
        assert_eq!(meta.total, 5);
        assert_eq!(meta.limit, 2);
        assert_eq!(meta.offset, 0);
        assert!(meta.has_more);
    }

    #[test]
    fn test_paginate_last_page() {
        let params = PaginationParams {
            limit: Some(2),
            offset: Some(4),
        };
        let items = vec![1, 2, 3, 4, 5];
        let (page, meta) = params.paginate(items);
        assert_eq!(page, vec![5]);
        assert_eq!(meta.total, 5);
        assert!(!meta.has_more);
    }

    #[test]
    fn test_paginate_empty() {
        let params = PaginationParams::default();
        let items: Vec<i32> = vec![];
        let (page, meta) = params.paginate(items);
        assert!(page.is_empty());
        assert_eq!(meta.total, 0);
        assert!(!meta.has_more);
    }

    #[test]
    fn test_paginate_beyond_range() {
        let params = PaginationParams {
            limit: Some(10),
            offset: Some(100),
        };
        let items = vec![1, 2, 3];
        let (page, meta) = params.paginate(items);
        assert!(page.is_empty());
        assert_eq!(meta.total, 3);
        assert!(!meta.has_more);
    }
}
