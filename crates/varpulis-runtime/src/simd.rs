//! SIMD-optimized operations for high-performance event processing
//!
//! This module provides vectorized implementations of common operations:
//! - Aggregations (sum, min, max, avg)
//! - Batch comparisons for filtering
//! - Field extraction to contiguous arrays

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

// =============================================================================
// SIMD Aggregations (f64)
// =============================================================================

/// SIMD-optimized sum of f64 values
/// Uses AVX2 (256-bit) when available, falls back to scalar
#[inline]
pub fn sum_f64(values: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We checked for AVX2 support
            unsafe { sum_f64_avx2(values) }
        } else {
            sum_f64_scalar(values)
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        sum_f64_scalar(values)
    }
}

#[inline]
fn sum_f64_scalar(values: &[f64]) -> f64 {
    // Use 4-way unrolling for better ILP
    let mut sum0 = 0.0;
    let mut sum1 = 0.0;
    let mut sum2 = 0.0;
    let mut sum3 = 0.0;

    let chunks = values.len() / 4;
    let remainder = values.len() % 4;

    for i in 0..chunks {
        let base = i * 4;
        // SAFETY: Loop bounds guarantee base + 3 < chunks * 4 <= values.len().
        // chunks = values.len() / 4, so base + 3 = i*4 + 3 < chunks*4 = (len/4)*4 <= len.
        // All indices are within bounds.
        unsafe {
            sum0 += *values.get_unchecked(base);
            sum1 += *values.get_unchecked(base + 1);
            sum2 += *values.get_unchecked(base + 2);
            sum3 += *values.get_unchecked(base + 3);
        }
    }

    // Handle remainder
    let base = chunks * 4;
    for i in 0..remainder {
        // SAFETY: remainder = len % 4, so base + i = chunks*4 + i where i < remainder.
        // Thus base + i < chunks*4 + remainder = chunks*4 + (len % 4) = len.
        // All indices are within bounds.
        unsafe {
            sum0 += *values.get_unchecked(base + i);
        }
    }

    sum0 + sum1 + sum2 + sum3
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
/// SAFETY: Caller must ensure AVX2 is available (checked via is_x86_feature_detected!).
/// This function uses AVX2 intrinsics for 4-way parallel f64 summation.
unsafe fn sum_f64_avx2(values: &[f64]) -> f64 {
    let mut sum_vec = _mm256_setzero_pd();
    let chunks = values.len() / 4;

    for i in 0..chunks {
        // SAFETY: i * 4 + 3 < chunks * 4 <= values.len(), so pointer arithmetic is in bounds.
        // _mm256_loadu_pd handles unaligned loads safely.
        let ptr = values.as_ptr().add(i * 4);
        let v = _mm256_loadu_pd(ptr);
        sum_vec = _mm256_add_pd(sum_vec, v);
    }

    // Horizontal sum of the vector
    let mut result = [0.0f64; 4];
    // SAFETY: result is a 4-element array, correctly sized for 256-bit store.
    _mm256_storeu_pd(result.as_mut_ptr(), sum_vec);
    let mut total = result[0] + result[1] + result[2] + result[3];

    // Handle remainder
    let base = chunks * 4;
    for i in base..values.len() {
        // SAFETY: Loop range is [base, len), so i is always in bounds.
        total += *values.get_unchecked(i);
    }

    total
}

/// SIMD-optimized min of f64 values
#[inline]
pub fn min_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We checked for AVX2 support and non-empty
            unsafe { Some(min_f64_avx2(values)) }
        } else {
            Some(min_f64_scalar(values))
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        Some(min_f64_scalar(values))
    }
}

#[inline]
fn min_f64_scalar(values: &[f64]) -> f64 {
    let mut min = f64::INFINITY;
    for &v in values {
        if v < min {
            min = v;
        }
    }
    min
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
/// SAFETY: Caller must ensure AVX2 is available and values is non-empty.
/// Uses AVX2 intrinsics for 4-way parallel minimum computation.
unsafe fn min_f64_avx2(values: &[f64]) -> f64 {
    let mut min_vec = _mm256_set1_pd(f64::INFINITY);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        // SAFETY: i * 4 + 3 < chunks * 4 <= values.len(), pointer arithmetic in bounds.
        let ptr = values.as_ptr().add(i * 4);
        let v = _mm256_loadu_pd(ptr);
        min_vec = _mm256_min_pd(min_vec, v);
    }

    // Horizontal min
    let mut result = [0.0f64; 4];
    // SAFETY: result is correctly sized for 256-bit store.
    _mm256_storeu_pd(result.as_mut_ptr(), min_vec);
    let mut min = result[0].min(result[1]).min(result[2]).min(result[3]);

    // Handle remainder
    let base = chunks * 4;
    for i in base..values.len() {
        // SAFETY: Loop range [base, len) ensures i is in bounds.
        let v = *values.get_unchecked(i);
        if v < min {
            min = v;
        }
    }

    min
}

/// SIMD-optimized max of f64 values
#[inline]
pub fn max_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We checked for AVX2 support and non-empty
            unsafe { Some(max_f64_avx2(values)) }
        } else {
            Some(max_f64_scalar(values))
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        Some(max_f64_scalar(values))
    }
}

#[inline]
fn max_f64_scalar(values: &[f64]) -> f64 {
    let mut max = f64::NEG_INFINITY;
    for &v in values {
        if v > max {
            max = v;
        }
    }
    max
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
/// SAFETY: Caller must ensure AVX2 is available and values is non-empty.
/// Uses AVX2 intrinsics for 4-way parallel maximum computation.
unsafe fn max_f64_avx2(values: &[f64]) -> f64 {
    let mut max_vec = _mm256_set1_pd(f64::NEG_INFINITY);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        // SAFETY: i * 4 + 3 < chunks * 4 <= values.len(), pointer arithmetic in bounds.
        let ptr = values.as_ptr().add(i * 4);
        let v = _mm256_loadu_pd(ptr);
        max_vec = _mm256_max_pd(max_vec, v);
    }

    // Horizontal max
    let mut result = [0.0f64; 4];
    // SAFETY: result is correctly sized for 256-bit store.
    _mm256_storeu_pd(result.as_mut_ptr(), max_vec);
    let mut max = result[0].max(result[1]).max(result[2]).max(result[3]);

    // Handle remainder
    let base = chunks * 4;
    for i in base..values.len() {
        // SAFETY: Loop range [base, len) ensures i is in bounds.
        let v = *values.get_unchecked(i);
        if v > max {
            max = v;
        }
    }

    max
}

// =============================================================================
// SIMD Batch Comparisons
// =============================================================================

/// SIMD-optimized greater-than comparison
/// Returns a bitmask where bit `i` is set if `values[i] > threshold`
#[inline]
pub fn compare_gt_f64(values: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = vec![false; values.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We checked for AVX2 support
            unsafe { compare_gt_f64_avx2(values, threshold, &mut result) };
            return result;
        }
    }

    // Scalar fallback
    for (i, &v) in values.iter().enumerate() {
        result[i] = v > threshold;
    }
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
/// SAFETY: Caller must ensure AVX2 is available.
/// result must have length >= values.len().
/// Uses AVX2 intrinsics for 4-way parallel greater-than comparison.
unsafe fn compare_gt_f64_avx2(values: &[f64], threshold: f64, result: &mut [bool]) {
    let thresh_vec = _mm256_set1_pd(threshold);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        // SAFETY: i * 4 + 3 < chunks * 4 <= values.len(), pointer in bounds.
        let ptr = values.as_ptr().add(i * 4);
        let v = _mm256_loadu_pd(ptr);
        let cmp = _mm256_cmp_pd(v, thresh_vec, _CMP_GT_OQ);
        let mask = _mm256_movemask_pd(cmp);

        let base = i * 4;
        // SAFETY: base + 3 < chunks * 4 <= result.len() (result has same len as values).
        *result.get_unchecked_mut(base) = (mask & 1) != 0;
        *result.get_unchecked_mut(base + 1) = (mask & 2) != 0;
        *result.get_unchecked_mut(base + 2) = (mask & 4) != 0;
        *result.get_unchecked_mut(base + 3) = (mask & 8) != 0;
    }

    // Handle remainder
    let base = chunks * 4;
    for i in base..values.len() {
        // SAFETY: i is in [base, values.len()), both slices have same length.
        *result.get_unchecked_mut(i) = *values.get_unchecked(i) > threshold;
    }
}

/// SIMD-optimized less-than comparison
#[inline]
pub fn compare_lt_f64(values: &[f64], threshold: f64) -> Vec<bool> {
    let mut result = vec![false; values.len()];

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: We checked for AVX2 support
            unsafe { compare_lt_f64_avx2(values, threshold, &mut result) };
            return result;
        }
    }

    // Scalar fallback
    for (i, &v) in values.iter().enumerate() {
        result[i] = v < threshold;
    }
    result
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
/// SAFETY: Caller must ensure AVX2 is available.
/// result must have length >= values.len().
/// Uses AVX2 intrinsics for 4-way parallel less-than comparison.
unsafe fn compare_lt_f64_avx2(values: &[f64], threshold: f64, result: &mut [bool]) {
    let thresh_vec = _mm256_set1_pd(threshold);
    let chunks = values.len() / 4;

    for i in 0..chunks {
        // SAFETY: i * 4 + 3 < chunks * 4 <= values.len(), pointer in bounds.
        let ptr = values.as_ptr().add(i * 4);
        let v = _mm256_loadu_pd(ptr);
        let cmp = _mm256_cmp_pd(v, thresh_vec, _CMP_LT_OQ);
        let mask = _mm256_movemask_pd(cmp);

        let base = i * 4;
        // SAFETY: base + 3 < chunks * 4 <= result.len() (result has same len as values).
        *result.get_unchecked_mut(base) = (mask & 1) != 0;
        *result.get_unchecked_mut(base + 1) = (mask & 2) != 0;
        *result.get_unchecked_mut(base + 2) = (mask & 4) != 0;
        *result.get_unchecked_mut(base + 3) = (mask & 8) != 0;
    }

    // Handle remainder
    let base = chunks * 4;
    for i in base..values.len() {
        // SAFETY: i is in [base, values.len()), both slices have same length.
        *result.get_unchecked_mut(i) = *values.get_unchecked(i) < threshold;
    }
}

// =============================================================================
// Field Extraction
// =============================================================================

use crate::event::Event;

/// Extract float field values from events into a contiguous array
/// Returns None values as NaN for SIMD processing
#[inline]
pub fn extract_field_f64(events: &[Event], field: &str) -> Vec<f64> {
    let mut values = Vec::with_capacity(events.len());
    for event in events {
        values.push(event.get_float(field).unwrap_or(f64::NAN));
    }
    values
}

/// Extract float field values from events, filtering out None values
/// Returns (values, indices) where indices maps back to original positions
#[inline]
pub fn extract_field_f64_filtered(events: &[Event], field: &str) -> (Vec<f64>, Vec<usize>) {
    let mut values = Vec::with_capacity(events.len());
    let mut indices = Vec::with_capacity(events.len());

    for (i, event) in events.iter().enumerate() {
        if let Some(v) = event.get_float(field) {
            if !v.is_nan() {
                values.push(v);
                indices.push(i);
            }
        }
    }

    (values, indices)
}

// =============================================================================
// SIMD Aggregation Wrappers
// =============================================================================

/// Compute sum of a field across events using SIMD
pub fn simd_sum(events: &[Event], field: &str) -> f64 {
    let values = extract_field_f64(events, field);
    // Filter out NaN before summing
    let valid: Vec<f64> = values.into_iter().filter(|v| !v.is_nan()).collect();
    sum_f64(&valid)
}

/// Compute avg of a field across events using SIMD
pub fn simd_avg(events: &[Event], field: &str) -> Option<f64> {
    let values = extract_field_f64(events, field);
    let valid: Vec<f64> = values.into_iter().filter(|v| !v.is_nan()).collect();
    if valid.is_empty() {
        None
    } else {
        Some(sum_f64(&valid) / valid.len() as f64)
    }
}

/// Compute min of a field across events using SIMD
pub fn simd_min(events: &[Event], field: &str) -> Option<f64> {
    let (values, _) = extract_field_f64_filtered(events, field);
    min_f64(&values)
}

/// Compute max of a field across events using SIMD
pub fn simd_max(events: &[Event], field: &str) -> Option<f64> {
    let (values, _) = extract_field_f64_filtered(events, field);
    max_f64(&values)
}

// =============================================================================
// Incremental Aggregation
// =============================================================================

/// Incremental sum accumulator - O(1) updates instead of O(n) recomputation
#[derive(Debug, Clone, Default)]
pub struct IncrementalSum {
    sum: f64,
    count: usize,
}

impl IncrementalSum {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn add(&mut self, value: f64) {
        if !value.is_nan() {
            self.sum += value;
            self.count += 1;
        }
    }

    #[inline]
    pub fn remove(&mut self, value: f64) {
        if !value.is_nan() {
            self.sum -= value;
            self.count = self.count.saturating_sub(1);
        }
    }

    #[inline]
    pub fn sum(&self) -> f64 {
        self.sum
    }

    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn avg(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum / self.count as f64)
        }
    }

    pub fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }
}

/// Wrapper for f64 that implements Ord using total ordering.
/// NaN values sort after all other values for consistency.
#[derive(Debug, Clone, Copy)]
struct OrderedF64(f64);

impl PartialEq for OrderedF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == std::cmp::Ordering::Equal
    }
}

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Incremental min/max tracker using BTreeMap for O(log n) operations
#[derive(Debug, Clone)]
pub struct IncrementalMinMax {
    // BTreeMap provides O(log n) insert, remove, min, max
    // Value is count of duplicates for the same f64
    values: std::collections::BTreeMap<OrderedF64, usize>,
}

impl Default for IncrementalMinMax {
    fn default() -> Self {
        Self::new()
    }
}

impl IncrementalMinMax {
    pub fn new() -> Self {
        Self {
            values: std::collections::BTreeMap::new(),
        }
    }

    #[inline]
    pub fn add(&mut self, value: f64) {
        if !value.is_nan() {
            *self.values.entry(OrderedF64(value)).or_insert(0) += 1;
        }
    }

    #[inline]
    pub fn remove(&mut self, value: f64) {
        if !value.is_nan() {
            let key = OrderedF64(value);
            if let std::collections::btree_map::Entry::Occupied(mut entry) = self.values.entry(key)
            {
                let count = entry.get_mut();
                if *count > 1 {
                    *count -= 1;
                } else {
                    entry.remove();
                }
            }
        }
    }

    pub fn min(&mut self) -> Option<f64> {
        self.values.first_key_value().map(|(k, _)| k.0)
    }

    pub fn max(&mut self) -> Option<f64> {
        self.values.last_key_value().map(|(k, _)| k.0)
    }

    pub fn reset(&mut self) {
        self.values.clear();
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_f64_empty() {
        assert_eq!(sum_f64(&[]), 0.0);
    }

    #[test]
    fn test_sum_f64_single() {
        assert_eq!(sum_f64(&[42.0]), 42.0);
    }

    #[test]
    fn test_sum_f64_multiple() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();
        assert_eq!(sum_f64(&values), 5050.0);
    }

    #[test]
    fn test_sum_f64_large() {
        let values: Vec<f64> = (1..=10000).map(|x| x as f64).collect();
        let expected: f64 = (1..=10000).sum::<i64>() as f64;
        assert!((sum_f64(&values) - expected).abs() < 0.001);
    }

    #[test]
    fn test_min_f64() {
        assert_eq!(min_f64(&[3.0, 1.0, 4.0, 1.0, 5.0]), Some(1.0));
        assert_eq!(min_f64(&[]), None);
    }

    #[test]
    fn test_max_f64() {
        assert_eq!(max_f64(&[3.0, 1.0, 4.0, 1.0, 5.0]), Some(5.0));
        assert_eq!(max_f64(&[]), None);
    }

    #[test]
    fn test_compare_gt() {
        let values = vec![1.0, 5.0, 3.0, 7.0, 2.0];
        let result = compare_gt_f64(&values, 3.0);
        assert_eq!(result, vec![false, true, false, true, false]);
    }

    #[test]
    fn test_compare_lt() {
        let values = vec![1.0, 5.0, 3.0, 7.0, 2.0];
        let result = compare_lt_f64(&values, 3.0);
        assert_eq!(result, vec![true, false, false, false, true]);
    }

    #[test]
    fn test_incremental_sum() {
        let mut acc = IncrementalSum::new();
        acc.add(10.0);
        acc.add(20.0);
        acc.add(30.0);
        assert_eq!(acc.sum(), 60.0);
        assert_eq!(acc.count(), 3);
        assert_eq!(acc.avg(), Some(20.0));

        acc.remove(20.0);
        assert_eq!(acc.sum(), 40.0);
        assert_eq!(acc.count(), 2);
        assert_eq!(acc.avg(), Some(20.0));
    }

    #[test]
    fn test_incremental_minmax() {
        let mut tracker = IncrementalMinMax::new();
        tracker.add(5.0);
        tracker.add(3.0);
        tracker.add(7.0);
        tracker.add(1.0);

        assert_eq!(tracker.min(), Some(1.0));
        assert_eq!(tracker.max(), Some(7.0));

        tracker.remove(1.0);
        assert_eq!(tracker.min(), Some(3.0));

        tracker.remove(7.0);
        assert_eq!(tracker.max(), Some(5.0));
    }

    #[test]
    fn test_sum_scalar_vs_avx2() {
        // Test that scalar and AVX2 produce same results
        let values: Vec<f64> = (1..=1000).map(|x| x as f64).collect();
        let scalar = sum_f64_scalar(&values);

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                let avx2 = unsafe { sum_f64_avx2(&values) };
                assert!((scalar - avx2).abs() < 0.001);
            }
        }
    }
}
