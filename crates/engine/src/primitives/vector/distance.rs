//! Shared distance functions for vector similarity computation.
//!
//! These functions are used by both BruteForceBackend and HnswBackend.
//!
//! All scores are normalized to "higher = more similar" (Invariant R2).
//! Functions are single-threaded for determinism (Invariant R8).
//! No implicit normalization of vectors (Invariant R9).
//!
//! ## SIMD Acceleration
//!
//! - **aarch64** (Apple Silicon): NEON intrinsics (always available, no runtime check).
//! - **x86_64**: AVX2 with runtime `is_x86_feature_detected!` fallback to scalar.
//! - **Other targets**: Scalar fallback.

use crate::primitives::vector::DistanceMetric;

// ============================================================================
// Prefetch hint (hides DRAM latency for embedding fetches)
// ============================================================================

/// Software prefetch hint: bring the cache line containing `ptr` into L1.
///
/// Used in the HNSW inner loop to prefetch the *next* neighbor's embedding
/// while the CPU is computing distance for the current neighbor (hnswlib pattern).
/// On unsupported architectures this is a no-op.
#[inline(always)]
pub(crate) fn prefetch_read(ptr: *const u8) {
    #[cfg(target_arch = "aarch64")]
    unsafe {
        // PRFM PLDL1KEEP: prefetch for read, L1 cache, keep in cache
        core::arch::asm!(
            "prfm pldl1keep, [{x}]",
            x = in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
    #[cfg(target_arch = "x86_64")]
    unsafe {
        // PREFETCHT0: prefetch into all cache levels
        core::arch::asm!(
            "prefetcht0 ({})",
            in(reg) ptr,
            options(nostack, preserves_flags)
        );
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        let _ = ptr; // no-op on other architectures
    }
}

/// Compute similarity using pre-cached norms when available.
///
/// For Cosine metric with both norms present, avoids recomputing them
/// (saves ~13% per call). Falls back to `compute_similarity` otherwise.
#[inline]
pub fn compute_similarity_cached(
    a: &[f32],
    b: &[f32],
    metric: DistanceMetric,
    norm_a: Option<f32>,
    norm_b: Option<f32>,
) -> f32 {
    if metric == DistanceMetric::Cosine {
        if let (Some(na), Some(nb)) = (norm_a, norm_b) {
            return cosine_similarity_with_norms(a, b, na, nb);
        }
    }
    compute_similarity(a, b, metric)
}

/// Compute similarity score between two vectors
///
/// All scores are normalized to "higher = more similar" (Invariant R2).
/// This function is single-threaded for determinism (Invariant R8).
///
/// IMPORTANT: No implicit normalization of vectors (Invariant R9).
/// Vectors are used as-is.
#[inline]
pub fn compute_similarity(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    debug_assert_eq!(
        a.len(),
        b.len(),
        "Dimension mismatch in similarity computation"
    );

    match metric {
        DistanceMetric::Cosine => cosine_similarity(a, b),
        DistanceMetric::Euclidean => euclidean_similarity(a, b),
        DistanceMetric::DotProduct => dot_product(a, b),
    }
}

// ============================================================================
// Cosine similarity
// ============================================================================

/// Cosine similarity: dot(a,b) / (||a|| * ||b||)
///
/// Range: [-1, 1], higher = more similar
/// Returns 0.0 if either vector has zero norm (avoids division by zero)
#[inline]
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let (dot, norm_a_sq, norm_b_sq) = dot_norms(a, b);
    let denom = (norm_a_sq * norm_b_sq).sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

/// Compute cosine similarity using pre-computed norms.
///
/// Avoids recomputing ||a|| and ||b|| when norms are cached in VectorHeap.
#[inline]
pub fn cosine_similarity_with_norms(a: &[f32], b: &[f32], norm_a: f32, norm_b: f32) -> f32 {
    let denom = norm_a * norm_b;
    if denom == 0.0 {
        return 0.0;
    }
    dot_product(a, b) / denom
}

/// Single-pass dot product + both squared norms.
#[inline]
fn dot_norms(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    #[cfg(target_arch = "aarch64")]
    {
        dot_norms_neon(a, b)
    }
    #[cfg(target_arch = "x86_64")]
    {
        dot_norms_x86(a, b)
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        dot_norms_scalar(a, b)
    }
}

#[inline]
#[allow(dead_code)]
fn dot_norms_scalar(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (ai, bi) in a.iter().zip(b.iter()) {
        dot += ai * bi;
        na += ai * ai;
        nb += bi * bi;
    }
    (dot, na, nb)
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn dot_norms_neon(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    use std::arch::aarch64::*;
    unsafe {
        let mut vdot = vdupq_n_f32(0.0);
        let mut vna = vdupq_n_f32(0.0);
        let mut vnb = vdupq_n_f32(0.0);
        let chunks = a.len() / 4;
        for i in 0..chunks {
            let va = vld1q_f32(a.as_ptr().add(i * 4));
            let vb = vld1q_f32(b.as_ptr().add(i * 4));
            vdot = vfmaq_f32(vdot, va, vb);
            vna = vfmaq_f32(vna, va, va);
            vnb = vfmaq_f32(vnb, vb, vb);
        }
        let mut sd = vaddvq_f32(vdot);
        let mut sna = vaddvq_f32(vna);
        let mut snb = vaddvq_f32(vnb);
        // Scalar remainder for non-multiple-of-4 dimensions
        for i in (chunks * 4)..a.len() {
            sd += a[i] * b[i];
            sna += a[i] * a[i];
            snb += b[i] * b[i];
        }
        (sd, sna, snb)
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn dot_norms_x86(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        // SAFETY: we just checked the feature is available
        unsafe { dot_norms_avx2(a, b) }
    } else {
        dot_norms_scalar(a, b)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_norms_avx2(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    use std::arch::x86_64::*;
    let mut vdot = _mm256_setzero_ps();
    let mut vna = _mm256_setzero_ps();
    let mut vnb = _mm256_setzero_ps();
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let va = _mm256_loadu_ps(a.as_ptr().add(i * 8));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));
        vdot = _mm256_fmadd_ps(va, vb, vdot);
        vna = _mm256_fmadd_ps(va, va, vna);
        vnb = _mm256_fmadd_ps(vb, vb, vnb);
    }
    // Horizontal sum of 8-wide vectors
    let hsum = |v: __m256| -> f32 {
        let hi = _mm256_extractf128_ps(v, 1);
        let lo = _mm256_castps256_ps128(v);
        let sum128 = _mm_add_ps(lo, hi);
        let hi64 = _mm_movehl_ps(sum128, sum128);
        let sum64 = _mm_add_ps(sum128, hi64);
        let hi32 = _mm_shuffle_ps(sum64, sum64, 0x1);
        let sum32 = _mm_add_ss(sum64, hi32);
        _mm_cvtss_f32(sum32)
    };
    let mut sd = hsum(vdot);
    let mut sna = hsum(vna);
    let mut snb = hsum(vnb);
    // Scalar remainder
    for i in (chunks * 8)..a.len() {
        sd += a[i] * b[i];
        sna += a[i] * a[i];
        snb += b[i] * b[i];
    }
    (sd, sna, snb)
}

// ============================================================================
// Euclidean similarity
// ============================================================================

/// Euclidean similarity: 1 / (1 + l2_distance)
///
/// Range: (0, 1], higher = more similar
/// Transforms distance to similarity (inversely related)
#[inline]
fn euclidean_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dist = euclidean_distance(a, b);
    1.0 / (1.0 + dist)
}

/// Euclidean distance (L2 distance)
#[inline]
fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        euclidean_distance_neon(a, b)
    }
    #[cfg(target_arch = "x86_64")]
    {
        euclidean_distance_x86(a, b)
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        euclidean_distance_scalar(a, b)
    }
}

#[inline]
#[allow(dead_code)]
fn euclidean_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum::<f32>()
        .sqrt()
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn euclidean_distance_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;
    unsafe {
        let mut vsum = vdupq_n_f32(0.0);
        let chunks = a.len() / 4;
        for i in 0..chunks {
            let va = vld1q_f32(a.as_ptr().add(i * 4));
            let vb = vld1q_f32(b.as_ptr().add(i * 4));
            let diff = vsubq_f32(va, vb);
            vsum = vfmaq_f32(vsum, diff, diff);
        }
        let mut s = vaddvq_f32(vsum);
        for i in (chunks * 4)..a.len() {
            let d = a[i] - b[i];
            s += d * d;
        }
        s.sqrt()
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn euclidean_distance_x86(a: &[f32], b: &[f32]) -> f32 {
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        unsafe { euclidean_distance_avx2(a, b) }
    } else {
        euclidean_distance_scalar(a, b)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn euclidean_distance_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    let mut vsum = _mm256_setzero_ps();
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let va = _mm256_loadu_ps(a.as_ptr().add(i * 8));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));
        let diff = _mm256_sub_ps(va, vb);
        vsum = _mm256_fmadd_ps(diff, diff, vsum);
    }
    // Horizontal sum
    let hi = _mm256_extractf128_ps(vsum, 1);
    let lo = _mm256_castps256_ps128(vsum);
    let sum128 = _mm_add_ps(lo, hi);
    let hi64 = _mm_movehl_ps(sum128, sum128);
    let sum64 = _mm_add_ps(sum128, hi64);
    let hi32 = _mm_shuffle_ps(sum64, sum64, 0x1);
    let sum32 = _mm_add_ss(sum64, hi32);
    let mut s = _mm_cvtss_f32(sum32);
    for i in (chunks * 8)..a.len() {
        let d = a[i] - b[i];
        s += d * d;
    }
    s.sqrt()
}

// ============================================================================
// Dot product
// ============================================================================

/// Dot product (inner product)
///
/// Range: unbounded, higher = more similar
/// Assumes vectors are pre-normalized for meaningful comparison
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "aarch64")]
    {
        dot_product_neon(a, b)
    }
    #[cfg(target_arch = "x86_64")]
    {
        dot_product_x86(a, b)
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    {
        dot_product_scalar(a, b)
    }
}

#[inline]
#[allow(dead_code)]
fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn dot_product_neon(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::aarch64::*;
    unsafe {
        let mut vdot = vdupq_n_f32(0.0);
        let chunks = a.len() / 4;
        for i in 0..chunks {
            let va = vld1q_f32(a.as_ptr().add(i * 4));
            let vb = vld1q_f32(b.as_ptr().add(i * 4));
            vdot = vfmaq_f32(vdot, va, vb);
        }
        let mut s = vaddvq_f32(vdot);
        for i in (chunks * 4)..a.len() {
            s += a[i] * b[i];
        }
        s
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn dot_product_x86(a: &[f32], b: &[f32]) -> f32 {
    if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        unsafe { dot_product_avx2(a, b) }
    } else {
        dot_product_scalar(a, b)
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_product_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    let mut vdot = _mm256_setzero_ps();
    let chunks = a.len() / 8;
    for i in 0..chunks {
        let va = _mm256_loadu_ps(a.as_ptr().add(i * 8));
        let vb = _mm256_loadu_ps(b.as_ptr().add(i * 8));
        vdot = _mm256_fmadd_ps(va, vb, vdot);
    }
    let hi = _mm256_extractf128_ps(vdot, 1);
    let lo = _mm256_castps256_ps128(vdot);
    let sum128 = _mm_add_ps(lo, hi);
    let hi64 = _mm_movehl_ps(sum128, sum128);
    let sum64 = _mm_add_ps(sum128, hi64);
    let hi32 = _mm_shuffle_ps(sum64, sum64, 0x1);
    let sum32 = _mm_add_ss(sum64, hi32);
    let mut s = _mm_cvtss_f32(sum32);
    for i in (chunks * 8)..a.len() {
        s += a[i] * b[i];
    }
    s
}

// ============================================================================
// L2 norm (for heap norms cache)
// ============================================================================

/// L2 norm (Euclidean length)
#[allow(dead_code)]
#[inline]
fn l2_norm(v: &[f32]) -> f32 {
    v.iter().map(|x| x * x).sum::<f32>().sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_identical_vectors() {
        let v = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&v, &v);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_opposite_vectors() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![-1.0, 0.0];
        let sim = cosine_similarity(&v1, &v2);
        assert!((sim - (-1.0)).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_orthogonal_vectors() {
        let v1 = vec![1.0, 0.0];
        let v2 = vec![0.0, 1.0];
        let sim = cosine_similarity(&v1, &v2);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_identical_vectors() {
        let v = vec![1.0, 2.0, 3.0];
        let sim = euclidean_similarity(&v, &v);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_distant_vectors() {
        let v1 = vec![0.0, 0.0];
        let v2 = vec![100.0, 0.0];
        let sim = euclidean_similarity(&v1, &v2);
        assert!(sim < 0.01);
        assert!(sim > 0.0);
        assert!(sim <= 1.0);
    }

    #[test]
    fn test_dot_product_unit_vectors() {
        let v = vec![1.0, 0.0];
        assert!((dot_product(&v, &v) - 1.0).abs() < 1e-6);

        let v1 = vec![1.0, 0.0];
        let v2 = vec![0.0, 1.0];
        assert!(dot_product(&v1, &v2).abs() < 1e-6);
    }

    #[test]
    fn test_zero_vector_handling() {
        let zero = vec![0.0, 0.0, 0.0];
        let nonzero = vec![1.0, 2.0, 3.0];

        assert_eq!(cosine_similarity(&zero, &nonzero), 0.0);
        assert_eq!(cosine_similarity(&nonzero, &zero), 0.0);
        assert_eq!(cosine_similarity(&zero, &zero), 0.0);

        let sim = euclidean_similarity(&zero, &nonzero);
        assert!(sim > 0.0 && sim <= 1.0);
    }

    #[test]
    fn test_compute_similarity_dispatches_correctly() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];

        let cosine = compute_similarity(&a, &b, DistanceMetric::Cosine);
        assert!(cosine.abs() < 1e-6); // Orthogonal

        let euclidean = compute_similarity(&a, &b, DistanceMetric::Euclidean);
        assert!(euclidean > 0.0 && euclidean < 1.0);

        let dot = compute_similarity(&a, &b, DistanceMetric::DotProduct);
        assert!(dot.abs() < 1e-6); // Orthogonal
    }

    #[test]
    fn test_simd_matches_scalar_128d() {
        // Verify SIMD produces same results as scalar for typical dimensions
        let a: Vec<f32> = (0..128).map(|i| (i as f32 / 100.0).sin()).collect();
        let b: Vec<f32> = (0..128).map(|i| (i as f32 / 50.0).cos()).collect();

        let (dot, na, nb) = dot_norms_scalar(&a, &b);
        let (dot2, na2, nb2) = dot_norms(&a, &b);

        assert!(
            (dot - dot2).abs() < 1e-4,
            "dot mismatch: {} vs {}",
            dot,
            dot2
        );
        assert!(
            (na - na2).abs() < 1e-4,
            "norm_a mismatch: {} vs {}",
            na,
            na2
        );
        assert!(
            (nb - nb2).abs() < 1e-4,
            "norm_b mismatch: {} vs {}",
            nb,
            nb2
        );

        let ed_scalar = euclidean_distance_scalar(&a, &b);
        let ed_simd = euclidean_distance(&a, &b);
        assert!(
            (ed_scalar - ed_simd).abs() < 1e-4,
            "euclidean mismatch: {} vs {}",
            ed_scalar,
            ed_simd
        );

        let dp_scalar = dot_product_scalar(&a, &b);
        let dp_simd = dot_product(&a, &b);
        assert!(
            (dp_scalar - dp_simd).abs() < 1e-4,
            "dot_product mismatch: {} vs {}",
            dp_scalar,
            dp_simd
        );
    }

    #[test]
    fn test_simd_odd_dimensions() {
        // Verify remainder handling for non-multiple-of-4/8 dimensions
        for dim in [1, 3, 5, 7, 13, 33, 127, 129] {
            let a: Vec<f32> = (0..dim).map(|i| (i as f32 / 100.0).sin()).collect();
            let b: Vec<f32> = (0..dim).map(|i| (i as f32 / 50.0).cos()).collect();

            let scalar = cosine_similarity(&a, &b);
            let (dot, na, nb) = dot_norms_scalar(&a, &b);
            let expected = if (na * nb).sqrt() == 0.0 {
                0.0
            } else {
                dot / (na * nb).sqrt()
            };
            assert!(
                (scalar - expected).abs() < 1e-5,
                "dim={}: cosine mismatch: {} vs {}",
                dim,
                scalar,
                expected
            );
        }
    }

    #[test]
    fn test_cosine_with_norms() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        let sim1 = cosine_similarity(&a, &b);
        let sim2 = cosine_similarity_with_norms(&a, &b, norm_a, norm_b);
        assert!((sim1 - sim2).abs() < 1e-6);
    }

    #[test]
    fn test_compute_similarity_cached_cosine_matches() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let norm_a = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b = b.iter().map(|x| x * x).sum::<f32>().sqrt();

        // With both norms: should use cached path
        let cached =
            compute_similarity_cached(&a, &b, DistanceMetric::Cosine, Some(norm_a), Some(norm_b));
        let direct = compute_similarity(&a, &b, DistanceMetric::Cosine);
        assert!((cached - direct).abs() < 1e-6);

        // With one norm missing: should fall back to compute_similarity
        let partial = compute_similarity_cached(&a, &b, DistanceMetric::Cosine, Some(norm_a), None);
        assert!((partial - direct).abs() < 1e-6);
        let partial2 =
            compute_similarity_cached(&a, &b, DistanceMetric::Cosine, None, Some(norm_b));
        assert!((partial2 - direct).abs() < 1e-6);
    }

    #[test]
    fn test_compute_similarity_cached_non_cosine_ignores_norms() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];

        // Euclidean: norms should be ignored
        let euclidean_cached =
            compute_similarity_cached(&a, &b, DistanceMetric::Euclidean, Some(999.0), Some(999.0));
        let euclidean_direct = compute_similarity(&a, &b, DistanceMetric::Euclidean);
        assert!((euclidean_cached - euclidean_direct).abs() < 1e-6);

        // DotProduct: norms should be ignored
        let dot_cached =
            compute_similarity_cached(&a, &b, DistanceMetric::DotProduct, Some(999.0), Some(999.0));
        let dot_direct = compute_similarity(&a, &b, DistanceMetric::DotProduct);
        assert!((dot_cached - dot_direct).abs() < 1e-6);
    }

    #[test]
    fn test_compute_similarity_cached_zero_norms() {
        let zero = vec![0.0, 0.0, 0.0];
        let nonzero = vec![1.0, 2.0, 3.0];

        // Zero norm with cached path should return 0.0 (same as uncached)
        let cached = compute_similarity_cached(
            &zero,
            &nonzero,
            DistanceMetric::Cosine,
            Some(0.0),
            Some(3.742),
        );
        assert_eq!(cached, 0.0);

        let direct = compute_similarity(&zero, &nonzero, DistanceMetric::Cosine);
        assert_eq!(direct, 0.0);
    }
}

#[cfg(test)]
mod profiling_tests {
    use super::*;
    use std::time::Instant;

    fn make_embedding(dim: usize, seed: usize) -> Vec<f32> {
        (0..dim)
            .map(|j| ((seed * dim + j) as f32 / 1000.0).sin())
            .collect()
    }

    /// Test 4: SIMD Activation Verification
    ///
    /// Times 100K distance computations via scalar vs SIMD-dispatched paths
    /// to confirm SIMD is actually activating and providing speedup.
    #[test]
    fn profile_simd_vs_scalar() {
        let dim = 128;
        let num_pairs = 100_000;

        // Pre-generate vector pairs
        let pairs: Vec<(Vec<f32>, Vec<f32>)> = (0..num_pairs)
            .map(|i| (make_embedding(dim, i), make_embedding(dim, i + num_pairs)))
            .collect();

        // --- dot_norms: scalar ---
        let start = Instant::now();
        let mut checksum_scalar = 0.0f64;
        for (a, b) in &pairs {
            let (dot, na, nb) = dot_norms_scalar(a, b);
            checksum_scalar += dot as f64 + na as f64 + nb as f64;
        }
        let scalar_dot_ns = start.elapsed().as_nanos() as f64 / num_pairs as f64;

        // --- dot_norms: SIMD-dispatched ---
        let start = Instant::now();
        let mut checksum_simd = 0.0f64;
        for (a, b) in &pairs {
            let (dot, na, nb) = dot_norms(a, b);
            checksum_simd += dot as f64 + na as f64 + nb as f64;
        }
        let simd_dot_ns = start.elapsed().as_nanos() as f64 / num_pairs as f64;

        // --- euclidean_distance: scalar ---
        let start = Instant::now();
        let mut checksum_eu_scalar = 0.0f64;
        for (a, b) in &pairs {
            checksum_eu_scalar += euclidean_distance_scalar(a, b) as f64;
        }
        let scalar_eu_ns = start.elapsed().as_nanos() as f64 / num_pairs as f64;

        // --- euclidean_distance: SIMD-dispatched ---
        let start = Instant::now();
        let mut checksum_eu_simd = 0.0f64;
        for (a, b) in &pairs {
            checksum_eu_simd += euclidean_distance(a, b) as f64;
        }
        let simd_eu_ns = start.elapsed().as_nanos() as f64 / num_pairs as f64;

        // Detect platform
        let platform = if cfg!(target_arch = "aarch64") {
            "aarch64/NEON"
        } else if cfg!(target_arch = "x86_64") {
            "x86_64 (AVX2 detection at runtime)"
        } else {
            "scalar"
        };

        let dot_speedup = scalar_dot_ns / simd_dot_ns;
        let eu_speedup = scalar_eu_ns / simd_eu_ns;

        println!("\n=== SIMD vs Scalar Throughput ({num_pairs} pairs, dim={dim}) ===");
        println!("  Platform: {platform}");
        println!("  dot_norms_scalar:            {scalar_dot_ns:.1} ns/pair");
        println!("  dot_norms (SIMD-dispatched):  {simd_dot_ns:.1} ns/pair");
        println!("  dot_norms speedup: {dot_speedup:.2}x");
        println!();
        println!("  euclidean_distance_scalar:            {scalar_eu_ns:.1} ns/pair");
        println!("  euclidean_distance (SIMD-dispatched):  {simd_eu_ns:.1} ns/pair");
        println!("  euclidean_distance speedup: {eu_speedup:.2}x");
        println!();
        println!("  checksums (dot): scalar={checksum_scalar:.2}, simd={checksum_simd:.2}");
        println!("  checksums (eu):  scalar={checksum_eu_scalar:.2}, simd={checksum_eu_simd:.2}");

        if platform != "scalar" && platform != "x86_64/scalar-fallback" && dot_speedup < 1.5 {
            println!(
                "  WARNING: SIMD dot_norms speedup < 1.5x — SIMD may not be activating properly"
            );
        }
        if platform != "scalar" && platform != "x86_64/scalar-fallback" && eu_speedup < 1.5 {
            println!(
                "  WARNING: SIMD euclidean speedup < 1.5x — SIMD may not be activating properly"
            );
        }
    }
}
