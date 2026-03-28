//! Scalar Quantization (SQ8) for vector embeddings
//!
//! Quantizes f32 embeddings to u8 [0, 255] using per-dimension min/max calibration.
//! This provides 4x memory savings with ~1-2% recall loss.
//!
//! ## Quantization formula
//!
//! ```text
//! quantized[d] = clamp(round((value[d] - min[d]) / scale[d]), 0, 255) as u8
//! scale[d] = (max[d] - min[d]) / 255.0
//! ```
//!
//! ## Dequantization formula
//!
//! ```text
//! value[d] = quantized[d] as f32 * scale[d] + min[d]
//! ```
//!
//! ## Calibration
//!
//! Uses first-N strategy: track per-dimension min/max during the first N inserts
//! (default 256). After calibration, parameters are frozen and subsequent vectors
//! are quantized using the calibrated range (outliers clipped).

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::io;
use std::io::Cursor;

/// Default number of vectors to use for calibration.
pub const DEFAULT_CALIBRATION_THRESHOLD: usize = 256;

/// Per-dimension quantization parameters for SQ8.
///
/// Maps f32 values to u8 [0, 255] per dimension using linear scaling.
/// Stored alongside quantized data in heap and mmap files.
#[derive(Debug, Clone)]
pub struct QuantizationParams {
    /// Per-dimension minimum values (length = dimension)
    pub mins: Vec<f32>,
    /// Per-dimension scale factors: (max - min) / 255.0 (length = dimension)
    pub scales: Vec<f32>,
    /// Per-dimension maximum values — only used during calibration
    maxs: Vec<f32>,
    /// Number of calibration samples seen
    pub calibration_count: usize,
    /// Whether calibration is complete (frozen)
    pub calibrated: bool,
}

/// Minimum scale to avoid division by zero for constant-value dimensions.
const MIN_SCALE: f32 = 1.0 / 255.0;

impl QuantizationParams {
    /// Create uncalibrated params for the given dimension.
    pub fn new(dimension: usize) -> Self {
        QuantizationParams {
            mins: vec![f32::MAX; dimension],
            scales: vec![MIN_SCALE; dimension],
            maxs: vec![f32::MIN; dimension],
            calibration_count: 0,
            calibrated: false,
        }
    }

    /// Dimension count.
    pub fn dimension(&self) -> usize {
        self.mins.len()
    }

    /// Update running min/max from a new embedding (pre-calibration).
    ///
    /// Panics if called after calibration is finalized.
    pub fn update_calibration(&mut self, embedding: &[f32]) {
        assert!(
            !self.calibrated,
            "cannot update calibration after finalization"
        );
        assert_eq!(embedding.len(), self.mins.len(), "dimension mismatch");

        for (i, &val) in embedding.iter().enumerate() {
            if val < self.mins[i] {
                self.mins[i] = val;
            }
            if val > self.maxs[i] {
                self.maxs[i] = val;
            }
        }
        self.calibration_count += 1;
    }

    /// Freeze calibration parameters and compute scales.
    ///
    /// After this call, `quantize_into` and `dequantize_into` are usable.
    /// Handles zero-range dimensions by clamping scale to MIN_SCALE.
    pub fn finalize_calibration(&mut self) {
        for i in 0..self.mins.len() {
            let range = self.maxs[i] - self.mins[i];
            if range <= 0.0 {
                // Constant dimension: center the single value at 128
                self.scales[i] = MIN_SCALE;
            } else {
                self.scales[i] = range / 255.0;
            }
        }
        self.calibrated = true;
    }

    /// Quantize an f32 embedding into a u8 buffer.
    ///
    /// Values outside the calibrated [min, max] range are clipped to 0 or 255.
    /// `out` must have length == dimension.
    #[inline]
    pub fn quantize_into(&self, embedding: &[f32], out: &mut [u8]) {
        debug_assert!(
            self.calibrated,
            "must finalize calibration before quantizing"
        );
        debug_assert_eq!(embedding.len(), self.mins.len());
        debug_assert_eq!(out.len(), self.mins.len());
        debug_assert!(
            embedding.iter().all(|v| v.is_finite()),
            "embedding contains NaN or Infinity"
        );

        for i in 0..embedding.len() {
            let val = (embedding[i] - self.mins[i]) / self.scales[i];
            // clamp to [0, 255] and round
            out[i] = val.round().clamp(0.0, 255.0) as u8;
        }
    }

    /// Quantize an f32 embedding, returning a new Vec<u8>.
    pub fn quantize(&self, embedding: &[f32]) -> Vec<u8> {
        let mut out = vec![0u8; embedding.len()];
        self.quantize_into(embedding, &mut out);
        out
    }

    /// Dequantize a u8 buffer back to f32.
    ///
    /// `out` must have length == dimension.
    #[inline]
    pub fn dequantize_into(&self, quantized: &[u8], out: &mut [f32]) {
        debug_assert_eq!(quantized.len(), self.mins.len());
        debug_assert_eq!(out.len(), self.mins.len());

        for i in 0..quantized.len() {
            out[i] = quantized[i] as f32 * self.scales[i] + self.mins[i];
        }
    }

    /// Dequantize a u8 buffer, returning a new Vec<f32>.
    pub fn dequantize(&self, quantized: &[u8]) -> Vec<f32> {
        let mut out = vec![0.0f32; quantized.len()];
        self.dequantize_into(quantized, &mut out);
        out
    }

    /// Compute the L2 norm of a quantized vector (for cosine similarity caching).
    pub fn quantized_l2_norm(&self, quantized: &[u8]) -> f32 {
        let mut sum = 0.0f32;
        for (i, &q) in quantized.iter().enumerate() {
            let val = q as f32 * self.scales[i] + self.mins[i];
            sum += val * val;
        }
        sum.sqrt()
    }

    // ========================================================================
    // Binary serialization for mmap/snapshot persistence
    // ========================================================================

    /// Serialize to bytes: [dimension u32 LE] [mins: dim * f32 LE] [scales: dim * f32 LE]
    pub fn to_bytes(&self) -> Vec<u8> {
        let dim = self.mins.len();
        // 4 (dim) + dim*4 (mins) + dim*4 (scales)
        let mut buf = Vec::with_capacity(4 + dim * 8);
        buf.write_u32::<LittleEndian>(dim as u32).unwrap();
        for &m in &self.mins {
            buf.write_f32::<LittleEndian>(m).unwrap();
        }
        for &s in &self.scales {
            buf.write_f32::<LittleEndian>(s).unwrap();
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        let mut reader = Cursor::new(data);
        let dim = reader.read_u32::<LittleEndian>()? as usize;
        let mut mins = vec![0.0f32; dim];
        for m in &mut mins {
            *m = reader.read_f32::<LittleEndian>()?;
        }
        let mut scales = vec![0.0f32; dim];
        for s in &mut scales {
            *s = reader.read_f32::<LittleEndian>()?;
        }
        Ok(QuantizationParams {
            maxs: mins
                .iter()
                .zip(scales.iter())
                .map(|(&m, &s)| m + s * 255.0)
                .collect(),
            mins,
            scales,
            calibration_count: 0,
            calibrated: true, // deserialized params are always finalized
        })
    }
}

// ============================================================================
// RaBitQ Binary Quantization (SIGMOD 2024)
// ============================================================================

/// Per-collection RaBitQ parameters for binary quantization.
///
/// Stores the dataset centroid and a random orthogonal rotation matrix.
/// Vectors are centered, rotated, then binarized (sign of each dimension).
///
/// Reference: Gao & Long, "RaBitQ: Quantizing High-Dimensional Vectors with
/// a Theoretical Error Bound for Approximate Nearest Neighbor Search", SIGMOD 2024.
#[derive(Debug, Clone)]
pub struct RaBitQParams {
    /// D-dimensional centroid (mean of calibration vectors)
    centroid: Vec<f32>,
    /// Accumulator for centroid computation (pre-calibration only)
    centroid_sum: Vec<f32>,
    /// D×D orthogonal rotation matrix, stored flat row-major.
    /// Row i of the matrix is rotation[i*D .. (i+1)*D].
    rotation: Vec<f32>,
    /// Deterministic seed for rotation matrix regeneration.
    seed: u64,
    /// Embedding dimension
    pub dimension: usize,
    /// Number of calibration samples seen
    pub calibration_count: usize,
    /// Whether calibration is complete (frozen)
    pub calibrated: bool,
}

impl RaBitQParams {
    /// Create uncalibrated params for the given dimension.
    pub fn new(dimension: usize) -> Self {
        RaBitQParams {
            centroid: vec![0.0; dimension],
            centroid_sum: vec![0.0; dimension],
            rotation: Vec::new(), // generated during finalization
            seed: 42,
            dimension,
            calibration_count: 0,
            calibrated: false,
        }
    }

    /// Number of bytes per binary code for the given dimension.
    pub fn code_size(dimension: usize) -> usize {
        dimension.div_ceil(8)
    }

    /// Update calibration with a new embedding (accumulate into centroid sum).
    pub fn update_calibration(&mut self, embedding: &[f32]) {
        assert!(
            !self.calibrated,
            "cannot update calibration after finalization"
        );
        assert_eq!(embedding.len(), self.dimension, "dimension mismatch");

        for (i, &val) in embedding.iter().enumerate() {
            self.centroid_sum[i] += val;
        }
        self.calibration_count += 1;
    }

    /// Freeze calibration: compute centroid and generate rotation matrix.
    pub fn finalize_calibration(&mut self) {
        let n = self.calibration_count as f32;
        if n > 0.0 {
            for i in 0..self.dimension {
                self.centroid[i] = self.centroid_sum[i] / n;
            }
        }
        self.rotation = generate_orthogonal_matrix(self.dimension, self.seed);
        self.centroid_sum = Vec::new(); // free accumulator memory
        self.calibrated = true;
    }

    /// Encode an f32 embedding into a binary code + auxiliary scalars.
    ///
    /// Returns (binary_code, centroid_dist, quantized_dot).
    pub fn encode(&self, embedding: &[f32]) -> (Vec<u8>, f32, f32) {
        debug_assert!(self.calibrated);
        let dim = self.dimension;
        let code_size = Self::code_size(dim);

        // Center
        let mut centered = vec![0.0f32; dim];
        for i in 0..dim {
            centered[i] = embedding[i] - self.centroid[i];
        }

        // centroid_dist = ||centered||
        let centroid_dist = centered.iter().map(|x| x * x).sum::<f32>().sqrt();

        // Handle degenerate case (vector equals centroid)
        if centroid_dist < 1e-10 {
            return (vec![0u8; code_size], centroid_dist, 0.8);
        }

        // Normalize
        let inv_cd = 1.0 / centroid_dist;
        let mut normalized = vec![0.0f32; dim];
        for i in 0..dim {
            normalized[i] = centered[i] * inv_cd;
        }

        // Rotate: rotated = P^T · normalized
        let mut rotated = vec![0.0f32; dim];
        mat_vec_mul_transpose(&self.rotation, &normalized, &mut rotated, dim);

        // Binarize + compute quantized_dot
        let mut code = vec![0u8; code_size];
        let mut abs_sum = 0.0f32;
        for (d, &rotated_val) in rotated.iter().enumerate() {
            if rotated_val > 0.0 {
                let byte_idx = d / 8;
                let bit_idx = 7 - (d % 8); // MSB-first packing
                code[byte_idx] |= 1 << bit_idx;
            }
            abs_sum += rotated_val.abs();
        }

        let inv_sqrt_d = 1.0 / (dim as f32).sqrt();
        let quantized_dot = abs_sum * inv_sqrt_d; // = ⟨ō, o_normalized⟩

        (code, centroid_dist, quantized_dot)
    }

    /// In-place encode variant. Writes binary code into `code` buffer.
    /// Returns (centroid_dist, quantized_dot).
    pub fn encode_into(&self, embedding: &[f32], code: &mut [u8]) -> (f32, f32) {
        let (code_vec, cd, qd) = self.encode(embedding);
        code.copy_from_slice(&code_vec);
        (cd, qd)
    }

    /// Preprocess a query for binary distance computation.
    ///
    /// Returns (q_rotated, q_dist) where:
    /// - q_rotated: centered, normalized, then rotated query
    /// - q_dist: ||query - centroid||
    ///
    /// This is O(D²) due to the rotation and should be called ONCE per search.
    pub fn preprocess_query(&self, query: &[f32]) -> (Vec<f32>, f32) {
        debug_assert!(self.calibrated);
        let dim = self.dimension;

        // Center
        let mut centered = vec![0.0f32; dim];
        for i in 0..dim {
            centered[i] = query[i] - self.centroid[i];
        }

        // q_dist
        let q_dist = centered.iter().map(|x| x * x).sum::<f32>().sqrt();

        if q_dist < 1e-10 {
            return (vec![0.0f32; dim], q_dist);
        }

        // Normalize
        let inv_qd = 1.0 / q_dist;
        let mut normalized = vec![0.0f32; dim];
        for i in 0..dim {
            normalized[i] = centered[i] * inv_qd;
        }

        // Rotate: q_rotated = P^T · normalized
        let mut q_rotated = vec![0.0f32; dim];
        mat_vec_mul_transpose(&self.rotation, &normalized, &mut q_rotated, dim);

        (q_rotated, q_dist)
    }

    /// Lossy reconstruction of an f32 embedding from its binary code.
    ///
    /// Used for cold paths (snapshots, mmap persistence). Quality is coarse:
    /// only sign information is preserved per dimension.
    pub fn dequantize(&self, code: &[u8], centroid_dist: f32, quantized_dot: f32) -> Vec<f32> {
        let dim = self.dimension;
        let inv_sqrt_d = 1.0 / (dim as f32).sqrt();

        // Reconstruct the rotated vector: sign_vector * (quantized_dot / sqrt(D))
        // This gives a uniform-magnitude approximation in the rotated space.
        let magnitude = quantized_dot * inv_sqrt_d;
        let mut rotated = vec![0.0f32; dim];
        for (d, rotated_val) in rotated.iter_mut().enumerate() {
            let byte_idx = d / 8;
            let bit_idx = 7 - (d % 8);
            let bit = (code[byte_idx] >> bit_idx) & 1;
            *rotated_val = if bit == 1 { magnitude } else { -magnitude };
        }

        // Un-rotate: embedding_normalized ≈ P · rotated (undo P^T rotation)
        let mut normalized = vec![0.0f32; dim];
        mat_vec_mul(&self.rotation, &rotated, &mut normalized, dim);

        // Un-normalize and un-center
        let mut result = vec![0.0f32; dim];
        for i in 0..dim {
            result[i] = normalized[i] * centroid_dist + self.centroid[i];
        }
        result
    }

    // ========================================================================
    // Serialization
    // ========================================================================

    /// Serialize to bytes: [dimension u32 LE] [seed u64 LE] [centroid: dim * f32 LE]
    ///
    /// The rotation matrix is NOT stored — it's regenerated from seed on load,
    /// saving D²×4 bytes (e.g., 576 KB for D=384).
    pub fn to_bytes(&self) -> Vec<u8> {
        let dim = self.dimension;
        let mut buf = Vec::with_capacity(4 + 8 + dim * 4);
        buf.write_u32::<LittleEndian>(dim as u32).unwrap();
        buf.write_u64::<LittleEndian>(self.seed).unwrap();
        for &c in &self.centroid {
            buf.write_f32::<LittleEndian>(c).unwrap();
        }
        buf
    }

    /// Deserialize from bytes. Regenerates rotation matrix from stored seed.
    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        let mut reader = Cursor::new(data);
        let dim = reader.read_u32::<LittleEndian>()? as usize;
        let seed = reader.read_u64::<LittleEndian>()?;
        let mut centroid = vec![0.0f32; dim];
        for c in &mut centroid {
            *c = reader.read_f32::<LittleEndian>()?;
        }
        let rotation = generate_orthogonal_matrix(dim, seed);
        Ok(RaBitQParams {
            centroid,
            centroid_sum: Vec::new(),
            rotation,
            seed,
            dimension: dim,
            calibration_count: 0,
            calibrated: true,
        })
    }
}

// ============================================================================
// Linear algebra helpers
// ============================================================================

/// Matrix-vector multiply: out = M · v (M is dim×dim row-major).
fn mat_vec_mul(matrix: &[f32], vec_in: &[f32], vec_out: &mut [f32], dim: usize) {
    for (i, out_val) in vec_out.iter_mut().enumerate().take(dim) {
        let mut sum = 0.0f32;
        let row_start = i * dim;
        for j in 0..dim {
            sum += matrix[row_start + j] * vec_in[j];
        }
        *out_val = sum;
    }
}

/// Matrix-transpose-vector multiply: out = M^T · v (M is dim×dim row-major).
fn mat_vec_mul_transpose(matrix: &[f32], vec_in: &[f32], vec_out: &mut [f32], dim: usize) {
    vec_out.fill(0.0);
    for (i, &vi) in vec_in.iter().enumerate().take(dim) {
        let row_start = i * dim;
        for j in 0..dim {
            vec_out[j] += matrix[row_start + j] * vi;
        }
    }
}

/// Generate a D×D random orthogonal matrix via QR decomposition of a Gaussian matrix.
///
/// Uses modified Gram-Schmidt orthogonalization, seeded for reproducibility.
/// Matches the reference implementation: `Q, _ = np.linalg.qr(np.random.randn(D, D))`.
fn generate_orthogonal_matrix(dim: usize, seed: u64) -> Vec<f32> {
    if dim == 0 {
        return Vec::new();
    }

    let mut rng = StdRng::seed_from_u64(seed);

    // Generate D×D matrix with entries from N(0,1) via Box-Muller
    let total = dim * dim;
    let mut matrix = Vec::with_capacity(total);
    let mut i = 0;
    while i < total {
        // Box-Muller transform: generate pairs of N(0,1) from U(0,1)
        let u1: f64 = rng.gen::<f64>().max(1e-300); // clamp away from 0 to avoid ln(0)
        let u2: f64 = rng.gen::<f64>();
        let r = (-2.0 * u1.ln()).sqrt();
        let theta = 2.0 * std::f64::consts::PI * u2;
        matrix.push((r * theta.cos()) as f32);
        i += 1;
        if i < total {
            matrix.push((r * theta.sin()) as f32);
            i += 1;
        }
    }

    // Modified Gram-Schmidt orthogonalization (in-place, row-wise)
    for i in 0..dim {
        // Subtract projections onto all previous rows
        for j in 0..i {
            let mut dot = 0.0f32;
            for k in 0..dim {
                dot += matrix[i * dim + k] * matrix[j * dim + k];
            }
            for k in 0..dim {
                matrix[i * dim + k] -= dot * matrix[j * dim + k];
            }
        }
        // Normalize row i
        let mut norm_sq = 0.0f32;
        for k in 0..dim {
            norm_sq += matrix[i * dim + k] * matrix[i * dim + k];
        }
        let norm = norm_sq.sqrt();
        if norm < 1e-10 {
            // Degenerate row (extremely rare) — regenerate and re-orthogonalize.
            for k in 0..dim {
                let u1: f64 = rng.gen::<f64>().max(1e-300);
                let u2: f64 = rng.gen::<f64>();
                let r = (-2.0 * u1.ln()).sqrt();
                let theta = 2.0 * std::f64::consts::PI * u2;
                matrix[i * dim + k] = (r * theta.cos()) as f32;
            }
            // Re-orthogonalize against all previous rows
            for j in 0..i {
                let mut dot = 0.0f32;
                for k in 0..dim {
                    dot += matrix[i * dim + k] * matrix[j * dim + k];
                }
                for k in 0..dim {
                    matrix[i * dim + k] -= dot * matrix[j * dim + k];
                }
            }
            // Normalize the regenerated row
            let fallback_norm = matrix[i * dim..(i + 1) * dim]
                .iter()
                .map(|x| x * x)
                .sum::<f32>()
                .sqrt();
            if fallback_norm > 1e-10 {
                let inv = 1.0 / fallback_norm;
                for k in 0..dim {
                    matrix[i * dim + k] *= inv;
                }
            }
        } else {
            let inv = 1.0 / norm;
            for k in 0..dim {
                matrix[i * dim + k] *= inv;
            }
        }
    }

    matrix
}

/// Compute binary inner product between a code and a rotated query.
///
/// Returns (1/√D) · Σ (2·bit[d] - 1) · q_rotated[d]
/// This is the core O(D) hot-path operation for RaBitQ distance estimation.
pub fn binary_inner_product(code: &[u8], q_rotated: &[f32], dim: usize) -> f32 {
    let mut sum = 0.0f32;
    for (d, &q_val) in q_rotated.iter().enumerate().take(dim) {
        let byte_idx = d / 8;
        let bit_idx = 7 - (d % 8); // MSB-first packing
        let bit = (code[byte_idx] >> bit_idx) & 1;
        if bit == 1 {
            sum += q_val;
        } else {
            sum -= q_val;
        }
    }
    sum / (dim as f32).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantize_dequantize_roundtrip() {
        let mut params = QuantizationParams::new(4);
        // Calibrate with two vectors to establish range
        params.update_calibration(&[0.0, -1.0, 0.5, 2.0]);
        params.update_calibration(&[1.0, 1.0, -0.5, -2.0]);
        params.finalize_calibration();

        // Quantize and dequantize
        let original = [0.5, 0.0, 0.0, 0.0];
        let quantized = params.quantize(&original);
        let recovered = params.dequantize(&quantized);

        // Check roundtrip accuracy (within quantization error)
        for (o, r) in original.iter().zip(recovered.iter()) {
            assert!(
                (o - r).abs() < 0.02,
                "roundtrip error too large: {} vs {}",
                o,
                r
            );
        }
    }

    #[test]
    fn test_quantize_range_boundaries() {
        let mut params = QuantizationParams::new(2);
        params.update_calibration(&[0.0, 0.0]);
        params.update_calibration(&[1.0, 1.0]);
        params.finalize_calibration();

        // min maps to 0, max maps to 255
        let q_min = params.quantize(&[0.0, 0.0]);
        assert_eq!(q_min, vec![0, 0]);

        let q_max = params.quantize(&[1.0, 1.0]);
        assert_eq!(q_max, vec![255, 255]);

        // midpoint maps to ~128
        let q_mid = params.quantize(&[0.5, 0.5]);
        assert!(q_mid[0] == 127 || q_mid[0] == 128);
    }

    #[test]
    fn test_quantize_clipping() {
        let mut params = QuantizationParams::new(2);
        params.update_calibration(&[0.0, 0.0]);
        params.update_calibration(&[1.0, 1.0]);
        params.finalize_calibration();

        // Values outside calibrated range are clipped
        let q = params.quantize(&[-0.5, 1.5]);
        assert_eq!(q[0], 0); // clipped to min
        assert_eq!(q[1], 255); // clipped to max
    }

    #[test]
    fn test_zero_range_dimension() {
        let mut params = QuantizationParams::new(2);
        // First dim has range, second is constant
        params.update_calibration(&[0.0, 5.0]);
        params.update_calibration(&[1.0, 5.0]);
        params.finalize_calibration();

        // Constant dimension should still work (maps to ~0)
        let q = params.quantize(&[0.5, 5.0]);
        assert!(q[0] == 127 || q[0] == 128);
        // Constant dim: (5.0 - 5.0) / MIN_SCALE = 0.0 → 0
        assert_eq!(q[1], 0);
    }

    #[test]
    fn test_single_vector_calibration() {
        let mut params = QuantizationParams::new(3);
        params.update_calibration(&[1.0, 2.0, 3.0]);
        params.finalize_calibration();

        // All dimensions are zero-range, should still work
        let q = params.quantize(&[1.0, 2.0, 3.0]);
        // Zero-range: (val - min) / MIN_SCALE = 0 → 0
        assert_eq!(q, vec![0, 0, 0]);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut params = QuantizationParams::new(4);
        params.update_calibration(&[0.0, -1.0, 0.5, 2.0]);
        params.update_calibration(&[1.0, 1.0, -0.5, -2.0]);
        params.finalize_calibration();

        let bytes = params.to_bytes();
        let restored = QuantizationParams::from_bytes(&bytes).unwrap();

        assert_eq!(params.mins, restored.mins);
        assert_eq!(params.scales, restored.scales);
        assert!(restored.calibrated);
    }

    #[test]
    fn test_quantized_l2_norm() {
        let mut params = QuantizationParams::new(3);
        params.update_calibration(&[0.0, 0.0, 0.0]);
        params.update_calibration(&[1.0, 1.0, 1.0]);
        params.finalize_calibration();

        let embedding = [0.6, 0.8, 0.0];
        let quantized = params.quantize(&embedding);
        let norm = params.quantized_l2_norm(&quantized);

        // Expected: sqrt(0.6^2 + 0.8^2 + 0^2) = 1.0 (approximately, due to quantization)
        assert!((norm - 1.0).abs() < 0.02, "norm = {}", norm);
    }

    #[test]
    #[should_panic(expected = "cannot update calibration after finalization")]
    fn test_update_after_finalize_panics() {
        let mut params = QuantizationParams::new(2);
        params.update_calibration(&[0.0, 0.0]);
        params.finalize_calibration();
        params.update_calibration(&[1.0, 1.0]); // should panic
    }

    // ================================================================
    // RaBitQ tests
    // ================================================================

    #[test]
    fn test_rabitq_rotation_orthogonality() {
        let dim = 64;
        let matrix = generate_orthogonal_matrix(dim, 42);

        // P^T · P should be approximately I
        for i in 0..dim {
            for j in 0..dim {
                let mut dot = 0.0f32;
                for k in 0..dim {
                    dot += matrix[k * dim + i] * matrix[k * dim + j];
                }
                let expected = if i == j { 1.0 } else { 0.0 };
                assert!(
                    (dot - expected).abs() < 1e-4,
                    "P^T·P[{},{}] = {}, expected {}",
                    i,
                    j,
                    dot,
                    expected
                );
            }
        }
    }

    #[test]
    fn test_rabitq_encode_produces_valid_code() {
        let mut params = RaBitQParams::new(16);
        // Calibrate with a few vectors
        for i in 0..10 {
            let v: Vec<f32> = (0..16)
                .map(|j| ((i * 16 + j) as f32 / 100.0).sin())
                .collect();
            params.update_calibration(&v);
        }
        params.finalize_calibration();

        let embedding: Vec<f32> = (0..16).map(|j| (j as f32 / 10.0).cos()).collect();
        let (code, cd, x0) = params.encode(&embedding);

        assert_eq!(code.len(), RaBitQParams::code_size(16)); // 2 bytes
        assert!(cd > 0.0, "centroid_dist should be positive");
        assert!(x0 > 0.0 && x0 <= 1.0, "x0 should be in (0, 1], got {}", x0);
    }

    #[test]
    fn test_rabitq_distance_estimation() {
        let dim = 64;
        let mut params = RaBitQParams::new(dim);

        // Calibrate
        for i in 0..100 {
            let v: Vec<f32> = (0..dim)
                .map(|j| ((i * dim + j) as f32 / 1000.0).sin())
                .collect();
            params.update_calibration(&v);
        }
        params.finalize_calibration();

        // Encode a vector and estimate distance to a query
        let data: Vec<f32> = (0..dim).map(|j| (j as f32 / 30.0).sin()).collect();
        let query: Vec<f32> = (0..dim).map(|j| (j as f32 / 20.0).cos()).collect();

        let (code, cd, x0) = params.encode(&data);
        let (q_rotated, q_dist) = params.preprocess_query(&query);

        // Estimate distance
        let ip = binary_inner_product(&code, &q_rotated, dim);
        let ratio = ip / x0;
        let est_dist_sq = (cd * cd + q_dist * q_dist - 2.0 * cd * q_dist * ratio).max(0.0);

        // Exact distance
        let exact_dist_sq: f32 = data
            .iter()
            .zip(query.iter())
            .map(|(a, b)| (a - b) * (a - b))
            .sum();

        // RaBitQ error is O(1/√D), so for D=64 we expect ~12% error
        let relative_error = (est_dist_sq - exact_dist_sq).abs() / exact_dist_sq.max(1e-6);
        assert!(
            relative_error < 0.5,
            "relative error {} too large (est={}, exact={})",
            relative_error,
            est_dist_sq,
            exact_dist_sq
        );
    }

    #[test]
    fn test_rabitq_serialization_roundtrip() {
        let dim = 32;
        let mut params = RaBitQParams::new(dim);
        for i in 0..50 {
            let v: Vec<f32> = (0..dim).map(|j| ((i + j) as f32).sin()).collect();
            params.update_calibration(&v);
        }
        params.finalize_calibration();

        let bytes = params.to_bytes();
        let restored = RaBitQParams::from_bytes(&bytes).unwrap();

        assert_eq!(params.centroid, restored.centroid);
        assert_eq!(params.dimension, restored.dimension);
        assert_eq!(params.seed, restored.seed);
        assert!(restored.calibrated);
        // Rotation should be regenerated identically from the same seed
        assert_eq!(params.rotation.len(), restored.rotation.len());
        for (a, b) in params.rotation.iter().zip(restored.rotation.iter()) {
            assert!((a - b).abs() < 1e-6, "rotation mismatch: {} vs {}", a, b);
        }
    }

    #[test]
    fn test_rabitq_dequantize_preserves_direction() {
        let dim = 32;
        let mut params = RaBitQParams::new(dim);
        for i in 0..50 {
            let v: Vec<f32> = (0..dim).map(|j| ((i + j) as f32 / 10.0).sin()).collect();
            params.update_calibration(&v);
        }
        params.finalize_calibration();

        let embedding: Vec<f32> = (0..dim).map(|j| (j as f32 / 5.0).cos()).collect();
        let (code, cd, x0) = params.encode(&embedding);
        let reconstructed = params.dequantize(&code, cd, x0);

        // Cosine similarity between original and reconstructed should be positive
        let dot: f32 = embedding
            .iter()
            .zip(reconstructed.iter())
            .map(|(a, b)| a * b)
            .sum();
        let norm_orig = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_recon = reconstructed.iter().map(|x| x * x).sum::<f32>().sqrt();
        let cosine = dot / (norm_orig * norm_recon + 1e-10);

        assert!(
            cosine > 0.5,
            "reconstructed vector should roughly preserve direction, cosine={}",
            cosine
        );
    }

    #[test]
    fn test_rabitq_degenerate_zero_vector() {
        let dim = 8;
        let mut params = RaBitQParams::new(dim);
        params.update_calibration(&[1.0; 8]);
        params.finalize_calibration();

        // Encode a vector that equals the centroid
        let (code, cd, x0) = params.encode(&params.centroid.clone());
        assert!(cd < 1e-9);
        assert!((x0 - 0.8).abs() < 1e-6, "degenerate x0 should be 0.8");
        assert_eq!(code, vec![0u8; 1]); // all zeros
    }

    #[test]
    fn test_binary_inner_product_basic() {
        // 8-dim: code = [0b11110000] = bits 1,1,1,1,0,0,0,0
        let code = vec![0b11110000u8];
        let q = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let ip = binary_inner_product(&code, &q, 8);

        // Expected: (1+2+3+4 - 5-6-7-8) / sqrt(8) = (10 - 26) / 2.828 = -16/2.828
        let expected = (1.0 + 2.0 + 3.0 + 4.0 - 5.0 - 6.0 - 7.0 - 8.0) / (8.0f32).sqrt();
        assert!(
            (ip - expected).abs() < 1e-5,
            "ip={}, expected={}",
            ip,
            expected
        );
    }
}
