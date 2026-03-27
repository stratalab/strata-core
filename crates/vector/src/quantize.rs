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
        for i in 0..quantized.len() {
            let val = quantized[i] as f32 * self.scales[i] + self.mins[i];
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
}
