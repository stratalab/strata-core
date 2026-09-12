//! Exact vector scoring.

use crate::diagnostics::EngineError;

use super::{VectorDistanceMetric, VectorEmbedding};

pub(crate) fn vector_score(
    query: &VectorEmbedding,
    candidate: &VectorEmbedding,
    metric: VectorDistanceMetric,
) -> Result<f32, EngineError> {
    if query.dimension() != candidate.dimension() {
        return Err(EngineError::invalid_input(
            "invalid_argument.engine.vector_dimension",
            "vector score dimension mismatch",
        ));
    }
    let score = match metric {
        VectorDistanceMetric::Cosine => cosine_similarity(query.as_slice(), candidate.as_slice()),
        VectorDistanceMetric::Euclidean => {
            euclidean_similarity(query.as_slice(), candidate.as_slice())
        }
        VectorDistanceMetric::DotProduct => dot_product(query.as_slice(), candidate.as_slice()),
    };
    Ok(score)
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    // Accumulate in f64: an f32 norm overflows to inf around sqrt(f32::MAX),
    // which silently corrupts the score (an aligned vector scores 0.0, or
    // inf/inf yields NaN). f64 cannot overflow for f32 inputs across the max
    // dimension, and the final ratio is clamped to cosine's [-1, 1] range.
    let mut dot = 0.0f64;
    let mut left_norm = 0.0f64;
    let mut right_norm = 0.0f64;
    for (left_value, right_value) in left.iter().zip(right) {
        let left_value = f64::from(*left_value);
        let right_value = f64::from(*right_value);
        dot += left_value * right_value;
        left_norm += left_value * left_value;
        right_norm += right_value * right_value;
    }
    let denominator = (left_norm * right_norm).sqrt();
    if denominator == 0.0 {
        0.0
    } else {
        // Clamped to [-1, 1], so the f64 -> f32 narrowing cannot lose meaning.
        #[allow(clippy::cast_possible_truncation)]
        let cosine = (dot / denominator).clamp(-1.0, 1.0) as f32;
        cosine
    }
}

fn euclidean_similarity(left: &[f32], right: &[f32]) -> f32 {
    // Accumulate the squared distance in f64 for the same reason as cosine:
    // an f32 sum overflows to inf, collapsing every far vector to 1/(1+inf)=0.0
    // and tying them.
    let distance = left
        .iter()
        .zip(right)
        .map(|(left_value, right_value)| {
            let delta = f64::from(*left_value) - f64::from(*right_value);
            delta * delta
        })
        .sum::<f64>()
        .sqrt();
    // Bounded to (0, 1], so the f64 -> f32 narrowing cannot lose meaning.
    #[allow(clippy::cast_possible_truncation)]
    let similarity = (1.0 / (1.0 + distance)) as f32;
    similarity
}

fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(left_value, right_value)| left_value * right_value)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::vector_score;
    use crate::data::vector::{VectorDistanceMetric, VectorEmbedding};

    #[test]
    fn metric_scores_are_higher_is_better() {
        let query = VectorEmbedding::new([1.0, 0.0]).expect("valid query");
        let same = VectorEmbedding::new([1.0, 0.0]).expect("valid vector");
        let orthogonal = VectorEmbedding::new([0.0, 1.0]).expect("valid vector");
        assert!(
            vector_score(&query, &same, VectorDistanceMetric::Cosine).expect("score")
                > vector_score(&query, &orthogonal, VectorDistanceMetric::Cosine).expect("score")
        );
        let euclidean =
            vector_score(&query, &same, VectorDistanceMetric::Euclidean).expect("score");
        assert!((euclidean - 1.0).abs() < f32::EPSILON);
        let dot = vector_score(&query, &same, VectorDistanceMetric::DotProduct).expect("score");
        assert!((dot - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn cosine_is_correct_and_bounded_at_magnitude_extremes() {
        let query = VectorEmbedding::new([1.0, 0.0]).expect("valid query");
        let aligned = VectorEmbedding::new([1e20, 0.0]).expect("valid vector");
        let orthogonal = VectorEmbedding::new([0.0, 1e20]).expect("valid vector");

        let aligned_score =
            vector_score(&query, &aligned, VectorDistanceMetric::Cosine).expect("score");
        let orthogonal_score =
            vector_score(&query, &orthogonal, VectorDistanceMetric::Cosine).expect("score");

        // A perfectly aligned vector has cosine 1.0 regardless of magnitude, and
        // must outrank an orthogonal one. The f32-accumulator bug scored it 0.0
        // (the norm overflowed to inf), ranking the best match last.
        assert!(
            (aligned_score - 1.0).abs() < 1e-3,
            "aligned cosine: {aligned_score}"
        );
        assert!(
            aligned_score > orthogonal_score,
            "aligned {aligned_score} vs orthogonal {orthogonal_score}"
        );

        // Cosine is mathematically bounded to [-1, 1]. Two identical
        // extreme-magnitude vectors overflowed every f32 accumulator, yielding
        // inf/inf = NaN (or a value outside the range).
        let huge = VectorEmbedding::new([1e20, 1e20]).expect("valid vector");
        let self_score = vector_score(&huge, &huge, VectorDistanceMetric::Cosine).expect("score");
        assert!(
            (-1.0..=1.0).contains(&self_score),
            "cosine out of [-1,1]: {self_score}"
        );
        assert!(
            (self_score - 1.0).abs() < 1e-3,
            "identical-vector cosine: {self_score}"
        );
    }

    #[test]
    fn euclidean_distinguishes_far_vectors_instead_of_collapsing_to_zero() {
        // Two candidates at different large distances from the query. The
        // f32-accumulator bug squared the deltas into inf, so both similarities
        // collapsed to 1/(1+inf)=0.0 and tied. f64 accumulation keeps them
        // distinct and correctly ordered (nearer -> higher similarity).
        let query = VectorEmbedding::new([1.0, 0.0]).expect("valid query");
        let near = VectorEmbedding::new([1e20, 0.0]).expect("valid vector");
        let far = VectorEmbedding::new([2e20, 0.0]).expect("valid vector");

        let near_score =
            vector_score(&query, &near, VectorDistanceMetric::Euclidean).expect("score");
        let far_score = vector_score(&query, &far, VectorDistanceMetric::Euclidean).expect("score");

        assert!(near_score.is_finite() && far_score.is_finite());
        assert!(
            near_score > far_score,
            "near {near_score} should outrank far {far_score}"
        );
    }

    #[test]
    fn cosine_matches_a_known_partial_overlap_value() {
        // A 45-degree pair has cosine 1/sqrt(2). Pinning an exact value strictly
        // between 0 and 1 is what distinguishes `dot / denominator` from a
        // `dot * denominator` (the product clamps to 1.0), which the
        // extremes-only cases cannot see.
        let query = VectorEmbedding::new([1.0, 0.0]).expect("valid query");
        let candidate = VectorEmbedding::new([1.0, 1.0]).expect("valid vector");
        let score = vector_score(&query, &candidate, VectorDistanceMetric::Cosine).expect("score");
        let expected = 1.0_f32 / 2.0_f32.sqrt();
        assert!(
            (score - expected).abs() < 1e-4,
            "cosine {score}, expected {expected}"
        );
    }
}
