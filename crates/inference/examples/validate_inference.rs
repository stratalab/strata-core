//! Inference pipeline validation benchmark.
//!
//! Tests local models (embed + generate) and cloud providers (Anthropic, OpenAI, Google).
//! Outputs JSON report with throughput, latency, and error handling metrics.
//!
//! Usage:
//!   cargo run --example validate_inference \
//!     --features "local,download,anthropic,openai,google" \
//!     -- [--skip-local] [--skip-cloud]
//!
//! Environment variables for cloud providers:
//!   ANTHROPIC_API_KEY, OPENAI_API_KEY, GOOGLE_API_KEY

use std::time::Instant;

use strata_inference::{GenerateRequest, GenerationEngine, ProviderKind};

#[cfg(feature = "local")]
use strata_inference::EmbeddingEngine;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let skip_local = args.iter().any(|a| a == "--skip-local");
    let skip_cloud = args.iter().any(|a| a == "--skip-cloud");

    println!("{{");
    println!("  \"validation\": \"strata-inference\",");

    // ===== Local Models =====
    if !skip_local {
        #[cfg(feature = "local")]
        {
            print!("  \"local\": {{");
            validate_local_embed();
            validate_local_generate();
            println!("\n  }},");
        }
        #[cfg(not(feature = "local"))]
        {
            println!("  \"local\": {{\"skip\": \"feature not enabled\"}},");
        }
    } else {
        println!("  \"local\": {{\"skip\": \"--skip-local\"}},");
    }

    // ===== Cloud Providers =====
    if !skip_cloud {
        print!("  \"cloud\": {{");
        let mut any_cloud = false;

        // Anthropic
        if let Ok(key) = std::env::var("ANTHROPIC_API_KEY") {
            if any_cloud {
                print!(",");
            }
            validate_cloud(
                "anthropic",
                ProviderKind::Anthropic,
                &key,
                "claude-sonnet-4-20250514",
            );
            any_cloud = true;
        }

        // OpenAI
        if let Ok(key) = std::env::var("OPENAI_API_KEY") {
            if any_cloud {
                print!(",");
            }
            validate_cloud("openai", ProviderKind::OpenAI, &key, "gpt-4o-mini");
            any_cloud = true;
        }

        // Google
        if let Ok(key) = std::env::var("GOOGLE_API_KEY") {
            if any_cloud {
                print!(",");
            }
            validate_cloud(
                "google",
                ProviderKind::Google,
                &key,
                "gemini-3-flash-preview",
            );
            any_cloud = true;
        }

        if !any_cloud {
            print!("\"skip\": \"no API keys set\"");
        }
        println!("\n  }},");
    } else {
        println!("  \"cloud\": {{\"skip\": \"--skip-cloud\"}},");
    }

    // ===== Error Handling =====
    print!("  \"error_handling\": {{");
    validate_errors();
    println!("\n  }}");

    println!("}}");
}

// =============================================================================
// Local Embedding Validation
// =============================================================================

#[cfg(feature = "local")]
fn validate_local_embed() {
    eprint!("\n  [embed] Loading miniLM... ");
    let start = Instant::now();
    let engine = match EmbeddingEngine::from_registry("miniLM") {
        Ok(e) => {
            let load_ms = start.elapsed().as_millis();
            eprintln!("OK ({load_ms}ms)");
            e
        }
        Err(e) => {
            eprintln!("FAIL: {e}");
            print!(
                "\n    \"embed\": {{\"status\": \"fail\", \"error\": {:?}}}",
                e.to_string()
            );
            return;
        }
    };

    // Single embed
    let text = "The quick brown fox jumps over the lazy dog";
    let start = Instant::now();
    let embedding = engine.embed(text).expect("embed failed");
    let single_us = start.elapsed().as_micros();

    let dim = embedding.len();
    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    eprintln!("  [embed] dim={dim}, norm={norm:.4}, single={single_us}us");

    // Batch embed throughput
    let texts: Vec<&str> = vec![
        "Medical research on cardiovascular disease",
        "Clinical trial results for phase 3 drugs",
        "FDA adverse event reporting system data",
        "Drug interaction with ACE inhibitors",
        "Myocardial infarction treatment outcomes",
        "Randomized controlled trial methodology",
        "Pharmacokinetics of novel compounds",
        "Patient safety monitoring protocols",
        "Biomarker discovery in oncology",
        "Epidemiological study of diabetes prevalence",
    ];
    let n_rounds = 10;
    let total_texts = texts.len() * n_rounds;

    let start = Instant::now();
    for _ in 0..n_rounds {
        let _ = engine.embed_batch(&texts).expect("batch embed failed");
    }
    let batch_ms = start.elapsed().as_millis();
    let throughput = (total_texts as f64 / batch_ms as f64) * 1000.0;

    eprintln!("  [embed] {total_texts} texts in {batch_ms}ms ({throughput:.0} embed/s)");

    print!(
        "\n    \"embed\": {{\"status\": \"pass\", \"dim\": {dim}, \"norm\": {norm:.4}, \
         \"single_us\": {single_us}, \"batch_texts\": {total_texts}, \
         \"batch_ms\": {batch_ms}, \"throughput_per_s\": {throughput:.0}}}"
    );
}

// =============================================================================
// Local Generation Validation
// =============================================================================

#[cfg(feature = "local")]
fn validate_local_generate() {
    eprint!("\n  [generate] Loading qwen3:1.7b... ");
    let start = Instant::now();
    let mut engine = match GenerationEngine::from_registry("qwen3:1.7b:q8_0") {
        Ok(e) => {
            let load_ms = start.elapsed().as_millis();
            eprintln!("OK ({load_ms}ms)");
            e
        }
        Err(e) => {
            eprintln!("FAIL: {e}");
            print!(
                ",\n    \"generate\": {{\"status\": \"fail\", \"error\": {:?}}}",
                e.to_string()
            );
            return;
        }
    };

    // Generate responses
    let prompts = [
        "What is the capital of France? Answer in one word.",
        "List three primary colors. Be brief.",
        "What is 2+2? Just the number.",
    ];

    let mut total_prompt_tokens = 0usize;
    let mut total_completion_tokens = 0usize;
    let mut total_ms = 0u128;
    let mut results = Vec::new();

    for prompt in &prompts {
        let req = GenerateRequest {
            prompt: prompt.to_string(),
            max_tokens: 64,
            temperature: 0.0,
            ..Default::default()
        };

        let start = Instant::now();
        match engine.generate(&req) {
            Ok(resp) => {
                let ms = start.elapsed().as_millis();
                total_prompt_tokens += resp.prompt_tokens;
                total_completion_tokens += resp.completion_tokens;
                total_ms += ms;
                eprintln!(
                    "  [generate] {}ms | {} prompt + {} completion | {:?}",
                    ms,
                    resp.prompt_tokens,
                    resp.completion_tokens,
                    resp.text.trim()
                );
                results.push(format!(
                    "{{\"ms\": {ms}, \"prompt_tokens\": {}, \"completion_tokens\": {}, \
                     \"stop\": \"{}\", \"text\": {:?}}}",
                    resp.prompt_tokens,
                    resp.completion_tokens,
                    resp.stop_reason,
                    resp.text.trim()
                ));
            }
            Err(e) => {
                eprintln!("  [generate] FAIL: {e}");
                results.push(format!("{{\"ms\": 0, \"error\": {:?}}}", e.to_string()));
            }
        }
    }

    let tok_per_s = if total_ms > 0 {
        (total_completion_tokens as f64 / total_ms as f64) * 1000.0
    } else {
        0.0
    };

    eprintln!(
        "  [generate] total: {total_completion_tokens} tokens in {total_ms}ms ({tok_per_s:.1} tok/s)"
    );

    print!(
        ",\n    \"generate\": {{\"status\": \"pass\", \"model\": \"qwen3:1.7b\", \
         \"total_prompt_tokens\": {total_prompt_tokens}, \
         \"total_completion_tokens\": {total_completion_tokens}, \
         \"total_ms\": {total_ms}, \"tok_per_s\": {tok_per_s:.1}, \
         \"responses\": [{}]}}",
        results.join(", ")
    );
}

// =============================================================================
// Cloud Provider Validation
// =============================================================================

fn validate_cloud(name: &str, provider: ProviderKind, api_key: &str, model: &str) {
    eprint!("\n  [{}] {} ... ", name, model);

    let mut engine = match GenerationEngine::cloud(provider, api_key.to_string(), model.to_string())
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("FAIL: {e}");
            print!(
                "\n    {:?}: {{\"status\": \"fail\", \"error\": {:?}}}",
                name,
                e.to_string()
            );
            return;
        }
    };

    let req = GenerateRequest {
        prompt: "What is 2+2? Reply with just the number.".to_string(),
        max_tokens: 32,
        temperature: 0.0,
        ..Default::default()
    };

    let start = Instant::now();
    match engine.generate(&req) {
        Ok(resp) => {
            let ms = start.elapsed().as_millis();
            eprintln!(
                "OK ({ms}ms) | {:?} | {} prompt + {} completion",
                resp.text.trim(),
                resp.prompt_tokens,
                resp.completion_tokens
            );
            print!(
                "\n    {:?}: {{\"status\": \"pass\", \"model\": {:?}, \"ms\": {ms}, \
                 \"prompt_tokens\": {}, \"completion_tokens\": {}, \
                 \"stop\": \"{}\", \"text\": {:?}}}",
                name,
                model,
                resp.prompt_tokens,
                resp.completion_tokens,
                resp.stop_reason,
                resp.text.trim()
            );
        }
        Err(e) => {
            let ms = start.elapsed().as_millis();
            eprintln!("FAIL ({ms}ms): {e}");
            print!(
                "\n    {:?}: {{\"status\": \"fail\", \"model\": {:?}, \"ms\": {ms}, \
                 \"error\": {:?}}}",
                name,
                model,
                e.to_string()
            );
        }
    }
}

// =============================================================================
// Error Handling Validation
// =============================================================================

fn validate_errors() {
    // Invalid API key
    eprint!("\n  [error] Invalid API key... ");
    let result = GenerationEngine::cloud(
        ProviderKind::Anthropic,
        "sk-invalid-key-12345".to_string(),
        "claude-sonnet-4-20250514".to_string(),
    );
    match result {
        Ok(mut engine) => {
            let req = GenerateRequest {
                prompt: "test".to_string(),
                max_tokens: 1,
                ..Default::default()
            };
            match engine.generate(&req) {
                Err(e) => {
                    eprintln!("correctly rejected: {e}");
                    print!(
                        "\n    \"invalid_api_key\": {{\"status\": \"pass\", \"error\": {:?}}}",
                        e.to_string()
                    );
                }
                Ok(_) => {
                    eprintln!("UNEXPECTED SUCCESS");
                    print!(
                        "\n    \"invalid_api_key\": {{\"status\": \"fail\", \
                         \"error\": \"expected rejection but got success\"}}"
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("rejected at construction: {e}");
            print!(
                "\n    \"invalid_api_key\": {{\"status\": \"pass\", \"error\": {:?}}}",
                e.to_string()
            );
        }
    }

    // Empty API key
    eprint!(",\n  [error] Empty API key... ");
    match GenerationEngine::cloud(
        ProviderKind::Anthropic,
        "".to_string(),
        "claude-sonnet-4-20250514".to_string(),
    ) {
        Err(e) => {
            eprintln!("correctly rejected: {e}");
            print!(
                "\n    \"empty_api_key\": {{\"status\": \"pass\", \"error\": {:?}}}",
                e.to_string()
            );
        }
        Ok(_) => {
            eprintln!("UNEXPECTED SUCCESS");
            print!(
                ",\n    \"empty_api_key\": {{\"status\": \"fail\", \
                 \"error\": \"expected rejection\"}}"
            );
        }
    }

    // Empty model name
    eprint!(",\n  [error] Empty model... ");
    match GenerationEngine::cloud(
        ProviderKind::Anthropic,
        "sk-test".to_string(),
        "".to_string(),
    ) {
        Err(e) => {
            eprintln!("correctly rejected: {e}");
            print!(
                "\n    \"empty_model\": {{\"status\": \"pass\", \"error\": {:?}}}",
                e.to_string()
            );
        }
        Ok(_) => {
            eprintln!("UNEXPECTED SUCCESS");
            print!(
                ",\n    \"empty_model\": {{\"status\": \"fail\", \
                 \"error\": \"expected rejection\"}}"
            );
        }
    }

    // Local provider rejected via cloud()
    eprint!(",\n  [error] Local via cloud()... ");
    match GenerationEngine::cloud(ProviderKind::Local, "key".to_string(), "model".to_string()) {
        Err(e) => {
            eprintln!("correctly rejected: {e}");
            print!(
                "\n    \"local_via_cloud\": {{\"status\": \"pass\", \"error\": {:?}}}",
                e.to_string()
            );
        }
        Ok(_) => {
            eprintln!("UNEXPECTED SUCCESS");
            print!(
                ",\n    \"local_via_cloud\": {{\"status\": \"fail\", \
                 \"error\": \"expected rejection\"}}"
            );
        }
    }
}
