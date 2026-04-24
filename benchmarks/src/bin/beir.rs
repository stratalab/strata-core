//! BEIR benchmark harness — direct executor, no Python overhead.
//!
//! Downloads and evaluates BEIR datasets using StrataDB's native search.
//! Computes nDCG@10 and compares against Pyserini BM25 baselines.
//!
//! Usage:
//!   cargo run --release --bin beir -- [OPTIONS]
//!
//! Options:
//!   --datasets nfcorpus,scifact   Comma-separated dataset names (default: nfcorpus)
//!   --recipe keyword|hybrid       Named recipe (default: keyword)
//!   --k 10                        Top-k for evaluation (default: 10)
//!   --data-dir ./datasets/beir    Where BEIR datasets are stored

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use stratadb::{SearchQuery, Strata, Value};

// =============================================================================
// BEIR Dataset Types
// =============================================================================

struct Corpus {
    docs: HashMap<String, CorpusDoc>,
}

struct CorpusDoc {
    title: String,
    text: String,
}

struct Queries {
    queries: HashMap<String, String>,
}

/// Relevance judgments: query_id -> doc_id -> relevance_score
type Qrels = HashMap<String, HashMap<String, u32>>;

// =============================================================================
// Dataset Loading
// =============================================================================

fn load_corpus(path: &Path) -> Corpus {
    let file = std::fs::File::open(path)
        .unwrap_or_else(|e| panic!("Cannot open {}: {}", path.display(), e));
    let reader = BufReader::new(file);
    let mut docs = HashMap::new();

    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line).expect("Invalid JSON in corpus");
        let id = v["_id"].as_str().expect("Missing _id").to_string();
        let title = v["title"].as_str().unwrap_or("").to_string();
        let text = v["text"].as_str().expect("Missing text").to_string();
        docs.insert(id, CorpusDoc { title, text });
    }

    Corpus { docs }
}

fn load_queries(path: &Path) -> Queries {
    let file = std::fs::File::open(path)
        .unwrap_or_else(|e| panic!("Cannot open {}: {}", path.display(), e));
    let reader = BufReader::new(file);
    let mut queries = HashMap::new();

    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line).expect("Invalid JSON in queries");
        let id = v["_id"].as_str().expect("Missing _id").to_string();
        let text = v["text"].as_str().expect("Missing text").to_string();
        queries.insert(id, text);
    }

    Queries { queries }
}

fn load_qrels(path: &Path) -> Qrels {
    let file = std::fs::File::open(path)
        .unwrap_or_else(|e| panic!("Cannot open {}: {}", path.display(), e));
    let reader = BufReader::new(file);
    let mut qrels: Qrels = HashMap::new();

    for (i, line) in reader.lines().enumerate() {
        let line = line.expect("Failed to read line");
        if i == 0 || line.trim().is_empty() {
            continue; // skip header
        }
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() < 3 {
            continue;
        }
        let query_id = parts[0].to_string();
        let doc_id = parts[1].to_string();
        let score: u32 = parts[2].parse().unwrap_or(0);
        qrels.entry(query_id).or_default().insert(doc_id, score);
    }

    qrels
}

// =============================================================================
// nDCG Computation
// =============================================================================

fn dcg(scores: &[f64], k: usize) -> f64 {
    scores
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, &s)| s / (i as f64 + 2.0).log2())
        .sum()
}

fn ndcg_at_k(ranked_doc_ids: &[String], qrel: &HashMap<String, u32>, k: usize) -> f64 {
    let gains: Vec<f64> = ranked_doc_ids
        .iter()
        .take(k)
        .map(|doc_id| *qrel.get(doc_id).unwrap_or(&0) as f64)
        .collect();
    let actual_dcg = dcg(&gains, k);

    let mut ideal_gains: Vec<f64> = qrel.values().map(|&s| s as f64).collect();
    ideal_gains.sort_by(|a, b| b.partial_cmp(a).unwrap());
    let ideal_dcg = dcg(&ideal_gains, k);

    if ideal_dcg == 0.0 {
        0.0
    } else {
        actual_dcg / ideal_dcg
    }
}

// =============================================================================
// Pyserini Baselines (from BEIR leaderboard)
// =============================================================================

fn pyserini_baseline(dataset: &str) -> Option<f64> {
    match dataset {
        "nfcorpus" => Some(0.3218),
        "scifact" => Some(0.6647),
        "arguana" => Some(0.3970),
        "scidocs" => Some(0.1490),
        "fiqa" => Some(0.2361),
        "trec-covid" => Some(0.5947),
        "quora" => Some(0.7886),
        "webis-touche2020" => Some(0.4422),
        "dbpedia-entity" => Some(0.3180),
        "nq" => Some(0.3055),
        "hotpotqa" => Some(0.6027),
        "fever" => Some(0.6513),
        "climate-fever" => Some(0.1651),
        "msmarco" => Some(0.2280),
        _ => None,
    }
}

// =============================================================================
// Dataset Download
// =============================================================================

fn ensure_dataset(data_dir: &Path, name: &str) -> PathBuf {
    let dataset_dir = data_dir.join(name);
    if dataset_dir.join("corpus.jsonl").exists() {
        return dataset_dir;
    }

    eprintln!("Downloading BEIR dataset: {} ...", name);
    let url = format!(
        "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip",
        name
    );

    let zip_path = data_dir.join(format!("{}.zip", name));
    std::fs::create_dir_all(data_dir).expect("Failed to create data dir");

    let output = std::process::Command::new("curl")
        .args(["-sL", "-o", zip_path.to_str().unwrap(), &url])
        .output()
        .expect("Failed to run curl");
    if !output.status.success() {
        panic!(
            "Failed to download {}: {}",
            url,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let output = std::process::Command::new("unzip")
        .args([
            "-q",
            "-o",
            zip_path.to_str().unwrap(),
            "-d",
            data_dir.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run unzip");
    if !output.status.success() {
        panic!(
            "Failed to unzip: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let _ = std::fs::remove_file(&zip_path);
    eprintln!("  Downloaded to {}", dataset_dir.display());
    dataset_dir
}

// =============================================================================
// Benchmark Runner
// =============================================================================

fn run_dataset(dataset_name: &str, dataset_dir: &Path, recipe: &str, k: usize) {
    // Load dataset
    let corpus = load_corpus(&dataset_dir.join("corpus.jsonl"));
    let queries = load_queries(&dataset_dir.join("queries.jsonl"));
    let qrels = load_qrels(&dataset_dir.join("qrels/test.tsv"));

    eprintln!(
        "  Corpus: {} docs, Queries: {} (with qrels: {})",
        corpus.docs.len(),
        queries.queries.len(),
        qrels.len(),
    );

    // Create temp database
    let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let db = Strata::open(temp_dir.path()).expect("Failed to open database");

    // Enable auto-embed only for recipes that need vector search
    let needs_embed =
        recipe == "hybrid" || recipe == "semantic" || recipe == "default" || recipe == "rag";
    if needs_embed {
        db.set_auto_embed(true)
            .expect("Failed to enable auto-embed");
    }

    // Index corpus (sorted by doc_id for deterministic BM25 statistics)
    let index_start = Instant::now();
    let mut indexed = 0;
    let mut sorted_docs: Vec<_> = corpus.docs.iter().collect();
    sorted_docs.sort_by_key(|(id, _)| id.as_str());
    for (doc_id, doc) in &sorted_docs {
        let text = if doc.title.is_empty() {
            doc.text.clone()
        } else {
            format!("{} {}", doc.title, doc.text)
        };
        db.kv_put(doc_id, Value::String(text.into()))
            .expect("Failed to index doc");
        indexed += 1;
        if indexed % 5000 == 0 {
            eprint!("\r  Indexed {}/{}", indexed, corpus.docs.len());
            std::io::stderr().flush().unwrap();
        }
    }
    db.flush().expect("Failed to flush");
    let index_time = index_start.elapsed();
    eprintln!(
        "\r  Indexed {} docs in {:.1}s",
        indexed,
        index_time.as_secs_f64()
    );

    // Wait for embeddings to complete if embedding was enabled
    if needs_embed {
        eprintln!("  Waiting for embeddings...");
        loop {
            let status = db.embed_status().unwrap();
            if status.pending == 0 {
                eprintln!("  Embeddings complete: {} total", status.total_embedded);
                break;
            }
            eprint!(
                "\r  Embedding: {}/{} pending",
                status.pending, status.total_queued
            );
            std::io::stderr().flush().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    }

    // Search and evaluate
    let search_start = Instant::now();
    let mut per_query_ndcg: Vec<f64> = Vec::new();
    let mut queries_run = 0;

    // Only evaluate queries that have qrels
    let eval_queries: Vec<(&String, &String)> = queries
        .queries
        .iter()
        .filter(|(qid, _)| qrels.contains_key(*qid))
        .collect();

    for (qid, query_text) in &eval_queries {
        let search_query = SearchQuery {
            query: query_text.to_string(),
            recipe: Some(
                serde_json::from_str(recipe)
                    .unwrap_or_else(|_| serde_json::Value::String(recipe.to_string())),
            ),
            k: Some(k as u64),
            precomputed_embedding: None,
            as_of: None,
            diff: None,
        };

        let result = db.search(search_query);
        let ranked_ids: Vec<String> = match result {
            Ok((hits, _stats)) => hits
                .iter()
                // BEIR convention: ignore self-matches (query_id == doc_id)
                .filter(|h| h.entity_ref.key.as_deref() != Some(*qid))
                .filter_map(|h| h.entity_ref.key.clone())
                .collect(),
            Err(e) => {
                eprintln!("  Search error for query {}: {}", qid, e);
                vec![]
            }
        };

        if let Some(qrel) = qrels.get(*qid) {
            let score = ndcg_at_k(&ranked_ids, qrel, k);
            per_query_ndcg.push(score);
        }

        queries_run += 1;
        if queries_run % 100 == 0 {
            eprint!("\r  Searched {}/{}", queries_run, eval_queries.len());
            std::io::stderr().flush().unwrap();
        }
    }

    let search_time = search_start.elapsed();
    let qps = eval_queries.len() as f64 / search_time.as_secs_f64();

    // Compute average nDCG@k
    let avg_ndcg = if per_query_ndcg.is_empty() {
        0.0
    } else {
        per_query_ndcg.iter().sum::<f64>() / per_query_ndcg.len() as f64
    };

    // Print results
    let baseline = pyserini_baseline(dataset_name);
    let delta = baseline.map(|b| avg_ndcg - b);

    println!();
    println!("============================================================");
    println!("  Dataset: {}  |  Recipe: {}", dataset_name, recipe);
    println!(
        "  Corpus: {} docs  |  Queries: {}",
        corpus.docs.len(),
        eval_queries.len()
    );
    println!("============================================================");
    println!("  nDCG@{}: {:.4}", k, avg_ndcg);
    println!(
        "  Index: {:.1}s  |  Search: {:.1}s  |  QPS: {:.0}",
        index_time.as_secs_f64(),
        search_time.as_secs_f64(),
        qps
    );
    if let Some(b) = baseline {
        let d = delta.unwrap();
        let sign = if d >= 0.0 { "+" } else { "" };
        println!("  Pyserini BM25: {:.4}  |  Delta: {}{:.4}", b, sign, d);
    }
    println!("============================================================");
    println!();
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut datasets = vec!["nfcorpus".to_string()];
    let mut recipe = "keyword".to_string();
    let mut k: usize = 10;
    let mut data_dir = PathBuf::from("datasets/beir");

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--datasets" => {
                i += 1;
                datasets = args[i].split(',').map(|s| s.trim().to_string()).collect();
            }
            "--recipe" => {
                i += 1;
                recipe = args[i].clone();
            }
            // Backward compat: --mode maps to --recipe
            "--mode" => {
                i += 1;
                recipe = args[i].clone();
            }
            "--k" => {
                i += 1;
                k = args[i].parse().expect("Invalid k");
            }
            "--data-dir" => {
                i += 1;
                data_dir = PathBuf::from(&args[i]);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                eprintln!("Usage: beir [--datasets nfcorpus,scifact] [--recipe keyword|hybrid] [--k 10] [--data-dir path]");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    eprintln!(
        "BEIR Benchmark: datasets={:?}, recipe={}, k={}",
        datasets, recipe, k
    );
    eprintln!();

    for name in &datasets {
        let dataset_dir = ensure_dataset(&data_dir, name);
        run_dataset(name, &dataset_dir, &recipe, k);
    }
}
