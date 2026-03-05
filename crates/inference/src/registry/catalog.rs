//! Static catalog of known models with HuggingFace download metadata.

use super::{CatalogEntry, ModelTask, QuantVariant};

/// Static catalog of all known models.
pub static CATALOG: &[CatalogEntry] = &[
    // ===== Embedding Models =====
    CatalogEntry {
        name: "miniLM",
        aliases: &["all-minilm"],
        task: ModelTask::Embed,
        hf_repo: "leliuga/all-MiniLM-L6-v2-GGUF",
        default_quant: "f16",
        variants: &[QuantVariant {
            name: "f16",
            hf_file: "all-MiniLM-L6-v2.F16.gguf",
            size_bytes: 45_000_000,
            sha256: None,
        }],
        architecture: "bert",
        embedding_dim: 384,
    },
    CatalogEntry {
        name: "nomic-embed",
        aliases: &["nomic", "nomic-embed-text"],
        task: ModelTask::Embed,
        hf_repo: "nomic-ai/nomic-embed-text-v1.5-GGUF",
        default_quant: "q8_0",
        variants: &[QuantVariant {
            name: "q8_0",
            hf_file: "nomic-embed-text-v1.5.Q8_0.gguf",
            size_bytes: 260_000_000,
            sha256: None,
        }],
        architecture: "nomic-bert",
        embedding_dim: 768,
    },
    CatalogEntry {
        name: "bge-m3",
        aliases: &["bge"],
        task: ModelTask::Embed,
        hf_repo: "second-state/BGE-M3-GGUF",
        default_quant: "q8_0",
        variants: &[QuantVariant {
            name: "q8_0",
            hf_file: "bge-m3-Q8_0.gguf",
            size_bytes: 1_200_000_000,
            sha256: None,
        }],
        architecture: "xlm-roberta",
        embedding_dim: 1024,
    },
    CatalogEntry {
        name: "gemma-embed",
        aliases: &["embedding-gemma"],
        task: ModelTask::Embed,
        hf_repo: "lm-kit/embedding-gemma-300M-GGUF",
        default_quant: "q8_0",
        variants: &[QuantVariant {
            name: "q8_0",
            hf_file: "embedding-gemma-300M-Q8_0.gguf",
            size_bytes: 320_000_000,
            sha256: None,
        }],
        architecture: "gemma3",
        embedding_dim: 768,
    },
    // ===== Generation Models =====
    CatalogEntry {
        name: "gpt2",
        aliases: &[],
        task: ModelTask::Generate,
        hf_repo: "QuantFactory/gpt2-GGUF",
        default_quant: "q8_0",
        variants: &[
            QuantVariant {
                name: "q4_k_m",
                hf_file: "gpt2.Q4_K_M.gguf",
                size_bytes: 113_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q8_0",
                hf_file: "gpt2.Q8_0.gguf",
                size_bytes: 178_000_000,
                sha256: None,
            },
        ],
        architecture: "gpt2",
        embedding_dim: 0,
    },
    CatalogEntry {
        name: "tinyllama",
        aliases: &["tiny-llama"],
        task: ModelTask::Generate,
        hf_repo: "TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF",
        default_quant: "q4_k_m",
        variants: &[
            QuantVariant {
                name: "q4_k_m",
                hf_file: "tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf",
                size_bytes: 670_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q8_0",
                hf_file: "tinyllama-1.1b-chat-v1.0.Q8_0.gguf",
                size_bytes: 1_100_000_000,
                sha256: None,
            },
        ],
        architecture: "llama",
        embedding_dim: 0,
    },
    CatalogEntry {
        name: "qwen3:1.7b",
        aliases: &["qwen3-1.7b"],
        task: ModelTask::Generate,
        hf_repo: "Qwen/Qwen3-1.7B-GGUF",
        default_quant: "q4_k_m",
        variants: &[
            QuantVariant {
                name: "q4_k_m",
                hf_file: "Qwen3-1.7B-Q4_K_M.gguf",
                size_bytes: 1_100_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q8_0",
                hf_file: "Qwen3-1.7B-Q8_0.gguf",
                size_bytes: 2_000_000_000,
                sha256: None,
            },
        ],
        architecture: "qwen3",
        embedding_dim: 0,
    },
    CatalogEntry {
        name: "gemma3:1b",
        aliases: &["gemma3-1b", "gemma-3-1b"],
        task: ModelTask::Generate,
        hf_repo: "unsloth/gemma-3-1b-it-GGUF",
        default_quant: "q4_k_m",
        variants: &[
            QuantVariant {
                name: "q4_k_m",
                hf_file: "gemma-3-1b-it-Q4_K_M.gguf",
                size_bytes: 780_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q8_0",
                hf_file: "gemma-3-1b-it-Q8_0.gguf",
                size_bytes: 1_400_000_000,
                sha256: None,
            },
        ],
        architecture: "gemma3",
        embedding_dim: 0,
    },
    CatalogEntry {
        name: "phi3.5",
        aliases: &["phi-3.5", "phi3.5-mini"],
        task: ModelTask::Generate,
        hf_repo: "bartowski/Phi-3.5-mini-instruct-GGUF",
        default_quant: "q4_k_m",
        variants: &[
            QuantVariant {
                name: "q4_k_m",
                hf_file: "Phi-3.5-mini-instruct-Q4_K_M.gguf",
                size_bytes: 2_400_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q8_0",
                hf_file: "Phi-3.5-mini-instruct-Q8_0.gguf",
                size_bytes: 4_100_000_000,
                sha256: None,
            },
        ],
        architecture: "phi3",
        embedding_dim: 0,
    },
    CatalogEntry {
        name: "llama3.1:8b",
        aliases: &["llama3.1-8b", "llama-3.1-8b"],
        task: ModelTask::Generate,
        hf_repo: "bartowski/Meta-Llama-3.1-8B-Instruct-GGUF",
        default_quant: "q4_k_m",
        variants: &[
            QuantVariant {
                name: "q4_k_m",
                hf_file: "Meta-Llama-3.1-8B-Instruct-Q4_K_M.gguf",
                size_bytes: 4_600_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q5_k_m",
                hf_file: "Meta-Llama-3.1-8B-Instruct-Q5_K_M.gguf",
                size_bytes: 5_300_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q6_k",
                hf_file: "Meta-Llama-3.1-8B-Instruct-Q6_K.gguf",
                size_bytes: 6_100_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q8_0",
                hf_file: "Meta-Llama-3.1-8B-Instruct-Q8_0.gguf",
                size_bytes: 8_100_000_000,
                sha256: None,
            },
        ],
        architecture: "llama",
        embedding_dim: 0,
    },
    CatalogEntry {
        name: "qwen3:8b",
        aliases: &["qwen3-8b"],
        task: ModelTask::Generate,
        hf_repo: "Qwen/Qwen3-8B-GGUF",
        default_quant: "q4_k_m",
        variants: &[
            QuantVariant {
                name: "q4_k_m",
                hf_file: "Qwen3-8B-Q4_K_M.gguf",
                size_bytes: 4_700_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q5_k_m",
                hf_file: "Qwen3-8B-Q5_K_M.gguf",
                size_bytes: 5_400_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q6_k",
                hf_file: "Qwen3-8B-Q6_K.gguf",
                size_bytes: 6_200_000_000,
                sha256: None,
            },
            QuantVariant {
                name: "q8_0",
                hf_file: "Qwen3-8B-Q8_0.gguf",
                size_bytes: 8_200_000_000,
                sha256: None,
            },
        ],
        architecture: "qwen3",
        embedding_dim: 0,
    },
];

/// Find a catalog entry by exact name or alias (case-insensitive).
pub fn find_entry(name: &str) -> Option<&'static CatalogEntry> {
    let lower = name.to_lowercase();
    CATALOG.iter().find(|e| {
        e.name.to_lowercase() == lower || e.aliases.iter().any(|a| a.to_lowercase() == lower)
    })
}

/// Find a catalog entry from colon-split parts.
///
/// Supports:
/// - `["miniLM"]` → exact match
/// - `["qwen3", "8b"]` → tries "qwen3:8b" as combined name
/// - `["qwen3", "8b", "q6_k"]` → tries "qwen3:8b" as name, "q6_k" as quant
pub fn find_entry_by_parts<'a>(
    parts: &[&'a str],
) -> Option<(&'static CatalogEntry, Option<&'a str>)> {
    match parts.len() {
        0 => None,
        1 => find_entry(parts[0]).map(|e| (e, None)),
        2 => {
            // Treat empty trailing part (from "name:") as single-part lookup
            if parts[1].is_empty() {
                return find_entry(parts[0]).map(|e| (e, None));
            }
            // Try "family:size" as combined name first
            let combined = format!("{}:{}", parts[0], parts[1]);
            if let Some(e) = find_entry(&combined) {
                return Some((e, None));
            }
            // Fall back to treating parts[1] as a quant of a single-part name
            find_entry(parts[0]).map(|e| (e, Some(parts[1])))
        }
        3 => {
            // Treat empty trailing part (from "name:size:") as two-part lookup
            if parts[2].is_empty() {
                let combined = format!("{}:{}", parts[0], parts[1]);
                if let Some(e) = find_entry(&combined) {
                    return Some((e, None));
                }
                return find_entry(parts[0]).map(|e| (e, Some(parts[1])));
            }
            let combined = format!("{}:{}", parts[0], parts[1]);
            find_entry(&combined).map(|e| (e, Some(parts[2])))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_entry_exact() {
        let entry = find_entry("miniLM").unwrap();
        assert_eq!(entry.name, "miniLM");
        assert_eq!(entry.task, ModelTask::Embed);
        assert_eq!(entry.embedding_dim, 384);
    }

    #[test]
    fn find_entry_case_insensitive() {
        assert!(find_entry("MINILM").is_some());
        assert!(find_entry("minilm").is_some());
        assert!(find_entry("MiniLM").is_some());
    }

    #[test]
    fn find_entry_alias() {
        let entry = find_entry("nomic").unwrap();
        assert_eq!(entry.name, "nomic-embed");
    }

    #[test]
    fn find_entry_unknown() {
        assert!(find_entry("unknown-model").is_none());
    }

    #[test]
    fn find_entry_colon_name() {
        let entry = find_entry("qwen3:8b").unwrap();
        assert_eq!(entry.name, "qwen3:8b");
        assert_eq!(entry.task, ModelTask::Generate);
    }

    #[test]
    fn find_entry_by_parts_single() {
        let (entry, quant) = find_entry_by_parts(&["miniLM"]).unwrap();
        assert_eq!(entry.name, "miniLM");
        assert!(quant.is_none());
    }

    #[test]
    fn find_entry_by_parts_two_combined() {
        let (entry, quant) = find_entry_by_parts(&["qwen3", "8b"]).unwrap();
        assert_eq!(entry.name, "qwen3:8b");
        assert!(quant.is_none());
    }

    #[test]
    fn find_entry_by_parts_two_with_quant_fallback() {
        let (entry, quant) = find_entry_by_parts(&["tinyllama", "q8_0"]).unwrap();
        assert_eq!(entry.name, "tinyllama");
        assert_eq!(quant, Some("q8_0"));
    }

    #[test]
    fn find_entry_by_parts_three() {
        let (entry, quant) = find_entry_by_parts(&["qwen3", "8b", "q6_k"]).unwrap();
        assert_eq!(entry.name, "qwen3:8b");
        assert_eq!(quant, Some("q6_k"));
    }

    #[test]
    fn find_entry_by_parts_empty() {
        assert!(find_entry_by_parts(&[]).is_none());
    }

    #[test]
    fn find_entry_by_parts_too_many() {
        assert!(find_entry_by_parts(&["a", "b", "c", "d"]).is_none());
    }

    #[test]
    fn all_entries_have_valid_default_quant() {
        for entry in CATALOG {
            let found = entry.variants.iter().any(|v| v.name == entry.default_quant);
            assert!(
                found,
                "Catalog entry '{}' has default_quant '{}' but no matching variant",
                entry.name, entry.default_quant
            );
        }
    }

    #[test]
    fn generation_models_have_zero_embedding_dim() {
        for entry in CATALOG {
            if entry.task == ModelTask::Generate {
                assert_eq!(
                    entry.embedding_dim, 0,
                    "Generation model '{}' should have embedding_dim=0",
                    entry.name
                );
            }
        }
    }

    #[test]
    fn embedding_models_have_nonzero_embedding_dim() {
        for entry in CATALOG {
            if entry.task == ModelTask::Embed {
                assert!(
                    entry.embedding_dim > 0,
                    "Embedding model '{}' should have embedding_dim > 0",
                    entry.name
                );
            }
        }
    }

    #[test]
    fn names_and_aliases_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for entry in CATALOG {
            let lower_name = entry.name.to_lowercase();
            assert!(
                seen.insert(lower_name.clone()),
                "Duplicate catalog name: '{}'",
                entry.name
            );
            for alias in entry.aliases {
                let lower_alias = alias.to_lowercase();
                assert!(
                    seen.insert(lower_alias.clone()),
                    "Duplicate alias '{}' in entry '{}'",
                    alias,
                    entry.name
                );
            }
        }
    }

    #[test]
    fn all_entries_have_nonempty_variants() {
        for entry in CATALOG {
            assert!(
                !entry.variants.is_empty(),
                "Catalog entry '{}' has no variants",
                entry.name
            );
        }
    }

    #[test]
    fn variant_filenames_end_with_gguf() {
        for entry in CATALOG {
            for variant in entry.variants {
                assert!(
                    variant.hf_file.ends_with(".gguf"),
                    "Variant '{}' of '{}' doesn't end with .gguf: {}",
                    variant.name,
                    entry.name,
                    variant.hf_file
                );
            }
        }
    }

    #[test]
    fn find_entry_empty_string() {
        assert!(find_entry("").is_none());
    }

    #[test]
    fn find_entry_whitespace_not_matched() {
        assert!(find_entry(" miniLM ").is_none());
        assert!(find_entry("  ").is_none());
    }

    #[test]
    fn find_entry_case_insensitive_colon_name() {
        let entry = find_entry("QWEN3:8B").unwrap();
        assert_eq!(entry.name, "qwen3:8b");
    }

    #[test]
    fn find_entry_alias_case_insensitive() {
        let entry = find_entry("TINY-LLAMA").unwrap();
        assert_eq!(entry.name, "tinyllama");
    }

    #[test]
    fn find_entry_by_parts_two_unknown_combined_unknown_single() {
        assert!(find_entry_by_parts(&["unknown", "thing"]).is_none());
    }

    #[test]
    fn find_entry_by_parts_three_unknown_combined() {
        assert!(find_entry_by_parts(&["unknown", "thing", "q8_0"]).is_none());
    }

    #[test]
    fn all_entries_have_nonempty_hf_repo() {
        for entry in CATALOG {
            assert!(
                !entry.hf_repo.is_empty(),
                "Catalog entry '{}' has empty hf_repo",
                entry.name
            );
        }
    }

    #[test]
    fn all_entries_have_nonempty_architecture() {
        for entry in CATALOG {
            assert!(
                !entry.architecture.is_empty(),
                "Catalog entry '{}' has empty architecture",
                entry.name
            );
        }
    }

    #[test]
    fn all_variants_have_nonzero_size() {
        for entry in CATALOG {
            for variant in entry.variants {
                assert!(
                    variant.size_bytes > 0,
                    "Variant '{}' of '{}' has zero size_bytes",
                    variant.name,
                    entry.name
                );
            }
        }
    }

    #[test]
    fn variant_filenames_are_unique_across_catalog() {
        let mut seen = std::collections::HashSet::new();
        for entry in CATALOG {
            for variant in entry.variants {
                assert!(
                    seen.insert(variant.hf_file),
                    "Duplicate hf_file '{}' in entry '{}' — would cause file collision in models dir",
                    variant.hf_file, entry.name
                );
            }
        }
    }

    #[test]
    fn variant_names_are_unique_within_entry() {
        for entry in CATALOG {
            let mut seen = std::collections::HashSet::new();
            for variant in entry.variants {
                assert!(
                    seen.insert(variant.name),
                    "Duplicate variant name '{}' in entry '{}'",
                    variant.name,
                    entry.name
                );
            }
        }
    }

    #[test]
    fn find_entry_by_parts_trailing_empty_part_ignored() {
        // "tinyllama:" splits to ["tinyllama", ""] — should match tinyllama, no quant
        let (entry, quant) = find_entry_by_parts(&["tinyllama", ""]).unwrap();
        assert_eq!(entry.name, "tinyllama");
        assert!(
            quant.is_none(),
            "empty trailing part should not be treated as quant"
        );
    }

    #[test]
    fn find_entry_by_parts_three_trailing_empty_part_ignored() {
        // "qwen3:8b:" splits to ["qwen3", "8b", ""] — should match qwen3:8b, no quant
        let (entry, quant) = find_entry_by_parts(&["qwen3", "8b", ""]).unwrap();
        assert_eq!(entry.name, "qwen3:8b");
        assert!(
            quant.is_none(),
            "empty trailing part should not be treated as quant"
        );
    }

    #[test]
    fn find_entry_by_parts_leading_empty_part() {
        // ":miniLM" splits to ["", "miniLM"]
        // Tries ":miniLM" combined → no match → tries find_entry("") → no match → None
        assert!(find_entry_by_parts(&["", "miniLM"]).is_none());
    }
}
