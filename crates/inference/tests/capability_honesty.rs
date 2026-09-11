//! The surface may not claim an ability this binary does not have (#3124).
//!
//! A released binary is built `native + cloud` — no `local` feature — and so
//! cannot run any of the eleven catalogued local models. It nonetheless
//! reported `can_embed: true` beside `provider_feature_enabled: false` in the
//! same object, listed every local model as present, and then refused the
//! operation with one of seven differently-worded messages, none of which said
//! what to do.
//!
//! These tests hold the surface to one law: **a capability flag is a promise
//! this binary can keep right now.**
//!
//! They are written to be correct under either feature set — with `local` on,
//! the claims become true and the assertions follow — so the same file guards
//! the source build and the released one.

use strata_inference::{Availability, AvailabilityKind, InferenceRuntime, InferenceRuntimeConfig};

const LOCAL_BUILT_IN: bool = cfg!(feature = "local");

fn runtime() -> InferenceRuntime {
    InferenceRuntime::new(InferenceRuntimeConfig::default())
}

/// No `can_*` may be true while the feature that would execute it is absent.
/// This is the contradiction #3124 reported, stated as a law.
#[test]
fn capability_never_claims_more_than_the_build_can_do() {
    let runtime = runtime();
    // One model per task, so every `can_*` has a case where the model itself
    // supports the operation — otherwise the feature check is the only thing
    // making the flag false and the rest of the expression is unobservable.
    // No catalogued model ranks (#3045); a present GGUF path claims every
    // local ability including rank, so it is the rank case.
    let dir = tempfile::tempdir().unwrap();
    let reranker = dir.path().join("reranker.gguf");
    std::fs::write(&reranker, b"gguf").unwrap();
    for spec in [
        "miniLM",                   // embed
        "gpt2",                     // generate
        "tinyllama",                // generate
        "nomic-embed",              // embed
        reranker.to_str().unwrap(), // rank (a path claims every local ability)
    ] {
        let Ok(capability) = runtime.capability(spec) else {
            continue; // not in this build's catalog; nothing claimed, nothing to check
        };
        if capability.provider_feature_enabled {
            continue;
        }
        assert!(
            !capability.can_generate
                && !capability.can_embed
                && !capability.can_tokenize
                && !capability.can_rank,
            "{spec} claims an ability while its provider feature is off: {capability:?}"
        );
    }
}

/// The specific case from the issue: `capability miniLM` on a released binary.
#[test]
fn a_local_embedding_model_reports_what_this_build_can_do() {
    let capability = runtime()
        .capability("miniLM")
        .expect("miniLM is catalogued");

    assert_eq!(
        capability.can_embed, LOCAL_BUILT_IN,
        "can_embed must track whether this binary can actually embed"
    );
    assert_eq!(capability.can_tokenize, LOCAL_BUILT_IN);
    assert_eq!(capability.provider_feature_enabled, LOCAL_BUILT_IN);

    // The model's inherent shape stays reportable either way: it is still an
    // embedding model with a known dimension, which is what the catalog is for.
    assert_eq!(capability.embedding_dim, 384);
}

/// A cloud model in the default build is genuinely usable, so its flags stay
/// true — the fix must not make every flag false.
#[test]
fn cloud_capability_is_unaffected_when_its_feature_is_on() {
    let capability = runtime()
        .capability("openai:gpt-4o-mini")
        .expect("cloud spec parses");
    assert_eq!(capability.can_generate, cfg!(feature = "openai"));
    assert_eq!(capability.can_embed, cfg!(feature = "openai"));
    assert!(capability.requires_api_key);
    assert!(capability.requires_network);
}

/// The catalog says whether this binary can run what it lists.
#[test]
fn every_listed_model_declares_whether_it_can_run_here() {
    for model in runtime().list_models() {
        assert_eq!(
            model.runnable, LOCAL_BUILT_IN,
            "{} must declare runnability honestly; is_local ({}) is about the \
             file on disk, not about execution",
            model.name, model.is_local
        );
    }
}

/// Every local refusal uses the one phrasing, and that phrasing tells the user
/// what to do. Seven different sentences for one fact was the reported defect.
#[test]
#[cfg(not(feature = "local"))]
fn local_refusals_share_one_actionable_phrasing() {
    use strata_inference::EmbedRequest;

    let runtime = runtime();
    // Every local entry point, so none can quietly start succeeding or drift to
    // its own wording. `detokenize` had no test at all before #3124.
    let refusals = [
        runtime
            .embed(
                "miniLM",
                &EmbedRequest {
                    text: "hello".to_owned(),
                },
            )
            .expect_err("embedding refuses without the local feature")
            .to_string(),
        runtime
            .tokenize("miniLM", "hello", false)
            .expect_err("tokenizing refuses without the local feature")
            .to_string(),
        runtime
            .detokenize("miniLM", &[1, 2, 3])
            .expect_err("detokenizing refuses without the local feature")
            .to_string(),
    ];

    for message in &refusals {
        // Both ways forward, and both must be commands the reader can run —
        // the reader here is usually a coding agent with no Rust toolchain.
        assert!(
            message.contains("strata inference install-local"),
            "a refusal must name the command that adds local execution: {message}"
        );
        assert!(
            message.contains("openai:"),
            "a refusal must name the alternative that needs no install: {message}"
        );
        assert!(
            !message.contains("cargo install"),
            "a refusal must not send the reader to a source build: {message}"
        );
    }
}

/// A build without downloading refuses `pull` the same actionable way, and
/// keeps the code that has always meant "downloads are off".
#[test]
#[cfg(not(feature = "download"))]
fn a_build_without_downloading_says_so_and_what_to_do_instead() {
    // An empty models directory: a model already on disk pulls as a no-op
    // success in every build, so the refusal needs one that is not.
    let models_dir = tempfile::tempdir().unwrap();
    let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: Some(models_dir.path().to_path_buf()),
        network_enabled: true,
    });
    let error = runtime
        .pull_model("miniLM")
        .expect_err("pull refuses without the download feature");
    assert_eq!(error.code(), "inference.download_disabled");
    let message = error.to_string();
    assert!(
        message.contains("strata inference install-local"),
        "the refusal must name the command that adds local execution: {message}"
    );
    assert!(
        message.contains("openai:"),
        "the refusal must name the alternative that needs no install: {message}"
    );
    assert!(
        !message.contains("cargo install"),
        "the refusal must not send the reader to a source build: {message}"
    );
}

/// The refusal codes are unchanged by the rewording.
///
/// `InferenceError::code()` classifies `NotSupported` by substring-matching the
/// human message: "provider" yields `unsupported_provider`, "download" yields
/// `download_disabled`, everything else `unsupported_operation`. Improving a
/// message can therefore silently reclassify an error, which is a trap this
/// change walked into once already. Until that design is fixed, this pins it.
#[test]
#[cfg(not(feature = "local"))]
fn inference_refusals_keep_their_codes() {
    use strata_inference::EmbedRequest;

    let runtime = runtime();
    assert_eq!(
        runtime
            .embed(
                "miniLM",
                &EmbedRequest {
                    text: "hello".to_owned()
                }
            )
            .expect_err("embedding refuses")
            .code(),
        "inference.unsupported_operation"
    );
    assert_eq!(
        runtime
            .tokenize("miniLM", "hello", false)
            .expect_err("tokenizing refuses")
            .code(),
        "inference.unsupported_operation"
    );
}

/// D11: `status` answers "will this work" before anything is attempted.
#[test]
fn status_reports_the_build_and_every_provider() {
    let status = runtime().status();

    assert_eq!(status.local_execution, LOCAL_BUILT_IN);
    assert_eq!(
        status.providers.len(),
        4,
        "every provider is reported, including the ones that are not usable"
    );

    // `ready` and `feature_enabled` stated as equalities, not implications.
    //
    // Every assertion below this used the form "if ready, then ...", which two
    // mutants satisfied by making `ready` false for everyone — a status page
    // reporting nothing usable passes an implication-only test perfectly.
    for provider in &status.providers {
        let expected_feature = match provider.provider {
            // A provider is enabled if it can do either job. Anthropic is the
            // case that distinguishes `||` from `&&`: it generates but has no
            // embedding support at all, so under `&&` it would report disabled
            // while being perfectly usable for generation.
            strata_inference::ProviderKind::Anthropic => cfg!(feature = "anthropic"),
            strata_inference::ProviderKind::OpenAI => cfg!(feature = "openai"),
            strata_inference::ProviderKind::Google => cfg!(feature = "google"),
            strata_inference::ProviderKind::Local => LOCAL_BUILT_IN,
        };
        assert_eq!(
            provider.feature_enabled, expected_feature,
            "{:?} must report the features this build has",
            provider.provider
        );
        assert_eq!(
            provider.ready,
            provider.feature_enabled && (provider.key_present || !provider.requires_api_key),
            "{:?} is ready exactly when it is built in and has whatever key it needs",
            provider.provider
        );
    }

    // And the keyless provider is ready on nothing but its feature — the case
    // where `key_present || !requires_api_key` carries the whole decision.
    let local = status
        .providers
        .iter()
        .find(|provider| provider.provider == strata_inference::ProviderKind::Local)
        .expect("local is reported");
    assert!(!local.key_present, "local never has a key");
    assert_eq!(
        local.ready, LOCAL_BUILT_IN,
        "local is ready whenever it is built in, with no key involved"
    );

    for provider in &status.providers {
        // `ready` is never a claim the build cannot keep — the same law the
        // capability flags follow.
        if provider.ready {
            assert!(
                provider.feature_enabled,
                "{:?} reports ready without being compiled in",
                provider.provider
            );
            assert!(
                provider.key_present || !provider.requires_api_key,
                "{:?} reports ready with no key",
                provider.provider
            );
        }
        // A cloud provider always names the variable to set, whether or not a
        // key is present — otherwise "no key" is not actionable.
        assert_eq!(
            provider.key_env_var.is_some(),
            provider.requires_api_key,
            "{:?} must name its key variable exactly when it needs one",
            provider.provider
        );
        // A source is only reported when a key was actually found.
        assert_eq!(provider.key_present, provider.key_source.is_some());
    }

    assert!(status.models_catalogued > 0, "the catalog is not empty");
    assert!(status.models_downloaded <= status.models_catalogued);

    // Exactly the local provider needs no key. Asserting this by provider
    // identity — rather than only that key_env_var agrees with the flag —
    // is what catches the flag being inverted, since both are the same
    // expression and would flip together.
    let keyless: Vec<_> = status
        .providers
        .iter()
        .filter(|provider| !provider.requires_api_key)
        .map(|provider| provider.provider)
        .collect();
    assert_eq!(
        keyless,
        vec![strata_inference::ProviderKind::Local],
        "only local runs without a key"
    );

    // The remedy is present exactly when local execution is absent. Without
    // this the field could silently become None and the CLI would print
    // nothing where it promises the way forward.
    assert_eq!(
        status.local_remedy.is_some(),
        !status.local_execution,
        "a build lacking local execution must carry the remedy, and one with it must not"
    );
}

/// `models_downloaded` counts what `models local` lists, judged the way
/// `resolve` judges: a model with any non-empty variant on disk counts once;
/// a zero-length leftover does not. The count used to walk `list_available`,
/// which looked only at the default quant and called any existing path
/// downloaded — so a model held as a non-default quant went uncounted while
/// an interrupted download that `resolve` would refuse was counted.
#[test]
fn downloaded_count_matches_what_resolves_and_what_models_local_lists() {
    let models_dir = tempfile::tempdir().unwrap();
    let runtime = InferenceRuntime::new(InferenceRuntimeConfig {
        models_dir: Some(models_dir.path().to_path_buf()),
        ..InferenceRuntimeConfig::default()
    });
    let catalog_len = strata_inference::registry::catalog::CATALOG.len();
    // Where a variant's artifact lives, and whether it is the model's default.
    let variant_file = |name: &str, quant: &str| {
        let entry = strata_inference::registry::catalog::find_entry(name).unwrap();
        let variant = entry.variants.iter().find(|v| v.name == quant).unwrap();
        (
            models_dir.path().join(variant.hf_file),
            quant == entry.default_quant,
        )
    };

    // An empty directory holds nothing.
    let status = runtime.status();
    assert_eq!(status.models_downloaded, 0);
    assert_eq!(status.models_catalogued, catalog_len);
    assert!(runtime.list_local_models().is_empty());

    // A non-default quant is a downloaded model like any other.
    let (tinyllama_q8, is_default) = variant_file("tinyllama", "q8_0");
    assert!(
        !is_default,
        "the case is a model held only as a non-default quant"
    );
    std::fs::write(&tinyllama_q8, b"gguf").unwrap();
    // An interrupted download of the default quant is not one.
    let (minilm_default, is_default) = variant_file("miniLM", "f16");
    assert!(
        is_default,
        "the case is a zero-length file where `models list` looks"
    );
    std::fs::write(&minilm_default, b"").unwrap();

    let status = runtime.status();
    let local: Vec<String> = runtime
        .list_local_models()
        .into_iter()
        .map(|info| info.name)
        .collect();
    assert_eq!(local, vec!["tinyllama".to_owned()]);
    assert_eq!(
        status.models_downloaded,
        local.len(),
        "the status count is the `models local` listing"
    );
    assert_eq!(status.models_catalogued, catalog_len);

    // `models list` says the same of each: the zero-length default is not
    // "ready", and tinyllama's default quant (absent) is not either.
    let listed = |name: &str| {
        runtime
            .list_models()
            .into_iter()
            .find(|info| info.name == name)
            .unwrap()
            .is_local
    };
    assert!(!listed("miniLM"), "a zero-length file is not downloaded");
    assert!(
        !listed("tinyllama"),
        "the default quant is what `models list` reports on"
    );

    // The same models resolve as downloaded — and only they.
    let tinyllama = runtime.resolve("tinyllama:q8_0", None).unwrap();
    assert_eq!(tinyllama.availability, Availability::Ready);
    assert_eq!(tinyllama.local_path(), Some(tinyllama_q8.as_path()));
    let minilm = runtime.resolve("miniLM", None).unwrap();
    assert!(
        matches!(minilm.availability, Availability::NotDownloaded { .. }),
        "{:?}",
        minilm.availability
    );
    assert_eq!(
        minilm.require_ready().unwrap_err().code(),
        "inference.missing_model"
    );
}

/// Whatever the ambient environment holds, a runtime built with `new` reads
/// the environment, and a reported source is the variable's name. The
/// value-never-leaks property itself is pinned by
/// `key_source_reports_the_place_never_the_value`, which is a pure function
/// and so needs no environment mutation — mutating the environment here would
/// race every other test in this binary.
#[test]
fn a_reported_key_source_is_a_variable_name() {
    for provider in runtime().status().providers {
        if let Some(source) = provider.key_source.as_deref() {
            assert_eq!(
                Some(source),
                provider.key_env_var.as_deref(),
                "{:?} must report the variable it read, not anything else",
                provider.provider
            );
            assert!(
                std::env::var_os(source).is_some(),
                "the reported source names a variable that is actually set"
            );
        }
    }
}

/// The spec forms the registry loads — an alias, a quant suffix, a different
/// case — must report the same model. `capability` matched the spec against
/// catalog names itself and so reported `nomic` and `miniLM:f16` as unknown
/// (no task, dimension 0) while `strata inference embed` accepted both.
#[test]
fn capability_resolves_the_spec_forms_the_registry_accepts() {
    let runtime = runtime();
    let canonical = runtime.capability("miniLM").expect("catalogued");
    assert_eq!(canonical.embedding_dim, 384);

    // A quant suffix needs the `local:` prefix: a bare `miniLM:f16` parses as
    // provider `miniLM`.
    for spec in [
        "MINILM",
        "all-minilm",
        "local:miniLM:f16",
        "local:all-minilm:f16",
    ] {
        let capability = runtime.capability(spec).expect("a catalogued form");
        assert_eq!(
            capability.embedding_dim, 384,
            "{spec} must resolve to the same catalog entry as miniLM"
        );
        assert_eq!(
            capability.can_embed, canonical.can_embed,
            "{spec} must report the same abilities as miniLM"
        );
        assert_eq!(capability.can_tokenize, canonical.can_tokenize, "{spec}");
        assert!(!capability.can_generate, "{spec} is an embedding model");
    }
}

/// A local spec the catalog does not know cannot be loaded, so nothing may be
/// claimed for it in any build. Before, it was reported as able to generate
/// and tokenize — abilities of a file that does not exist.
#[test]
fn an_uncatalogued_local_spec_claims_nothing() {
    let capability = runtime()
        .capability("local:no-such-model")
        .expect("capability is metadata-only and does not need the model");
    assert!(
        !capability.can_generate
            && !capability.can_tokenize
            && !capability.can_embed
            && !capability.can_rank,
        "an unknown local model must not claim abilities: {capability:?}"
    );
    assert_eq!(capability.embedding_dim, 0);
}

/// A caller-supplied GGUF file is loaded for whichever task is asked of it —
/// the resolver returns it ready for every use, and the load decides — so
/// `capability` claims for it every local ability this build has (#3303).
/// Before, a present path claimed nothing, like a name the catalog does not
/// know, while `generate`, `embed`, `rank` and `tokenize` all ran it.
#[test]
fn a_present_gguf_path_claims_every_local_ability_the_build_has() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("model.gguf");
    std::fs::write(&path, b"gguf").unwrap();

    let capability = runtime()
        .capability(path.to_str().unwrap())
        .expect("capability is metadata-only and does not read the file");

    assert_eq!(capability.availability, AvailabilityKind::Ready);
    assert_eq!(capability.can_generate, LOCAL_BUILT_IN, "{capability:?}");
    assert_eq!(capability.can_tokenize, LOCAL_BUILT_IN, "{capability:?}");
    assert_eq!(capability.can_embed, LOCAL_BUILT_IN, "{capability:?}");
    assert_eq!(capability.can_rank, LOCAL_BUILT_IN, "{capability:?}");
    assert_eq!(capability.provider_feature_enabled, LOCAL_BUILT_IN);
    assert_eq!(
        capability.embedding_dim, 0,
        "nothing is known about the file's shape until it is loaded"
    );
}

/// The same path with no file behind it names nothing (Q5): nothing is
/// claimed for it, in any build, exactly as for an uncatalogued name.
#[test]
fn an_absent_gguf_path_claims_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent.gguf");

    let capability = runtime()
        .capability(path.to_str().unwrap())
        .expect("capability answers for a missing file");

    assert_eq!(capability.availability, AvailabilityKind::PathMissing);
    assert!(
        !capability.can_generate
            && !capability.can_tokenize
            && !capability.can_embed
            && !capability.can_rank,
        "a path with no file behind it must not claim abilities: {capability:?}"
    );
}
