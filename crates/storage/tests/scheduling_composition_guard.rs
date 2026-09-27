//! TCP4.14: enqueue-mirrors-execution as an enforced contract.
//!
//! #2792 and #2798 were both the fork lifecycle and checkpoint scheduling
//! consulting different predicate subsets for the same state. The registry
//! (`checkpoint_structural_deferral`) is the single authority; visibility
//! makes a divergent consumer uncompilable, and this guard keeps the
//! *shape* pinned: the raw predicates stay private to the registry's file,
//! and every known scheduling surface references the registry.

use std::path::Path;

fn source(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

const PREDICATES: [&str; 2] = [
    "non_seeded_branch_has_durable_base",
    "any_branch_holds_unmaterialized_inherited_layers",
];

#[test]
fn structural_deferral_predicates_stay_private_to_the_registry() {
    let checkpoint = source("src/lifecycle/checkpoint.rs");
    for predicate in PREDICATES {
        assert!(
            !checkpoint.contains(&format!("pub(crate) fn {predicate}"))
                && !checkpoint.contains(&format!("pub fn {predicate}")),
            "{predicate} must stay private: a public predicate lets a new \
             scheduling site consult a divergent subset of the registry"
        );
        assert!(
            checkpoint.contains(&format!("fn {predicate}")),
            "{predicate} moved — update this guard alongside the registry"
        );
    }
}

#[test]
fn scheduling_sites_consult_the_registry_not_raw_predicates() {
    // The maintenance scheduler (enqueue evaluation, pacing helper, and the
    // background executor's guard arm) must reference the registry and must
    // not name the raw predicates at all.
    let maintenance = source("src/lifecycle/durable/maintenance.rs");
    for predicate in PREDICATES {
        assert!(
            !maintenance.contains(&format!("{predicate}(")),
            "durable/maintenance.rs calls raw predicate {predicate} — route \
             it through checkpoint_structural_deferral instead"
        );
    }
    let registry_calls = maintenance
        .matches("checkpoint_structural_deferral(")
        .count();
    assert!(
        registry_calls >= 2,
        "expected the executor arm and the growth-policy helper to consult \
         the registry (found {registry_calls} references)"
    );

    let checkpoint = source("src/lifecycle/checkpoint.rs");
    assert!(
        checkpoint
            .matches("checkpoint_structural_deferral(")
            .count()
            >= 2,
        "the synchronous checkpoint path must consult the registry"
    );

    // Space-reclamation contract §3.1 (slice 7): the close-time checkpoint is
    // a scheduling site too.
    let close = source("src/lifecycle/durable/close.rs");
    for predicate in PREDICATES {
        assert!(
            !close.contains(&format!("{predicate}(")),
            "durable/close.rs calls raw predicate {predicate} — route it \
             through checkpoint_structural_deferral instead"
        );
    }
    assert!(
        close.matches("checkpoint_structural_deferral(").count() >= 1,
        "the close-time checkpoint must consult the registry"
    );
}
