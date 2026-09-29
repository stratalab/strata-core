//! Exposes a sanitizer build to the crate's tests as `cfg(strata_sanitizer)`.
//!
//! `cfg(sanitize = "...")` is set by rustc under `-Zsanitizer=...` but reading
//! it in source needs the unstable `cfg_sanitize` feature, and this crate
//! builds on stable. Cargo hands a build script every target cfg, sanitizer
//! included, so this derives the flag from the real build — no lane has to
//! remember to pass it.
//!
//! Consumer: `tests/recovery_budget.rs` (#3496). Its budget envelope is stated
//! in application bytes and measured with the kernel's `VmHWM`, which under a
//! sanitizer also counts the shadow memory the runtime maps for every page the
//! program touches (~2.4-3.2x under the thread sanitizer), so the absolute-kB
//! assertions are meaningless there. Nothing in the library reads this cfg.

fn main() {
    println!("cargo::rustc-check-cfg=cfg(strata_sanitizer)");
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-env-changed=CARGO_CFG_SANITIZE");
    if std::env::var_os("CARGO_CFG_SANITIZE").is_some() {
        println!("cargo::rustc-cfg=strata_sanitizer");
    }
}
