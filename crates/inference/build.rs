fn main() {
    #[cfg(feature = "local")]
    build_llama_cpp();
}

#[cfg(feature = "local")]
fn build_llama_cpp() {
    let mut config = cmake::Config::new("vendor/llama.cpp");

    config
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("CMAKE_BUILD_TYPE", "Release")
        .define("GGML_NATIVE", "OFF")
        .define("LLAMA_BUILD_TESTS", "OFF")
        .define("LLAMA_BUILD_EXAMPLES", "OFF")
        .define("LLAMA_BUILD_SERVER", "OFF")
        .define("LLAMA_CURL", "OFF")
        // Disable the BLAS backend explicitly. Without this, cmake auto-detects
        // a BLAS provider on macOS (Accelerate) and `ggml-backend-reg.cpp`
        // references `ggml_backend_blas_reg`, but the build.rs below doesn't
        // know to link `libggml-blas.a` — we end up with an undefined symbol
        // at link time on aarch64-apple-darwin. The MiniLM embedding model
        // we ship doesn't benefit meaningfully from BLAS, and the local
        // generation path is acceptable without it for the v0.6.x line.
        // Re-enable when we actually need BLAS-accelerated prompt eval.
        .define("GGML_BLAS", "OFF");

    // Metal: cmake auto-detects (ON on Apple, OFF elsewhere).
    // We just need to embed the Metal library so it doesn't need a .metallib file at runtime.
    #[cfg(target_os = "macos")]
    config.define("GGML_METAL_EMBED_LIBRARY", "ON");

    // CUDA: probe for nvcc — if found, enable CUDA backend.
    let has_cuda = probe_cuda();
    if has_cuda {
        config.define("GGML_CUDA", "ON");
    }

    // CPU optimizations for x86_64
    #[cfg(target_arch = "x86_64")]
    config.define("GGML_AVX2", "ON");

    let dst = config.build_target("llama").build();

    // --- Link static libraries ---

    let build_dir = dst.join("build");
    // Add search paths for all possible library locations
    for subdir in &[
        "src",
        "ggml/src",
        "ggml/src/ggml-cuda",
        "ggml/src/ggml-metal",
        "",
    ] {
        let path = build_dir.join(subdir);
        if path.exists() {
            println!("cargo:rustc-link-search=native={}", path.display());
        }
    }

    // Core libraries (always present)
    println!("cargo:rustc-link-lib=static=llama");
    println!("cargo:rustc-link-lib=static=ggml");
    println!("cargo:rustc-link-lib=static=ggml-base");
    println!("cargo:rustc-link-lib=static=ggml-cpu");

    // GPU libraries (linked only if the static lib was actually built)
    if has_cuda {
        try_link_static(&build_dir, "ggml-cuda");
        println!("cargo:rustc-link-lib=cuda");
        println!("cargo:rustc-link-lib=cublas");
        println!("cargo:rustc-link-lib=cudart");
    }

    #[cfg(target_os = "macos")]
    try_link_static(&build_dir, "ggml-metal");

    // --- System dependencies ---

    #[cfg(target_os = "linux")]
    {
        println!("cargo:rustc-link-lib=stdc++");
        println!("cargo:rustc-link-lib=m");
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=gomp"); // OpenMP
    }

    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-lib=c++");
        println!("cargo:rustc-link-lib=framework=Accelerate");
        println!("cargo:rustc-link-lib=framework=Foundation");
        println!("cargo:rustc-link-lib=framework=Metal");
        println!("cargo:rustc-link-lib=framework=MetalKit");
    }

    println!("cargo:rerun-if-changed=vendor/llama.cpp/CMakeLists.txt");
}

/// Check if CUDA toolkit is available by looking for `nvcc`.
#[cfg(feature = "local")]
fn probe_cuda() -> bool {
    std::process::Command::new("nvcc")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Link a static library if its `.a` file exists in the build tree.
#[cfg(feature = "local")]
fn try_link_static(build_dir: &std::path::Path, name: &str) {
    let lib_name = format!("lib{name}.a");
    for entry in walkdir(build_dir) {
        if entry.ends_with(&lib_name) {
            println!("cargo:rustc-link-lib=static={name}");
            return;
        }
    }
}

/// Simple recursive file listing (avoids adding walkdir dependency).
#[cfg(feature = "local")]
fn walkdir(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(walkdir(&path));
            } else {
                files.push(path);
            }
        }
    }
    files
}
