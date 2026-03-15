fn main() {
    #[cfg(feature = "embed-bundled")]
    build_llama_cpp();
}

#[cfg(feature = "embed-bundled")]
fn build_llama_cpp() {
    let mut config = cmake::Config::new("vendor/llama.cpp");

    config
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("CMAKE_BUILD_TYPE", "Release")
        .define("GGML_NATIVE", "OFF")
        .define("LLAMA_BUILD_TESTS", "OFF")
        .define("LLAMA_BUILD_EXAMPLES", "OFF")
        .define("LLAMA_BUILD_SERVER", "OFF")
        .define("LLAMA_CURL", "OFF");

    // --- GPU backend selection ---

    #[cfg(feature = "embed-cuda")]
    {
        config.define("GGML_CUDA", "ON");
    }

    #[cfg(feature = "embed-metal")]
    {
        config.define("GGML_METAL", "ON");
        config.define("GGML_METAL_EMBED_LIBRARY", "ON");
    }

    // Auto-enable Metal on macOS ARM when no GPU feature is explicitly set
    #[cfg(all(
        target_os = "macos",
        target_arch = "aarch64",
        not(feature = "embed-metal"),
        not(feature = "embed-cuda")
    ))]
    {
        config.define("GGML_METAL", "ON");
        config.define("GGML_METAL_EMBED_LIBRARY", "ON");
    }

    // Disable Metal on macOS x86 unless explicitly requested
    #[cfg(all(target_os = "macos", target_arch = "x86_64", not(feature = "embed-metal")))]
    config.define("GGML_METAL", "OFF");

    // --- CPU optimizations ---

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    config.define("GGML_AVX2", "ON");

    let dst = config.build_target("llama").build();

    // --- Link static libraries ---

    let build_dir = dst.join("build");
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("src").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("ggml/src").display()
    );
    println!("cargo:rustc-link-search=native={}", build_dir.display());

    println!("cargo:rustc-link-lib=static=llama");
    println!("cargo:rustc-link-lib=static=ggml");
    println!("cargo:rustc-link-lib=static=ggml-base");
    println!("cargo:rustc-link-lib=static=ggml-cpu");

    // GPU-specific libraries
    #[cfg(feature = "embed-cuda")]
    {
        println!(
            "cargo:rustc-link-search=native={}",
            build_dir.join("ggml/src/ggml-cuda").display()
        );
        println!("cargo:rustc-link-lib=static=ggml-cuda");
        println!("cargo:rustc-link-lib=cuda");
        println!("cargo:rustc-link-lib=cublas");
        println!("cargo:rustc-link-lib=cudart");
    }

    #[cfg(any(
        feature = "embed-metal",
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    {
        println!(
            "cargo:rustc-link-search=native={}",
            build_dir.join("ggml/src/ggml-metal").display()
        );
        // ggml-metal may be built as a separate static lib
        // Try to link it; if it's folded into ggml, the linker will skip it
        println!("cargo:rustc-link-lib=static=ggml-metal");
    }

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
        // Metal frameworks (needed when Metal is enabled)
        #[cfg(any(
            feature = "embed-metal",
            all(target_os = "macos", target_arch = "aarch64")
        ))]
        {
            println!("cargo:rustc-link-lib=framework=Metal");
            println!("cargo:rustc-link-lib=framework=MetalKit");
            println!("cargo:rustc-link-lib=framework=Foundation");
        }
    }

    println!("cargo:rerun-if-changed=vendor/llama.cpp/CMakeLists.txt");
}
