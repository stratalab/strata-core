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

    // macOS ARM: enable Metal
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        config.define("GGML_METAL", "ON");
        config.define("GGML_METAL_EMBED_LIBRARY", "ON");
    }

    // macOS x86: disable Metal
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    config.define("GGML_METAL", "OFF");

    // Linux x86: enable AVX2 (matching CI)
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    config.define("GGML_AVX2", "ON");

    let dst = config.build_target("llama").build();

    // Link static libraries produced by cmake
    let build_dir = dst.join("build");
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("src").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        build_dir.join("ggml/src").display()
    );
    // Some cmake versions put libs directly under build/
    println!("cargo:rustc-link-search=native={}", build_dir.display());

    println!("cargo:rustc-link-lib=static=llama");
    println!("cargo:rustc-link-lib=static=ggml");
    println!("cargo:rustc-link-lib=static=ggml-base");
    println!("cargo:rustc-link-lib=static=ggml-cpu");

    // System dependencies
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
        #[cfg(target_arch = "aarch64")]
        {
            println!("cargo:rustc-link-lib=framework=Metal");
            println!("cargo:rustc-link-lib=framework=MetalKit");
            println!("cargo:rustc-link-lib=framework=Foundation");
        }
    }

    println!("cargo:rerun-if-changed=vendor/llama.cpp/CMakeLists.txt");
}
