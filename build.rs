fn main() {
    // Link against libchdb
    println!("cargo:rustc-link-lib=dylib=chdb");
    println!("cargo:rustc-link-search=native=/usr/local/lib");
    // Ensure the linker can find libchdb.so at runtime
    println!("cargo:rustc-link-arg=-Wl,-rpath,/usr/local/lib");
    println!("cargo:rerun-if-changed=build.rs");
}
