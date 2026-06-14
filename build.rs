fn main() {
    // Compile CSI protobuf definitions using tonic-prost-build
    let mut config = prost_build::Config::new();
    config.out_dir(std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap()));

    tonics_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_with_config(config, &["proto/csi.proto"], &["proto"])
        .expect("Failed to compile CSI protobuf");

    println!("cargo:rerun-if-changed=proto/csi.proto");
}
