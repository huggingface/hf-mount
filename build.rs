fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/csi.proto"], &["proto"])
        .expect("Failed to compile CSI protobuf");
}
