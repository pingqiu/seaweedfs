fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    let weed_dir = std::env::var("SEAWEEDFS_PATH")
        .map(|p| std::path::PathBuf::from(p).join("weed"))
        .unwrap_or_else(|_| std::path::PathBuf::from("../weed"));
    let filer_pb_dir = weed_dir.join("pb");
    let filer_proto = filer_pb_dir.join("filer.proto");
    let weed_dir_env = weed_dir.to_string_lossy().replace('\\', "/");

    println!("cargo:rerun-if-env-changed=SEAWEEDFS_PATH");
    println!("cargo:rerun-if-changed={}", filer_proto.display());
    println!("cargo:rerun-if-changed={}", filer_pb_dir.display());
    println!("cargo:rustc-env=SEAWEEDFS_WEED_DIR={weed_dir_env}");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("seaweed_descriptor.bin"))
        .compile_protos(
            &[
                std::path::PathBuf::from("proto/volume_server.proto"),
                std::path::PathBuf::from("proto/master.proto"),
                std::path::PathBuf::from("proto/remote.proto"),
                filer_proto,
            ],
            &[std::path::PathBuf::from("proto"), filer_pb_dir],
        )?;
    Ok(())
}
