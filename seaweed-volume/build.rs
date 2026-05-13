use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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
    emit_build_provenance();

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

fn emit_build_provenance() {
    let git_sha = std::env::var("SEAWEEDFS_COMMIT")
        .or_else(|_| std::env::var("GIT_COMMIT"))
        .or_else(|_| std::env::var("GIT_SHA"))
        .unwrap_or_else(|_| git_output(&["rev-parse", "--short=8", "HEAD"]).unwrap_or_default());
    let git_sha = if git_sha.trim().is_empty() {
        "unknown".to_string()
    } else {
        git_sha
    };
    let git_dirty = std::env::var("WEED_VOLUME_BUILD_GIT_DIRTY")
        .map(|value| value == "true" || value == "1")
        .unwrap_or_else(|_| {
            git_output(&["status", "--porcelain"])
                .map(|out| !out.trim().is_empty())
                .unwrap_or(false)
        });
    let build_unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string());

    println!("cargo:rerun-if-env-changed=SEAWEEDFS_COMMIT");
    println!("cargo:rerun-if-env-changed=GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=GIT_SHA");
    println!("cargo:rerun-if-env-changed=WEED_VOLUME_BUILD_GIT_DIRTY");
    emit_git_rerun_inputs();
    println!("cargo:rustc-env=SEAWEEDFS_COMMIT={git_sha}");
    println!("cargo:rustc-env=WEED_VOLUME_BUILD_GIT_SHA={git_sha}");
    println!("cargo:rustc-env=WEED_VOLUME_BUILD_GIT_DIRTY={git_dirty}");
    println!("cargo:rustc-env=WEED_VOLUME_BUILD_UNIX_SECONDS={build_unix_seconds}");
}

fn emit_git_rerun_inputs() {
    let Some(git_dir) = git_output(&["rev-parse", "--git-dir"]) else {
        return;
    };
    let git_dir = resolve_git_path(&git_dir);
    emit_if_exists(git_dir.join("HEAD"));
    emit_if_exists(git_dir.join("index"));
    emit_if_exists(git_dir.join("packed-refs"));

    if let Ok(head) = std::fs::read_to_string(git_dir.join("HEAD")) {
        if let Some(reference) = head.trim().strip_prefix("ref: ") {
            emit_if_exists(git_dir.join(reference));
        }
    }
}

fn resolve_git_path(path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string()))
            .join(path)
    }
}

fn emit_if_exists(path: PathBuf) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|s| s.trim().to_string())
}
