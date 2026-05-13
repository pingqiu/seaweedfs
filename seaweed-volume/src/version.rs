//! Version helpers aligned with Go's util/version package.

use std::sync::OnceLock;

#[cfg(feature = "5bytes")]
const SIZE_LIMIT: &str = "8000GB"; // Matches Go production builds (5BytesOffset)
#[cfg(not(feature = "5bytes"))]
const SIZE_LIMIT: &str = "30GB"; // Matches Go default build (!5BytesOffset)

pub fn size_limit() -> &'static str {
    SIZE_LIMIT
}

pub fn commit() -> &'static str {
    option_env!("SEAWEEDFS_COMMIT")
        .or(option_env!("GIT_COMMIT"))
        .or(option_env!("GIT_SHA"))
        .unwrap_or("")
}

pub fn version_number() -> &'static str {
    static VERSION_NUMBER: OnceLock<String> = OnceLock::new();
    VERSION_NUMBER
        .get_or_init(|| {
            parse_go_version_number().unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
        })
        .as_str()
}

pub fn version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| format!("{} {}", size_limit(), version_number()))
        .as_str()
}

pub fn full_version() -> &'static str {
    static FULL: OnceLock<String> = OnceLock::new();
    FULL.get_or_init(|| format!("{} {}", version(), commit()))
        .as_str()
}

pub fn server_header() -> &'static str {
    static HEADER: OnceLock<String> = OnceLock::new();
    HEADER
        .get_or_init(|| format!("SeaweedFS Volume {}", version()))
        .as_str()
}

pub fn build_git_sha() -> &'static str {
    let sha = option_env!("WEED_VOLUME_BUILD_GIT_SHA")
        .or(option_env!("SEAWEEDFS_COMMIT"))
        .or(option_env!("GIT_COMMIT"))
        .or(option_env!("GIT_SHA"))
        .unwrap_or("unknown");
    if sha.is_empty() {
        "unknown"
    } else {
        sha
    }
}

pub fn build_git_dirty() -> bool {
    matches!(option_env!("WEED_VOLUME_BUILD_GIT_DIRTY"), Some("true"))
}

pub fn build_unix_seconds() -> &'static str {
    option_env!("WEED_VOLUME_BUILD_UNIX_SECONDS").unwrap_or("unknown")
}

pub fn enabled_features() -> Vec<&'static str> {
    let mut features = Vec::new();
    #[cfg(feature = "5bytes")]
    features.push("5bytes");
    #[cfg(feature = "rdma")]
    features.push("rdma");
    features
}

pub fn rdma_transports() -> Vec<&'static str> {
    #[cfg(feature = "rdma")]
    {
        vec!["tcp", "rc"]
    }
    #[cfg(not(feature = "rdma"))]
    {
        vec!["tcp"]
    }
}

pub fn default_rdma_policy_fingerprint() -> String {
    seaweed_rdma::RdmaReadPolicy::for_pool(4 * 1024 * 1024, 64).fingerprint()
}

pub fn sra_version_json() -> serde_json::Value {
    serde_json::json!({
        "service": "weed-volume",
        "git_sha": build_git_sha(),
        "git_dirty": build_git_dirty(),
        "build_unix_seconds": build_unix_seconds(),
        "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "crate_version": env!("CARGO_PKG_VERSION"),
        "seaweedfs_version": version_number(),
        "features": enabled_features(),
        "rdma_transports": rdma_transports(),
        "wire_protocol_version": seaweed_rdma::sra_hello::PROTOCOL_VERSION,
        "policy_fingerprint": default_rdma_policy_fingerprint(),
    })
}

fn parse_go_version_number() -> Option<String> {
    let src = include_str!(concat!(
        env!("SEAWEEDFS_WEED_DIR"),
        "/util/version/constants.go"
    ));
    let mut major: Option<u32> = None;
    let mut minor: Option<u32> = None;
    for line in src.lines() {
        let l = line.trim();
        if l.starts_with("MAJOR_VERSION") {
            major = parse_int32_line(l);
        } else if l.starts_with("MINOR_VERSION") {
            minor = parse_int32_line(l);
        }
        if major.is_some() && minor.is_some() {
            break;
        }
    }
    match (major, minor) {
        (Some(maj), Some(min)) => Some(format!("{}.{}", maj, format!("{:02}", min))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sra_version_json_contains_release_gate_fields() {
        let value = sra_version_json();
        assert_eq!(value["service"], "weed-volume");
        assert!(value["git_sha"].as_str().is_some());
        assert!(value["git_dirty"].as_bool().is_some());
        assert!(value["build_profile"].as_str().is_some());
        assert!(value["crate_version"].as_str().is_some());
        assert!(value["features"].as_array().is_some());
        assert!(value["rdma_transports"].as_array().is_some());
        assert!(value["wire_protocol_version"].as_u64().unwrap() > 0);
        assert!(value["policy_fingerprint"]
            .as_str()
            .unwrap()
            .starts_with("fnv64:"));
    }
}

fn parse_int32_line(line: &str) -> Option<u32> {
    let start = line.find("int32(")? + "int32(".len();
    let rest = &line[start..];
    let end = rest.find(')')?;
    rest[..end].trim().parse::<u32>().ok()
}
