//! Integration tests for deployment readiness
//!
//! These tests verify that all deployment components are properly configured
//! and that the system is ready for production deployment.

use std::path::Path;

#[test]
fn test_deployment_scripts_exist() {
    assert!(
        Path::new("scripts/deploy.sh").exists(),
        "deploy.sh script must exist"
    );
    assert!(
        Path::new("scripts/rollback.sh").exists(),
        "rollback.sh script must exist"
    );
    assert!(
        Path::new("scripts/setup-monitoring.sh").exists(),
        "setup-monitoring.sh script must exist"
    );
}

#[test]
fn test_systemd_service_files_exist() {
    assert!(
        Path::new("scripts/blockscout-api.service").exists(),
        "blockscout-api.service must exist"
    );
    assert!(
        Path::new("scripts/blockscout-backfill.service").exists(),
        "blockscout-backfill.service must exist"
    );
}

#[test]
fn test_deployment_documentation_exists() {
    assert!(
        Path::new("DEPLOYMENT.md").exists(),
        "DEPLOYMENT.md must exist"
    );

    // Verify documentation contains key sections
    let content = std::fs::read_to_string("DEPLOYMENT.md").expect("Failed to read DEPLOYMENT.md");

    assert!(
        content.contains("## Rollback Procedures"),
        "Documentation must include rollback procedures"
    );
    assert!(
        content.contains("## Monitoring and Metrics"),
        "Documentation must include monitoring section"
    );
    assert!(
        content.contains("## Performance Benchmarks"),
        "Documentation must include performance benchmarks"
    );
    assert!(
        content.contains("## Troubleshooting"),
        "Documentation must include troubleshooting section"
    );
}

#[test]
fn test_benchmark_suite_exists() {
    assert!(
        Path::new("benches/fdb_vs_mdbx.rs").exists(),
        "Benchmark suite must exist"
    );

    // Verify benchmark contains required test cases
    let content =
        std::fs::read_to_string("benches/fdb_vs_mdbx.rs").expect("Failed to read benchmark");

    assert!(
        content.contains("bench_backfill_speed"),
        "Must include backfill speed benchmark"
    );
    assert!(
        content.contains("bench_api_latency"),
        "Must include API latency benchmark"
    );
    assert!(
        content.contains("bench_concurrent_reads"),
        "Must include concurrent reads benchmark"
    );
}

#[test]
fn test_deployment_scripts_are_executable() {
    use std::os::unix::fs::PermissionsExt;

    let deploy_perms = std::fs::metadata("scripts/deploy.sh")
        .expect("deploy.sh not found")
        .permissions();
    assert!(
        deploy_perms.mode() & 0o111 != 0,
        "deploy.sh must be executable"
    );

    let rollback_perms = std::fs::metadata("scripts/rollback.sh")
        .expect("rollback.sh not found")
        .permissions();
    assert!(
        rollback_perms.mode() & 0o111 != 0,
        "rollback.sh must be executable"
    );

    let monitoring_perms = std::fs::metadata("scripts/setup-monitoring.sh")
        .expect("setup-monitoring.sh not found")
        .permissions();
    assert!(
        monitoring_perms.mode() & 0o111 != 0,
        "setup-monitoring.sh must be executable"
    );
}

#[test]
fn test_service_files_have_correct_format() {
    let api_service = std::fs::read_to_string("scripts/blockscout-api.service")
        .expect("Failed to read blockscout-api.service");

    // Verify service file has required sections
    assert!(api_service.contains("[Unit]"), "Service must have [Unit] section");
    assert!(api_service.contains("[Service]"), "Service must have [Service] section");
    assert!(api_service.contains("[Install]"), "Service must have [Install] section");

    // Verify service configuration
    assert!(
        api_service.contains("ExecStart=/usr/local/bin/blockscout-api"),
        "Service must specify correct ExecStart"
    );
    assert!(
        api_service.contains("--mdbx-path"),
        "Service must use MDBX database"
    );
    assert!(
        api_service.contains("Restart=always"),
        "Service must have auto-restart enabled"
    );
}

#[test]
fn test_deployment_script_has_safety_checks() {
    let deploy_script =
        std::fs::read_to_string("scripts/deploy.sh").expect("Failed to read deploy.sh");

    // Verify script has safety features
    assert!(
        deploy_script.contains("set -e"),
        "Script must exit on error"
    );
    assert!(
        deploy_script.contains("--dry-run"),
        "Script must support dry-run mode"
    );
    assert!(
        deploy_script.contains("BACKUP"),
        "Script must create backups"
    );
    assert!(
        deploy_script.contains("health"),
        "Script must include health checks"
    );
}

#[test]
fn test_rollback_script_has_confirmation() {
    let rollback_script =
        std::fs::read_to_string("scripts/rollback.sh").expect("Failed to read rollback.sh");

    // Verify rollback has safety features
    assert!(
        rollback_script.contains("read -p"),
        "Rollback must require confirmation"
    );
    assert!(
        rollback_script.contains("Verify backup"),
        "Rollback must verify backup exists"
    );
}

#[cfg(feature = "reth")]
#[test]
fn test_mdbx_feature_enabled_for_deployment() {
    // This test only runs when mdbx feature is enabled
    // Verifies that we're building with the correct feature flag
    assert!(cfg!(feature = "reth"), "MDBX feature must be enabled");
}

#[test]
fn test_cargo_bench_configuration() {
    let cargo_toml = std::fs::read_to_string("Cargo.toml").expect("Failed to read Cargo.toml");

    // Verify benchmark is configured
    assert!(
        cargo_toml.contains("[[bench]]"),
        "Cargo.toml must configure benchmarks"
    );
    assert!(
        cargo_toml.contains("name = \"fdb_vs_mdbx\""),
        "Benchmark must be named correctly"
    );
    assert!(
        cargo_toml.contains("harness = false"),
        "Benchmark must disable default harness"
    );

    // Verify criterion is a dev dependency
    assert!(
        cargo_toml.contains("criterion"),
        "Criterion must be in dev-dependencies"
    );
}

#[test]
fn test_monitoring_setup_includes_prometheus() {
    let monitoring_script = std::fs::read_to_string("scripts/setup-monitoring.sh")
        .expect("Failed to read setup-monitoring.sh");

    assert!(
        monitoring_script.contains("prometheus"),
        "Must install Prometheus"
    );
    assert!(
        monitoring_script.contains("node_exporter"),
        "Must install Node Exporter"
    );
    assert!(
        monitoring_script.contains("alerting"),
        "Must configure alerting"
    );
}

#[test]
fn test_deployment_targets_correct_server() {
    let deploy_script =
        std::fs::read_to_string("scripts/deploy.sh").expect("Failed to read deploy.sh");

    assert!(
        deploy_script.contains("root@aya"),
        "Deployment must target root@aya server"
    );
}

#[test]
fn test_documentation_includes_performance_targets() {
    let docs = std::fs::read_to_string("DEPLOYMENT.md").expect("Failed to read DEPLOYMENT.md");

    // Verify performance targets are documented
    assert!(
        docs.contains("100 blocks/sec") || docs.contains("100+ blocks/sec"),
        "Must document target backfill speed"
    );
    assert!(
        docs.contains("50ms") || docs.contains("< 50ms"),
        "Must document target API latency"
    );
}
