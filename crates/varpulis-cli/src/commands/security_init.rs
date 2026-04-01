use std::path::Path;

use anyhow::Result;
use owo_colors::OwoColorize;
use varpulis_cli::output;

/// Initialize a security deployment directory with rules, config, and docker-compose.
pub fn run_security_init(dir: &Path) -> Result<()> {
    output::header("Varpulis Security Init");

    if dir.exists() {
        anyhow::bail!(
            "Directory '{}' already exists. Use a different path.",
            dir.display()
        );
    }

    std::fs::create_dir_all(dir)?;
    std::fs::create_dir_all(dir.join("rules"))?;
    std::fs::create_dir_all(dir.join("data"))?;

    // Write docker-compose.yml
    std::fs::write(
        dir.join("docker-compose.yml"),
        include_str!("../../../../deploy/security/docker-compose.yml"),
    )?;

    // Write prometheus config
    std::fs::write(
        dir.join("prometheus.yml"),
        include_str!("../../../../deploy/security/prometheus.yml"),
    )?;

    // Write .env.example
    std::fs::write(
        dir.join(".env.example"),
        include_str!("../../../../deploy/security/.env.example"),
    )?;

    // Write .env from example
    std::fs::write(
        dir.join(".env"),
        include_str!("../../../../deploy/security/.env.example"),
    )?;

    // Write .varpulis.toml
    std::fs::write(
        dir.join(".varpulis.toml"),
        "[deploy]\nurl = \"http://localhost:9000\"\napi_key = \"changeme\"\n",
    )?;

    // Copy VPL rules
    let rules = [
        (
            "detect_predictive_killchain.vpl",
            include_str!("../../../../examples/security-demo/detect_predictive_killchain.vpl"),
        ),
        (
            "detect_brute_force_takeover.vpl",
            include_str!("../../../../examples/security-demo/detect_brute_force_takeover.vpl"),
        ),
        (
            "detect_exfil_burst.vpl",
            include_str!("../../../../examples/security-demo/detect_exfil_burst.vpl"),
        ),
        (
            "detect_lotl_forecast.vpl",
            include_str!("../../../../examples/security-demo/detect_lotl_forecast.vpl"),
        ),
        (
            "detect_full_killchain.vpl",
            include_str!("../../../../examples/security-demo/detect_full_killchain.vpl"),
        ),
        (
            "detect_credential_dumping.vpl",
            include_str!("../../../../examples/security-demo/detect_credential_dumping.vpl"),
        ),
        (
            "detect_lateral_movement.vpl",
            include_str!("../../../../examples/security-demo/detect_lateral_movement.vpl"),
        ),
    ];

    for (name, content) in &rules {
        std::fs::write(dir.join("rules").join(name), content)?;
    }

    // Copy test data
    let datasets = [
        (
            "apt29_full_chain.jsonl",
            include_str!("../../../../examples/security-demo/data/apt29_full_chain.jsonl"),
        ),
        (
            "predictive_killchain.jsonl",
            include_str!("../../../../examples/security-demo/data/predictive_killchain.jsonl"),
        ),
        (
            "brute_force_takeover.jsonl",
            include_str!("../../../../examples/security-demo/data/brute_force_takeover.jsonl"),
        ),
    ];

    for (name, content) in &datasets {
        std::fs::write(dir.join("data").join(name), content)?;
    }

    // Write test injection script
    std::fs::write(
        dir.join("test-inject.sh"),
        r#"#!/usr/bin/env bash
# test-inject.sh — Verify Varpulis security deployment
set -euo pipefail

echo "Testing Varpulis security detection..."
echo ""

# Test 1: Full kill chain detection
echo "Test 1: Full APT kill chain..."
varpulis detect --rules rules/detect_full_killchain.vpl \
    --events data/apt29_full_chain.jsonl -w 1 --format table 2>/dev/null

echo ""

# Test 2: Predictive detection
echo "Test 2: Predictive kill chain (forecast)..."
varpulis detect --rules rules/detect_predictive_killchain.vpl \
    --events data/predictive_killchain.jsonl -w 1 --format table 2>/dev/null

echo ""

# Test 3: Brute force (Kleene+)
echo "Test 3: Brute force with Kleene+ closure..."
varpulis detect --rules rules/detect_brute_force_takeover.vpl \
    --events data/brute_force_takeover.jsonl -w 1 --format table 2>/dev/null

echo ""
echo "All tests complete."
"#,
    )?;

    // Make test script executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            dir.join("test-inject.sh"),
            std::fs::Permissions::from_mode(0o755),
        )?;
    }

    // Write README
    std::fs::write(
        dir.join("README.md"),
        format!(
            "# Varpulis Security Deployment\n\
             \n\
             ## Quick Start\n\
             \n\
             ```bash\n\
             # 1. Start the stack\n\
             docker compose up -d\n\
             \n\
             # 2. Deploy detection rules\n\
             varpulis deploy-rules --dir rules/ --server http://localhost:9000 --api-key changeme\n\
             \n\
             # 3. Verify detection works\n\
             bash test-inject.sh\n\
             ```\n\
             \n\
             ## What's Included\n\
             \n\
             - **7 detection rules** covering kill chains, credential dumping, lateral movement,\n  \
               brute force (Kleene+), exfiltration burst, and living-off-the-land chains\n\
             - **Predictive detection** alerts before attacks complete\n\
             - **Docker Compose** with Varpulis server, Prometheus, and Grafana\n\
             - **Test data** from MORDOR APT29 dataset for validation\n\
             \n\
             ## Monitoring\n\
             \n\
             - Grafana: http://localhost:3000 (admin/admin)\n\
             - Prometheus: http://localhost:9091\n\
             - Varpulis API: http://localhost:9000/api/v1/pipelines\n\
             \n\
             Generated by varpulis security init v{}\n",
            env!("CARGO_PKG_VERSION")
        ),
    )?;

    // Print summary
    println!("Created security deployment at: {}", dir.display().bold());
    println!();
    println!("  {}/", dir.display());
    println!("  ├── rules/          7 VPL detection rules");
    println!("  ├── data/           Test datasets (APT29)");
    println!("  ├── docker-compose.yml");
    println!("  ├── prometheus.yml");
    println!("  ├── .env");
    println!("  ├── .varpulis.toml");
    println!("  ├── test-inject.sh");
    println!("  └── README.md");
    println!();
    println!("{}", "Next steps:".bold());
    println!("  cd {} && docker compose up -d", dir.display());
    println!(
        "  varpulis deploy-rules --dir rules/ --server http://localhost:9000 --api-key changeme"
    );
    println!("  bash test-inject.sh");
    println!();

    Ok(())
}
