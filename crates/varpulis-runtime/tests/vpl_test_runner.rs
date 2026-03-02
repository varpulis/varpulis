//! Auto-discovery test runner for `.vpl.test` fixture files.
//!
//! This integration test discovers all `.vpl.test` files under `tests/vpl/`
//! and runs each one through the VPL test DSL engine.

use std::path::Path;

use varpulis_runtime::vpl_test;

#[test]
fn run_vpl_test_fixtures() {
    let fixture_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vpl");

    let fixtures = vpl_test::discover_fixtures(&fixture_dir);
    assert!(
        !fixtures.is_empty(),
        "No .vpl.test fixtures found in {}",
        fixture_dir.display()
    );

    let mut passed = 0;
    let mut failed = 0;
    let mut failures = Vec::new();

    for path in &fixtures {
        let fixture = match vpl_test::parse_fixture(path) {
            Ok(f) => f,
            Err(e) => {
                failed += 1;
                failures.push(format!("PARSE ERROR {}: {e}", path.display()));
                continue;
            }
        };

        let result = vpl_test::run_fixture(&fixture);

        if result.passed {
            passed += 1;
        } else {
            failed += 1;
            let failure_msg = result.failure.as_deref().unwrap_or("unknown failure");
            failures.push(format!(
                "FAIL {} ({})\n  {}",
                result.name,
                path.display(),
                failure_msg.replace('\n', "\n  ")
            ));
        }
    }

    // Print summary
    eprintln!("\n--- VPL Test Results ---");
    eprintln!(
        "{passed} passed, {failed} failed, {} total",
        passed + failed
    );

    if !failures.is_empty() {
        eprintln!("\nFailures:");
        for f in &failures {
            eprintln!("\n{f}");
        }
        panic!(
            "{failed} VPL test(s) failed out of {} total",
            passed + failed
        );
    }
}
