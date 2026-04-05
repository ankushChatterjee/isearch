//! CLI and regex-plan behavior for `isearch query`.

use std::path::PathBuf;

use assert_cmd::Command as AssertCmd;

#[test]
fn query_rejects_invalid_regex() {
    let mut cmd = AssertCmd::cargo_bin("isearch").expect("cargo_bin isearch");
    cmd.args(["query", "("]);
    cmd.current_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    let out = cmd.output().expect("spawn");
    assert!(
        !out.status.success(),
        "expected failure for invalid regex"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("invalid regex"),
        "stderr should mention invalid regex, got: {stderr}"
    );
}

#[test]
fn query_help_mentions_regex() {
    let mut cmd = AssertCmd::cargo_bin("isearch").expect("cargo_bin isearch");
    cmd.args(["query", "--help"]);
    let out = cmd.output().expect("spawn");
    assert!(out.status.success());
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(
        help.to_lowercase().contains("regex"),
        "help should describe regex query: {help}"
    );
}
