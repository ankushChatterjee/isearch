//! Parity tests vs `rg -F` on the containerd fixture under `fixtures/containerd`.

use std::path::PathBuf;
use std::process::Command;

use assert_cmd::Command as AssertCmd;

fn sort_lines(bytes: &[u8]) -> Vec<u8> {
    let mut lines: Vec<&[u8]> = bytes.split(|&b| b == b'\n').collect();
    if let Some(last) = lines.last() {
        if last.is_empty() {
            lines.pop();
        }
    }
    lines.sort();
    let mut out = Vec::new();
    for line in lines {
        out.extend_from_slice(line);
        out.push(b'\n');
    }
    out
}

#[test]
#[ignore = "main temporarily exercises ngram extraction; restore search CLI to re-enable"]
fn parity_validate_target_container_vs_rg_fixed_string() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let corpus = root.join("fixtures/containerd");
    assert!(
        corpus.join(".git").is_dir(),
        "missing corpus at {}; run ./scripts/ensure_containerd.sh",
        corpus.display()
    );

    let rg_out = Command::new("rg")
        .current_dir(&corpus)
        .args([
            "--no-heading",
            "--line-number",
            "--color",
            "never",
            "-F",
            "validateTargetContainer",
            ".",
        ])
        .output()
        .expect("spawn rg");
    assert!(
        rg_out.status.success(),
        "rg failed: {}",
        String::from_utf8_lossy(&rg_out.stderr)
    );

    let mut isearch = AssertCmd::cargo_bin("isearch").expect("cargo_bin isearch");
    let is_out = isearch
        .current_dir(&corpus)
        .args([
            "--no-heading",
            "--line-number",
            "--color",
            "never",
            "validateTargetContainer",
        ])
        .output()
        .expect("spawn isearch");
    assert!(
        is_out.status.success(),
        "isearch failed: {}",
        String::from_utf8_lossy(&is_out.stderr)
    );

    assert_eq!(
        sort_lines(&rg_out.stdout),
        sort_lines(&is_out.stdout),
        "sorted stdout mismatch vs rg -F"
    );
}
