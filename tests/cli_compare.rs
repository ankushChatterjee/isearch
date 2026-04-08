//! Parity tests vs `rg` for `isearch query` output.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use assert_cmd::Command as AssertCmd;

struct TempWorkspace {
    root: PathBuf,
}

impl TempWorkspace {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "isearch-parity-{prefix}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create temp workspace");
        Self { root }
    }

    fn write(&self, rel: &str, contents: &str) {
        let path = self.root.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent dirs");
        }
        fs::write(path, contents).expect("write test file");
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn run_isearch(args: &[&str], cwd: &Path) -> Output {
    let mut cmd = AssertCmd::cargo_bin("isearch").expect("cargo_bin isearch");
    let home = cwd.join(".home");
    fs::create_dir_all(&home).expect("create test home");
    cmd.args(args).current_dir(cwd).env("HOME", home);
    cmd.output().expect("spawn isearch")
}

fn index_workspace(root: &Path) {
    let out = run_isearch(&["index", root.to_string_lossy().as_ref()], root);
    assert!(
        out.status.success(),
        "index failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn parse_isearch_hits(stdout: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut current_file = String::new();
    for line in stdout.lines() {
        if let Some(path) = line.strip_prefix("./") {
            current_file = path.to_owned();
            continue;
        }
        if let Some((line_no, text)) = line.split_once(':') {
            if !current_file.is_empty() && line_no.chars().all(|c| c.is_ascii_digit()) {
                out.insert(format!("{current_file}:{line_no}:{text}"));
            }
        }
    }
    out
}

fn parse_rg_hits(stdout: &[u8]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let s = String::from_utf8_lossy(stdout);
    for line in s.lines() {
        if let Some((file, rest)) = line.split_once(':') {
            if let Some((line_no, text)) = rest.split_once(':') {
                let file = file.strip_prefix("./").unwrap_or(file);
                out.insert(format!("{file}:{line_no}:{text}"));
            }
        }
    }
    out
}

fn assert_query_parity(ws: &TempWorkspace, pattern: &str) {
    let out = run_isearch(
        &[
            "query",
            pattern,
            "--path",
            ws.root.to_string_lossy().as_ref(),
        ],
        &ws.root,
    );
    assert!(
        out.status.success(),
        "isearch query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let isearch_hits = parse_isearch_hits(&String::from_utf8_lossy(&out.stdout));

    let rg_out = Command::new("rg")
        .current_dir(&ws.root)
        .args([
            "--no-heading",
            "--line-number",
            "--color",
            "never",
            pattern,
            ".",
        ])
        .output()
        .expect("spawn rg");
    assert!(
        rg_out.status.success() || rg_out.status.code() == Some(1),
        "rg failed unexpectedly: {}",
        String::from_utf8_lossy(&rg_out.stderr)
    );
    let rg_hits = parse_rg_hits(&rg_out.stdout);

    assert_eq!(
        isearch_hits, rg_hits,
        "parity mismatch for pattern `{pattern}`\nisearch:\n{}\nrg:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&rg_out.stdout)
    );
}

#[test]
fn parity_against_rg_for_literal_and_regex_patterns() {
    let ws = TempWorkspace::new("parity");
    ws.write("src/a.txt", "alpha\nfoo_123\ncommon token\n");
    ws.write("src/b.txt", "beta\nbar_999\ncommon token\n");
    ws.write("docs/c.md", "gamma\nfoo_888 and bar_777\n");
    ws.write("docs/d.md", "no hits here\n");

    index_workspace(&ws.root);

    for pattern in ["common token", "foo_\\d{3}", "foo_\\d{3}|bar_\\d{3}"] {
        assert_query_parity(&ws, pattern);
    }
}

#[test]
fn parity_against_rg_for_no_match_query() {
    let ws = TempWorkspace::new("no-match");
    ws.write("a.txt", "alpha\n");
    ws.write("b.txt", "beta\n");
    index_workspace(&ws.root);
    assert_query_parity(&ws, "definitely_not_present_123");
}
