use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;
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
            "isearch-cli-{prefix}-{}-{nanos}",
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

#[test]
fn top_level_help_lists_all_subcommands() {
    let mut cmd = AssertCmd::cargo_bin("isearch").expect("cargo_bin isearch");
    let out = cmd.arg("--help").output().expect("spawn");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    for sub in ["index", "query", "watch", "live"] {
        assert!(
            text.contains(sub),
            "expected --help to contain subcommand {sub}, got:\n{text}"
        );
    }
}

#[test]
fn query_without_index_reports_actionable_error() {
    let ws = TempWorkspace::new("missing-index");
    ws.write("a.txt", "hello\n");
    let out = run_isearch(
        &[
            "query",
            "hello",
            "--path",
            ws.root.to_string_lossy().as_ref(),
        ],
        &ws.root,
    );
    assert!(!out.status.success(), "query should fail without index");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("run `isearch index`"),
        "expected actionable index hint, got:\n{stderr}"
    );
}

#[test]
fn query_requires_non_empty_pattern() {
    let mut cmd = AssertCmd::cargo_bin("isearch").expect("cargo_bin isearch");
    let out = cmd.args(["query", ""]).output().expect("spawn");
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("empty search text"),
        "expected empty-pattern validation, got:\n{stderr}"
    );
}

#[test]
fn index_then_query_finds_match_in_nested_path() {
    let ws = TempWorkspace::new("index-query");
    ws.write("src/lib.rs", "pub fn login() {}\n");
    ws.write("README.md", "no match\n");

    let index = run_isearch(&["index", ws.root.to_string_lossy().as_ref()], &ws.root);
    assert!(
        index.status.success(),
        "index failed: {}",
        String::from_utf8_lossy(&index.stderr)
    );

    let out = run_isearch(
        &[
            "query",
            "login\\(",
            "--path",
            ws.root.to_string_lossy().as_ref(),
        ],
        &ws.root,
    );
    assert!(
        out.status.success(),
        "query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("./src/lib.rs"),
        "expected relative file path in output, got:\n{stdout}"
    );
    assert!(stdout.contains("1:pub fn login() {}"));
}

#[test]
fn watch_and_live_help_show_runtime_flags() {
    for args in [
        vec!["watch", "--help"],
        vec!["live", "--help"],
        vec!["index", "--help"],
    ] {
        let mut cmd = AssertCmd::cargo_bin("isearch").expect("cargo_bin isearch");
        let out = cmd.args(args.clone()).output().expect("spawn");
        assert!(
            out.status.success(),
            "help command failed for {:?}: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(
            text.contains("--debounce-ms") || text.contains("--shard-target-postings-bytes"),
            "expected documented runtime flags for {:?}, got:\n{text}",
            args
        );
    }
}
