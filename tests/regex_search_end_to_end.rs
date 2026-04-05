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
            "isearch-test-{prefix}-{}-{nanos}",
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
    cmd.args(args).current_dir(cwd);
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

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.match_indices(needle).count()
}

#[test]
fn query_alternation_matches_files_from_each_branch() {
    let ws = TempWorkspace::new("alternation");
    ws.write("a.txt", "alpha\nfoo\n");
    ws.write("b.txt", "beta\nbar\n");
    ws.write("c.txt", "gamma\nneither\n");
    index_workspace(&ws.root);

    let out = run_isearch(
        &[
            "query",
            "foo|bar",
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
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains("./a.txt") && stdout.contains("./b.txt"),
        "expected both branch files in output, got stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("2:foo"), "missing foo hit:\n{stdout}\nstderr:\n{stderr}");
    assert!(stdout.contains("2:bar"), "missing bar hit:\n{stdout}\nstderr:\n{stderr}");
    assert!(!stdout.contains("./c.txt"), "unexpected non-match file:\n{stdout}");
}

#[test]
fn query_matches_with_all_docs_fallback_prefixless_regex() {
    let ws = TempWorkspace::new("fallback");
    ws.write("x.txt", "abc12\n");
    ws.write("y.txt", "zzz\n");
    index_workspace(&ws.root);

    let out = run_isearch(
        &[
            "query",
            "[a-z]{3}\\d{2}",
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
    assert!(stdout.contains("./x.txt"), "expected fallback hit, got:\n{stdout}");
    assert!(stdout.contains("1:abc12"), "expected matching line, got:\n{stdout}");
    assert!(!stdout.contains("./y.txt"), "unexpected non-match file:\n{stdout}");
}

#[test]
fn query_dedupes_multiple_regex_matches_on_same_line() {
    let ws = TempWorkspace::new("dedupe");
    ws.write("dup.txt", "foo foo foo\n");
    index_workspace(&ws.root);

    let out = run_isearch(
        &["query", "foo", "--path", ws.root.to_string_lossy().as_ref()],
        &ws.root,
    );
    assert!(
        out.status.success(),
        "query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("./dup.txt"), "missing file output:\n{stdout}");
    assert_eq!(
        count_occurrences(&stdout, "1:foo foo foo"),
        1,
        "expected one line hit per line despite multiple regex matches:\n{stdout}"
    );
}

#[test]
fn query_unsatisfiable_regex_returns_no_hits() {
    let ws = TempWorkspace::new("unsat");
    ws.write("t.txt", "abc\n");
    index_workspace(&ws.root);

    let out = run_isearch(
        &["query", "\\b\\B", "--path", ws.root.to_string_lossy().as_ref()],
        &ws.root,
    );
    assert!(
        out.status.success(),
        "query failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.trim().is_empty(),
        "expected no stdout matches for unsatisfiable regex, got:\n{stdout}"
    );
}
