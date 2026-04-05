use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::border;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Terminal;

use crate::index::{DocId, MmapBundle, PostingsReadTimings};
use crate::ngram;
use crate::verify;
use crate::watch::{self, WatchPhase, WatchStatus};

const SEARCH_DEBOUNCE_MS: u64 = 70;
const MAX_VERIFY_DOCS_PER_QUERY: usize = 1_200;

#[derive(Debug, Clone)]
pub struct LiveConfig {
    pub root: PathBuf,
    pub bundle_dir: PathBuf,
    pub debounce_ms: u64,
    pub compact_interval_secs: u64,
    pub max_batch_files: usize,
    pub max_results: usize,
}

#[derive(Debug, Clone)]
struct SearchJob {
    id: u64,
    query: String,
}

#[derive(Debug, Clone)]
struct SearchResult {
    id: u64,
    rendered_rows: Vec<ResultRow>,
    result_count: usize,
    candidate_count: usize,
    elapsed_ms: f64,
    backend: &'static str,
    error: Option<String>,
}

#[derive(Debug, Clone)]
enum ResultRow {
    FileHeader(String),
    Hit { line_no: usize, line: String },
}

struct App {
    query: String,
    scroll: usize,
    last_watch_status: WatchStatus,
    last_watch_update: Instant,
    rendered_rows: Vec<ResultRow>,
    result_count: usize,
    candidate_count: usize,
    elapsed_ms: f64,
    backend: &'static str,
    last_error: Option<String>,
    in_flight_query_id: u64,
    applied_query_id: u64,
    search_queued_at: Option<Instant>,
}

impl App {
    fn new() -> Self {
        Self {
            query: String::new(),
            scroll: 0,
            last_watch_status: WatchStatus {
                phase: WatchPhase::Bootstrapping,
                changed_paths: 0,
                delta_ops: 0,
            },
            last_watch_update: Instant::now(),
            rendered_rows: Vec::new(),
            result_count: 0,
            candidate_count: 0,
            elapsed_ms: 0.0,
            backend: "idle",
            last_error: None,
            in_flight_query_id: 0,
            applied_query_id: 0,
            search_queued_at: None,
        }
    }

    fn queue_search(&mut self) {
        self.in_flight_query_id = self.in_flight_query_id.saturating_add(1);
        self.search_queued_at = Some(Instant::now());
        self.scroll = 0;
    }

    fn search_ready(&self) -> bool {
        self.search_queued_at
            .map(|t| t.elapsed() >= Duration::from_millis(SEARCH_DEBOUNCE_MS))
            .unwrap_or(false)
    }

    fn pending_query_id(&self) -> Option<u64> {
        if self.in_flight_query_id > self.applied_query_id {
            Some(self.in_flight_query_id)
        } else {
            None
        }
    }
}

pub fn run(cfg: LiveConfig) -> io::Result<()> {
    let (watch_status_tx, watch_status_rx) = mpsc::channel::<WatchStatus>();
    let (watch_err_tx, watch_err_rx) = mpsc::channel::<String>();
    let (search_tx, search_rx) = mpsc::channel::<SearchJob>();
    let (search_result_tx, search_result_rx) = mpsc::channel::<SearchResult>();

    let watch_cfg = watch::WatchConfig {
        root: cfg.root.clone(),
        bundle_dir: cfg.bundle_dir.clone(),
        debounce_ms: cfg.debounce_ms,
        compact_interval_secs: cfg.compact_interval_secs,
        max_batch_files: cfg.max_batch_files,
        verbose: false,
        log_to_stderr: false,
        status_tx: Some(watch_status_tx),
    };

    thread::spawn(move || {
        if let Err(err) = watch::run(watch_cfg) {
            let _ = watch_err_tx.send(err.to_string());
        }
    });

    let search_root = cfg.root.clone();
    let search_bundle = cfg.bundle_dir.clone();
    let search_max_results = cfg.max_results;
    thread::spawn(move || {
        let mut engine = SearchEngine::new(search_root, search_bundle, search_max_results);
        while let Ok(mut job) = search_rx.recv() {
            while let Ok(next) = search_rx.try_recv() {
                job = next;
            }
            let result = engine.search(job.id, &job.query);
            let _ = search_result_tx.send(result);
        }
    });

    enable_raw_mode().map_err(io::Error::other)?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).map_err(io::Error::other)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let app_result = run_ui_loop(
        &mut terminal,
        watch_status_rx,
        watch_err_rx,
        search_tx,
        search_result_rx,
    );

    disable_raw_mode().map_err(io::Error::other)?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen).map_err(io::Error::other)?;
    terminal.show_cursor()?;

    app_result
}

fn run_ui_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    watch_status_rx: mpsc::Receiver<WatchStatus>,
    watch_err_rx: mpsc::Receiver<String>,
    search_tx: mpsc::Sender<SearchJob>,
    search_result_rx: mpsc::Receiver<SearchResult>,
) -> io::Result<()> {
    let mut app = App::new();
    app.queue_search();

    loop {
        while let Ok(status) = watch_status_rx.try_recv() {
            app.last_watch_status = status;
            app.last_watch_update = Instant::now();
        }

        while let Ok(err) = watch_err_rx.try_recv() {
            app.last_error = Some(format!("watch error: {err}"));
        }

        while let Ok(result) = search_result_rx.try_recv() {
            if result.id < app.applied_query_id {
                continue;
            }
            app.applied_query_id = result.id;
            app.rendered_rows = result.rendered_rows;
            app.result_count = result.result_count;
            app.candidate_count = result.candidate_count;
            app.elapsed_ms = result.elapsed_ms;
            app.backend = result.backend;
            app.last_error = result.error;
        }

        if app.search_ready() {
            if let Some(id) = app.pending_query_id() {
                let _ = search_tx.send(SearchJob {
                    id,
                    query: app.query.clone(),
                });
                app.search_queued_at = None;
            }
        }

        terminal.draw(|f| render_ui(f, &app))?;

        if event::poll(Duration::from_millis(30)).map_err(io::Error::other)? {
            let ev = event::read().map_err(io::Error::other)?;
            if let Event::Key(key) = ev {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                if key.modifiers == KeyModifiers::CONTROL && key.code == KeyCode::Char('c') {
                    break;
                }
                match key.code {
                    KeyCode::Esc => break,
                    KeyCode::Char('q') if key.modifiers == KeyModifiers::NONE => break,
                    KeyCode::Backspace => {
                        app.query.pop();
                        app.queue_search();
                    }
                    KeyCode::Char(c)
                        if key.modifiers == KeyModifiers::NONE
                            || key.modifiers == KeyModifiers::SHIFT =>
                    {
                        app.query.push(c);
                        app.queue_search();
                    }
                    KeyCode::Down => {
                        app.scroll = app.scroll.saturating_add(1);
                    }
                    KeyCode::Up => {
                        app.scroll = app.scroll.saturating_sub(1);
                    }
                    KeyCode::PageDown => {
                        app.scroll = app.scroll.saturating_add(12);
                    }
                    KeyCode::PageUp => {
                        app.scroll = app.scroll.saturating_sub(12);
                    }
                    KeyCode::Home => {
                        app.scroll = 0;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

fn render_ui(frame: &mut ratatui::Frame<'_>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(frame.size());

    let input_block = Block::default()
        .title(Line::from(vec![Span::styled(
            " ISEARCH LIVE ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )]))
        .borders(Borders::ALL)
        .border_set(border::THICK)
        .border_style(Style::default().fg(Color::Green));

    let input_text = if app.query.is_empty() {
        Line::from(vec![Span::styled(
            "type to search (results start at 3 chars)",
            Style::default().fg(Color::DarkGray),
        )])
    } else {
        Line::from(vec![Span::styled(
            app.query.as_str(),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )])
    };

    frame.render_widget(Clear, chunks[0]);
    let input = Paragraph::new(input_text).block(input_block);
    frame.render_widget(input, chunks[0]);

    let result_block = Block::default()
        .title(Span::styled(
            " Results ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_set(border::DOUBLE)
        .border_style(Style::default().fg(Color::Cyan));

    let results_view_height = chunks[1].height.saturating_sub(2) as usize;
    let available = app.rendered_rows.len();
    let max_scroll = available.saturating_sub(results_view_height);
    let scroll = app.scroll.min(max_scroll);
    let visible: Vec<Line<'_>> = app
        .rendered_rows
        .iter()
        .skip(scroll)
        .take(results_view_height)
        .map(|row| render_result_row(row, &app.query))
        .collect();

    let main_panel = if visible.is_empty() {
        if app.query.len() < 3 {
            Paragraph::new(Line::from(Span::styled(
                "waiting for 3+ chars...",
                Style::default().fg(Color::DarkGray),
            )))
        } else {
            Paragraph::new(Line::from(Span::styled(
                "no matches",
                Style::default().fg(Color::DarkGray),
            )))
        }
    } else {
        Paragraph::new(visible)
    }
    .block(result_block)
    .wrap(Wrap { trim: false });

    frame.render_widget(Clear, chunks[1]);
    frame.render_widget(main_panel, chunks[1]);

    let phase = match app.last_watch_status.phase {
        WatchPhase::Bootstrapping => "indexing",
        WatchPhase::Idle => "idle",
        WatchPhase::Updating => "updating",
        WatchPhase::Compacting => "compacting",
    };

    let phase_style = match app.last_watch_status.phase {
        WatchPhase::Bootstrapping => Style::default().fg(Color::Yellow),
        WatchPhase::Idle => Style::default().fg(Color::Green),
        WatchPhase::Updating => Style::default()
            .fg(Color::LightYellow)
            .add_modifier(Modifier::BOLD),
        WatchPhase::Compacting => Style::default()
            .fg(Color::Magenta)
            .add_modifier(Modifier::BOLD),
    };

    let mut status_segments = vec![
        Span::styled("watch:", Style::default().fg(Color::DarkGray)),
        Span::raw(" "),
        Span::styled(phase, phase_style),
        Span::raw("  |  "),
        Span::styled("search:", Style::default().fg(Color::DarkGray)),
        Span::raw(" "),
        Span::styled(
            format!(
                "{} hits / {} candidates ({})",
                app.result_count, app.candidate_count, app.backend
            ),
            Style::default().fg(Color::Cyan),
        ),
    ];

    if app.last_watch_status.phase == WatchPhase::Updating {
        status_segments.push(Span::raw("  |  "));
        status_segments.push(Span::styled(
            format!(
                "Δ paths={} ops={}",
                app.last_watch_status.changed_paths, app.last_watch_status.delta_ops
            ),
            Style::default().fg(Color::Yellow),
        ));
    }

    if let Some(err) = &app.last_error {
        status_segments.push(Span::raw("  |  "));
        status_segments.push(Span::styled(err.as_str(), Style::default().fg(Color::Red)));
    }

    frame.render_widget(Clear, chunks[2]);
    let status = Paragraph::new(Line::from(status_segments)).style(
        Style::default()
            .fg(Color::Green)
            .bg(Color::Black)
            .add_modifier(Modifier::BOLD),
    );
    frame.render_widget(status, chunks[2]);
}

struct SearchEngine {
    root: PathBuf,
    bundle_dir: PathBuf,
    max_results: usize,
    mmap_bundle: Option<MmapBundle>,
    mmap_paths: Vec<String>,
}

impl SearchEngine {
    fn new(root: PathBuf, bundle_dir: PathBuf, max_results: usize) -> Self {
        Self {
            root,
            bundle_dir,
            max_results,
            mmap_bundle: None,
            mmap_paths: Vec::new(),
        }
    }

    fn search(&mut self, id: u64, query: &str) -> SearchResult {
        let t0 = Instant::now();
        if query.len() < 3 {
            return SearchResult {
                id,
                rendered_rows: Vec::new(),
                result_count: 0,
                candidate_count: 0,
                elapsed_ms: 0.0,
                backend: "idle",
                error: None,
            };
        }

        match self.search_impl(query) {
            Ok((rows, result_count, candidate_count, backend)) => SearchResult {
                id,
                rendered_rows: rows,
                result_count,
                candidate_count,
                elapsed_ms: t0.elapsed().as_secs_f64() * 1000.0,
                backend,
                error: None,
            },
            Err(err) => SearchResult {
                id,
                rendered_rows: Vec::new(),
                result_count: 0,
                candidate_count: 0,
                elapsed_ms: t0.elapsed().as_secs_f64() * 1000.0,
                backend: "error",
                error: Some(err.to_string()),
            },
        }
    }

    fn search_impl(
        &mut self,
        query: &str,
    ) -> io::Result<(Vec<ResultRow>, usize, usize, &'static str)> {
        let query_bytes = query.as_bytes();
        let covering = ngram::covering_ngrams(query_bytes);
        let hashes: Vec<u32> = covering.iter().map(|ng| ngram::hash_ngram(ng)).collect();

        let (verify_results, candidate_count, backend) =
            if let Some(docs) = watch::load_query_docs(&self.bundle_dir)? {
                let mut path_map = HashMap::with_capacity(docs.len());
                let mut candidates = Vec::new();
                for (doc_id, path, doc_hashes) in docs {
                    if hashes.iter().all(|h| doc_hashes.binary_search(h).is_ok()) {
                        candidates.push(DocId(doc_id));
                    }
                    path_map.insert(doc_id, path);
                }
                let candidate_count = candidates.len();
                let candidate_pairs: Vec<(DocId, String)> = candidates
                    .into_iter()
                    .take(MAX_VERIFY_DOCS_PER_QUERY)
                    .filter_map(|doc| path_map.get(&doc.0).map(|p| (doc, p.clone())))
                    .collect();
                (
                    verify::verify_doc_paths_parallel(&candidate_pairs, query_bytes),
                    candidate_count,
                    "watch-state",
                )
            } else {
                self.ensure_mmap_loaded()?;
                let bundle = self
                    .mmap_bundle
                    .as_ref()
                    .ok_or_else(|| io::Error::other("mmap bundle unavailable"))?;
                let (candidates, PostingsReadTimings { .. }) = bundle.candidates(&hashes)?;
                let candidate_count = candidates.len();
                let limited: Vec<DocId> = candidates
                    .into_iter()
                    .take(MAX_VERIFY_DOCS_PER_QUERY)
                    .collect();
                (
                    verify::verify_candidates_parallel(&limited, &self.mmap_paths, query_bytes),
                    candidate_count,
                    "mmap",
                )
            };

        let mut rows = Vec::with_capacity(self.max_results);
        let mut total_hits = 0usize;
        for file in &verify_results {
            let path = query_result_path_display(&file.rel_path, &self.root);
            let mut wrote_header = false;
            for hit in &file.hits {
                total_hits = total_hits.saturating_add(1);
                if rows.len() >= self.max_results {
                    break;
                }
                if !wrote_header {
                    rows.push(ResultRow::FileHeader(format!("{path}:")));
                    wrote_header = true;
                    if rows.len() >= self.max_results {
                        break;
                    }
                }
                rows.push(ResultRow::Hit {
                    line_no: hit.line_no,
                    line: sanitize_for_tui(&hit.line),
                });
            }
            if rows.len() >= self.max_results {
                break;
            }
        }

        Ok((rows, total_hits, candidate_count, backend))
    }

    fn ensure_mmap_loaded(&mut self) -> io::Result<()> {
        if self.mmap_bundle.is_some() {
            return Ok(());
        }
        let (bundle, paths, _) = MmapBundle::open(&self.bundle_dir)?;
        self.mmap_bundle = Some(bundle);
        self.mmap_paths = paths;
        Ok(())
    }
}

fn query_result_path_display(file_path: &str, root: &Path) -> String {
    let p = Path::new(file_path);
    if let Ok(rel) = p.strip_prefix(root) {
        let s = rel.to_string_lossy();
        if s.is_empty() {
            "./".to_string()
        } else {
            format!("./{}", s.replace('\\', "/"))
        }
    } else {
        p.to_string_lossy().replace('\\', "/")
    }
}

fn sanitize_for_tui(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch == '\t' {
            out.push(' ');
            out.push(' ');
            out.push(' ');
            out.push(' ');
        } else if ch.is_control() {
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    out
}

fn render_result_row(row: &ResultRow, query: &str) -> Line<'static> {
    match row {
        ResultRow::FileHeader(path) => Line::from(Span::styled(
            path.clone(),
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        )),
        ResultRow::Hit { line_no, line } => {
            let mut spans = Vec::new();
            spans.push(Span::styled(
                format!("  {:>6}: ", line_no),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));
            spans.extend(highlight_spans(
                line,
                query,
                Style::default().fg(Color::White),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ));
            Line::from(spans)
        }
    }
}

fn highlight_spans(
    text: &str,
    query: &str,
    base_style: Style,
    hit_style: Style,
) -> Vec<Span<'static>> {
    if query.is_empty() {
        return vec![Span::styled(text.to_owned(), base_style)];
    }
    let mut spans = Vec::new();
    let mut cur = 0usize;
    while let Some(rel) = text[cur..].find(query) {
        let at = cur + rel;
        if at > cur {
            spans.push(Span::styled(text[cur..at].to_owned(), base_style));
        }
        let end = at + query.len();
        spans.push(Span::styled(text[at..end].to_owned(), hit_style));
        cur = end;
    }
    if cur < text.len() {
        spans.push(Span::styled(text[cur..].to_owned(), base_style));
    }
    if spans.is_empty() {
        spans.push(Span::styled(text.to_owned(), base_style));
    }
    spans
}
