//! batching-queue-server hands out the lines of a text file to HTTP clients, in file order, a
//! batch per request.

mod http;
mod queue;
mod state;
mod tui;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use queue::Queue;
use socket2::{Domain, Protocol, Socket, Type};
use state::{Shared, State};
use std::fs::{self, File};
use std::io::{IsTerminal, Write};
use std::net::{Ipv6Addr, SocketAddr};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

/// Checks a `--path` value: it must start with `/`, contain only `/` and the characters that
/// clients send unchanged, and have no segment that is `.` or `..`.
fn parse_path(value: &str) -> Result<String, String> {
    let Some(rest) = value.strip_prefix('/') else {
        return Err("it must start with /".to_owned());
    };
    let allowed = |c: char| c.is_ascii_alphanumeric() || "/-_.~".contains(c);
    if let Some(c) = value.chars().find(|&c| !allowed(c)) {
        return Err(format!(
            "it must not contain {c:?}; only / and the characters A-Z a-z 0-9 - _ . ~ are allowed"
        ));
    }
    if rest
        .split('/')
        .any(|segment| segment == "." || segment == "..")
    {
        return Err("it must not have a segment that is . or ..".to_owned());
    }
    Ok(value.to_owned())
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// The file whose lines are handed out
    item_file: PathBuf,

    /// Where the state file is [default: the ITEM_FILE path with .bqs.json appended]
    #[arg(long, value_name = "FILE")]
    state_file: Option<PathBuf>,

    /// The TCP port to listen on, for both IPv4 and IPv6
    #[arg(long, default_value_t = 31417, value_parser = clap::value_parser!(u16).range(1..))]
    port: u16,

    /// After the last line of the file is handed out, continue with its first line again
    #[arg(long)]
    cycle: bool,

    /// The most lines in one response
    #[arg(long, value_name = "N", default_value_t = 10000, value_parser = clap::value_parser!(u64).range(1..))]
    max_batch: u64,

    /// The only request path at which lines are handed out; every other path gets 403
    #[arg(long, default_value = "/", value_parser = parse_path)]
    path: String,
}

impl Args {
    fn state_file(&self) -> PathBuf {
        match &self.state_file {
            Some(state_file) => state_file.clone(),
            None => state::with_suffix(&self.item_file, ".bqs.json"),
        }
    }
}

/// Whether `a` and `b` are paths of the same existing file, also through a symlink or a hard
/// link.
fn same_file(a: &Path, b: &Path) -> bool {
    match (fs::metadata(a), fs::metadata(b)) {
        (Ok(a), Ok(b)) => (a.dev(), a.ino()) == (b.dev(), b.ino()),
        _ => false,
    }
}

/// Binds `port` on every IPv4 and IPv6 address, without accepting connections yet.
fn bind(port: u16) -> Result<TcpListener> {
    let bind = || -> std::io::Result<TcpListener> {
        let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_only_v6(false)?;
        // As `std::net::TcpListener::bind` does, so that a restart can bind the port while
        // connections of the previous run are in TIME_WAIT
        socket.set_reuse_address(true)?;
        socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
        socket.listen(1024)?;
        socket.set_nonblocking(true)?;
        TcpListener::from_std(socket.into())
    };
    bind().with_context(|| format!("cannot listen on port {port}"))
}

/// A server that has passed its startup checks and bound its port, but does not accept
/// connections yet.
struct Server {
    shared: Arc<Shared>,
    listener: TcpListener,
    router: axum::Router,
    /// Receives the errors that must stop the server.
    fatal: UnboundedReceiver<anyhow::Error>,
    /// Sends to `fatal`.
    fatal_sender: UnboundedSender<anyhow::Error>,
    /// The open lock file, which keeps the lock of the state file until the server is dropped.
    _lock: File,
}

/// Writes `line`, after the name of the program, to stderr. Unlike `eprintln!`, does not panic
/// (which aborts the server) when stderr is gone, as it is after the terminal window is closed.
fn print_line(line: &str) {
    let _ = writeln!(std::io::stderr(), "batching-queue-server: {line}");
}

/// Does the startup checks, loads the state, binds the port, and saves the state once.
fn start(args: &Args) -> Result<Server> {
    let state_file = args.state_file();
    // The server writes these files (the temporary one at every save)
    let own_files = [
        ("state file", state_file.clone()),
        ("lock file", state::lock_path(&state_file)),
        ("temporary state file", state::temporary_path(&state_file)),
    ];
    for (name, path) in &own_files {
        if same_file(path, &args.item_file) {
            bail!("the {name} {} is the item file", path.display());
        }
    }
    let lock = state::lock(&state_file)?;
    let loaded = state::load(&state_file)?;
    let created = loaded.is_none();
    let loaded = loaded.unwrap_or_default();
    let queue = Queue::open(&args.item_file, loaded.position, args.cycle)?;
    let listener = bind(args.port)?;

    let total = queue.total();
    let state = State::new(queue, loaded.wraps, loaded.clients, loaded.view);
    let shared = Arc::new(Shared::new(state, state_file));
    shared.save()?;

    let cycling = if args.cycle { "on" } else { "off" };
    let action = if created { "created" } else { "loaded" };
    print_line(&format!(
        "{action} the state file {}: position {} of {total} lines; cycling is {cycling}",
        shared.path().display(),
        loaded.position
    ));

    let (fatal_sender, fatal) = mpsc::unbounded_channel();
    let config = http::Config {
        path: args.path.clone(),
        max_batch: args.max_batch,
    };
    let router = http::router(shared.clone(), config, fatal_sender.clone());
    Ok(Server {
        shared,
        listener,
        router,
        fatal,
        fatal_sender,
        _lock: lock,
    })
}

/// Serves requests until `exit` completes or a failure stops the server, then stops handing out
/// lines and saves the state (unless a failed save is what stopped the server).
///
/// Returns an error if a failure stopped the server or the last save failed.
async fn serve(server: Server, exit: impl Future<Output = ()>) -> Result<()> {
    let Server {
        shared,
        listener,
        router,
        mut fatal,
        fatal_sender: _,
        _lock,
    } = server;
    let service = router.into_make_service_with_connect_info::<SocketAddr>();
    let mut save = true;
    let failure = tokio::select! {
        () = exit => None,
        error = state::save_periodically(shared.clone()) => {
            save = false;
            Some(error)
        }
        Some(error) = fatal.recv() => Some(error),
        result = axum::serve(listener, service) => Some(match result {
            Ok(()) => anyhow!("the HTTP server stopped"),
            Err(error) => anyhow!(error).context("the HTTP server failed"),
        }),
    };
    shared.state.lock().unwrap().stop();

    if save && let Err(save_error) = state::save_async(&shared).await {
        return Err(match failure {
            None => save_error,
            Some(failure) => anyhow!("{failure:#}\nthen: {save_error:#}"),
        });
    }
    match failure {
        None => Ok(()),
        Some(failure) => Err(failure),
    }
}

/// Starts listening for SIGINT, SIGTERM, and SIGHUP.
///
/// Returns a future that completes when one of them arrives.
fn exit_signal() -> Result<impl Future<Output = ()>> {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut hangup = signal(SignalKind::hangup())?;
    Ok(async move {
        let name = tokio::select! {
            _ = interrupt.recv() => "SIGINT",
            _ = terminate.recv() => "SIGTERM",
            _ = hangup.recv() => "SIGHUP",
        };
        tracing::info!("received {name}; exiting");
    })
}

/// Switches the terminal to the dashboard of `server`, which was started with `args`.
///
/// `quit` receives a message when the operator chooses to quit.
fn start_dashboard(
    args: &Args,
    server: &Server,
    quit: UnboundedSender<()>,
) -> Result<tui::Running> {
    let item_file = &args.item_file;
    let name = item_file.file_name().unwrap_or(item_file.as_os_str());
    let config = tui::Config {
        name: state::clean_for_terminal(name.as_bytes(), usize::MAX),
        port: server.listener.local_addr()?.port(),
        cycle: args.cycle,
    };
    let fatal = server.fatal_sender.clone();
    tui::start(server.shared.clone(), config, quit, fatal).context("cannot start the dashboard")
}

fn main() {
    let args = Args::parse();
    let show_dashboard = std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
    // With the dashboard, nothing else may write to the terminal
    if !show_dashboard {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_target(false)
            // Otherwise a failed write to stderr is reported with `eprintln!`, which panics
            // when stderr is gone
            .log_internal_errors(false)
            .init();
    }
    let runtime = tokio::runtime::Runtime::new().expect("the tokio runtime starts");
    let mut dashboard = None;
    let result = runtime.block_on(async {
        let server = start(&args)?;
        // Only now, so that until then (during a long scan of the item file, for example) these
        // signals end the process at once, which loses nothing
        let signal = exit_signal().context("cannot listen for signals")?;
        let (quit_sender, mut quit) = mpsc::unbounded_channel();
        if show_dashboard {
            dashboard = Some(start_dashboard(&args, &server, quit_sender)?);
        }
        let exit = async move {
            tokio::select! {
                () = signal => {}
                Some(()) = quit.recv() => {}
            }
        };
        serve(server, exit).await
    });
    // After the final save, so that a terminal that is gone cannot prevent it
    if let Some(dashboard) = dashboard {
        dashboard.finish();
    }
    if let Err(error) = result {
        print_line(&format!("{error:#}"));
        std::process::exit(1);
    }
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    /// Writes `contents` to `items.txt` in a new temporary directory.
    ///
    /// Returns the directory, which is removed when dropped, and the arguments for a server of
    /// that item file, on a port that the system chooses.
    fn setup(contents: &str) -> (TempDir, Args) {
        let dir = tempfile::tempdir().unwrap();
        let item_file = dir.path().join("items.txt");
        fs::write(&item_file, contents).unwrap();
        let args = Args {
            item_file,
            state_file: None,
            port: 0,
            cycle: false,
            max_batch: 10000,
            path: "/".to_owned(),
        };
        (dir, args)
    }

    /// A server running in the background.
    struct Running {
        port: u16,
        exit: oneshot::Sender<()>,
        task: JoinHandle<Result<()>>,
    }

    impl Running {
        fn start(args: &Args) -> Running {
            let server = start(args).unwrap();
            let port = server.listener.local_addr().unwrap().port();
            let (exit, exited) = oneshot::channel();
            let task = tokio::spawn(serve(server, async {
                // Dropping `exit` counts as exiting too
                let _ = exited.await;
            }));
            Running { port, exit, task }
        }

        /// Sends `request`, a complete HTTP/1.1 request, through a new connection.
        ///
        /// Returns the status code and the body of the response.
        async fn send(&self, request: &str) -> (u16, String) {
            let mut stream = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            let status = response[9..12].parse().unwrap();
            let (_, body) = response.split_once("\r\n\r\n").unwrap();
            (status, body.to_owned())
        }

        async fn get(&self, target: &str) -> (u16, String) {
            self.send(&format!(
                "GET {target} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n"
            ))
            .await
        }

        /// Exits cleanly and returns what `serve` returned.
        async fn exit(self) -> Result<()> {
            self.exit.send(()).unwrap();
            self.task.await.unwrap()
        }
    }

    fn start_error(args: &Args) -> String {
        format!("{:#}", start(args).err().unwrap())
    }

    #[test]
    fn arguments() {
        let args = Args::try_parse_from(["bqs", "/data/items.txt"]).unwrap();
        assert_eq!(args.state_file(), Path::new("/data/items.txt.bqs.json"));
        assert_eq!(
            (args.port, args.cycle, args.max_batch),
            (31417, false, 10000)
        );
        assert_eq!(args.path, "/");

        let args = Args::try_parse_from(["bqs", "--state-file", "s.json", "items.txt"]).unwrap();
        assert_eq!(args.state_file(), Path::new("s.json"));

        for valid in ["/", "/secret", "/a/b-c_d.e~f/", "/...", "/.a", "//"] {
            let args = Args::try_parse_from(["bqs", "--path", valid, "items.txt"]).unwrap();
            assert_eq!(args.path, valid);
        }
        for invalid in [
            &["bqs", "items.txt", "position.txt"][..],
            &["bqs", "--path", "secret", "items.txt"],
            &["bqs", "--path", "/secret?x", "items.txt"],
            &["bqs", "--path", "/se%63ret", "items.txt"],
            &["bqs", "--path", "/a b", "items.txt"],
            &["bqs", "--path", "/a/../b", "items.txt"],
            &["bqs", "--path", "/.", "items.txt"],
            &["bqs", "--path", "/é", "items.txt"],
            &["bqs", "--port", "0", "items.txt"],
            &["bqs", "--max-batch", "0", "items.txt"],
        ] {
            let error = Args::try_parse_from(invalid).err().unwrap();
            assert_eq!(error.exit_code(), 2, "{invalid:?}");
        }
    }

    #[tokio::test]
    async fn startup_errors() {
        let (dir, mut args) = setup("a\nb\n");
        let state_file = args.state_file();

        for (contents, error) in [
            ("", "is not valid"),
            ("{", "is not valid"),
            ("1234\n", "is not valid"),
            (r#"{"version": 2}"#, "has version 2"),
            (
                r#"{"version": 1, "position": 0, "wraps": 0}"#,
                "missing field `clients`",
            ),
            (
                r#"{"version": 1, "position": 3, "wraps": 0, "clients": {}}"#,
                "position 3 is beyond the end",
            ),
        ] {
            fs::write(&state_file, contents).unwrap();
            let message = start_error(&args);
            assert!(message.contains(error), "{contents:?} gave {message:?}");
            assert_eq!(fs::read_to_string(&state_file).unwrap(), contents);
        }

        fs::remove_file(&state_file).unwrap();
        fs::create_dir(&state_file).unwrap();
        assert!(start_error(&args).contains("cannot read the state file"));

        // A state file "items" has the lock file "items.lock" and the temporary file "items.tmp"
        let hard_link = dir.path().join("hard-link");
        fs::hard_link(&args.item_file, &hard_link).unwrap();
        for (state_file, item_file, name) in [
            ("items.txt", "items.txt", "the state file"),
            ("hard-link", "items.txt", "the state file"),
            ("items", "items.lock", "the lock file"),
            ("items", "items.tmp", "the temporary state file"),
        ] {
            fs::rename(&args.item_file, dir.path().join(item_file)).unwrap();
            args.item_file = dir.path().join(item_file);
            args.state_file = Some(dir.path().join(state_file));
            let message = start_error(&args);
            assert!(message.contains(&format!("{name} ")), "{message}");
            assert!(message.contains("is the item file"), "{message}");
            assert_eq!(fs::read_to_string(&args.item_file).unwrap(), "a\nb\n");
        }

        args.state_file = Some(dir.path().join("missing").join("state.json"));
        assert!(start_error(&args).contains("cannot open the lock file"));

        args.state_file = None;
        args.item_file = dir.path().join("missing.txt");
        assert!(start_error(&args).contains("cannot open the item file"));
    }

    #[tokio::test]
    async fn a_port_that_cannot_be_bound_changes_nothing() {
        let (_dir, mut args) = setup("a\nb\n");
        let contents = r#"{"version": 1, "position": 1, "wraps": 0, "clients": {}}"#;
        fs::write(args.state_file(), contents).unwrap();
        let taken = std::net::TcpListener::bind("[::]:0").unwrap();
        args.port = taken.local_addr().unwrap().port();
        assert!(start_error(&args).contains("cannot listen on port"));
        assert_eq!(fs::read_to_string(args.state_file()).unwrap(), contents);
    }

    #[tokio::test]
    async fn a_second_server_with_the_same_state_file_does_not_start() {
        let (_dir, args) = setup("a\nb\nc\n");
        let first = Running::start(&args);
        assert_eq!(first.get("/").await, (200, "a\n".to_owned()));
        let saved = fs::read_to_string(args.state_file()).unwrap();

        // Without its item file, the second server would fail differently if it got past the
        // lock. (It also gets a port of its own.)
        let moved = args.item_file.with_extension("moved");
        fs::rename(&args.item_file, &moved).unwrap();
        let error = start_error(&args);
        assert!(
            error.contains("a running server is using the state file"),
            "{error}"
        );
        fs::rename(&moved, &args.item_file).unwrap();
        assert_eq!(fs::read_to_string(args.state_file()).unwrap(), saved);
        assert_eq!(first.get("/").await, (200, "b\n".to_owned()));
        first.exit().await.unwrap();

        // The lock is released with the server, although the lock file stays
        let second = Running::start(&args);
        assert_eq!(second.get("/").await, (200, "c\n".to_owned()));
    }

    #[tokio::test]
    async fn get_and_head_through_tcp() {
        let (_dir, args) = setup("a\nb\n");
        let server = Running::start(&args);
        assert_eq!(server.get("/?max=1").await, (200, "a\n".to_owned()));
        let head = "HEAD / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
        assert_eq!(server.send(head).await.0, 405);
        assert_eq!(server.get("/?max=5").await, (200, "b\n".to_owned()));
        server.exit().await.unwrap();

        let clients = state::load(&args.state_file()).unwrap().unwrap().clients;
        let client = &clients[&"127.0.0.1".parse().unwrap()];
        assert_eq!((clients.len(), client.requests, client.items), (1, 3, 2));
    }

    #[tokio::test]
    async fn a_restart_continues_with_the_next_line() {
        let (_dir, mut args) = setup("a\nb\nc\n");
        args.cycle = true;
        let server = Running::start(&args);
        assert_eq!(server.get("/?max=2").await, (200, "a\nb\n".to_owned()));
        server.exit().await.unwrap();

        let server = Running::start(&args);
        assert_eq!(server.get("/?max=1").await, (200, "c\n".to_owned()));
        // This request reaches the end and wraps
        assert_eq!(server.get("/?max=5").await, (200, String::new()));
        assert_eq!(server.get("/?max=1").await, (200, "a\n".to_owned()));
        server.exit().await.unwrap();

        let loaded = state::load(&args.state_file()).unwrap().unwrap();
        assert_eq!((loaded.position, loaded.wraps), (1, 1));
        let server = Running::start(&args);
        assert_eq!(server.get("/").await, (200, "b\n".to_owned()));
    }

    #[tokio::test]
    async fn a_server_without_a_dashboard_keeps_the_saved_view_settings() {
        let (_dir, args) = setup("a\nb\n");
        let view = r#"{"sort_column": "last_seen", "sort_inverted": true, "show_host": false, "show_ua": true}"#;
        fs::write(
            args.state_file(),
            format!(
                r#"{{"version": 1, "position": 0, "wraps": 0, "view": {view}, "clients": {{}}}}"#
            ),
        )
        .unwrap();
        let server = Running::start(&args);
        assert_eq!(server.get("/").await, (200, "a\n".to_owned()));
        server.exit().await.unwrap();
        let loaded = state::load(&args.state_file()).unwrap().unwrap();
        assert_eq!(
            loaded.view,
            state::View {
                sort_column: state::Column::Last,
                sort_inverted: true,
                show_host: false,
                show_ua: true,
            }
        );
    }

    #[tokio::test]
    async fn a_block_and_the_counters_survive_a_restart() {
        let (_dir, args) = setup("a\nb\n");
        let server = Running::start(&args);
        assert_eq!(server.get("/").await, (200, "a\n".to_owned()));
        server.exit().await.unwrap();

        // As the operator blocks a client while the server is stopped
        let contents = fs::read_to_string(args.state_file()).unwrap();
        let blocked = contents.replace(r#""blocked": false"#, r#""blocked": true"#);
        assert_ne!(blocked, contents);
        fs::write(args.state_file(), blocked).unwrap();

        let server = Running::start(&args);
        assert_eq!(server.get("/").await, (403, String::new()));
        server.exit().await.unwrap();
        let loaded = state::load(&args.state_file()).unwrap().unwrap();
        let client = &loaded.clients[&"127.0.0.1".parse().unwrap()];
        assert_eq!(loaded.position, 1);
        assert_eq!(
            (client.blocked, client.requests, client.items),
            (true, 2, 1)
        );
    }

    #[tokio::test]
    async fn without_cycling_the_position_stays_at_the_end() {
        let (_dir, args) = setup("a\nb\n");
        let server = Running::start(&args);
        assert_eq!(server.get("/?max=2").await, (200, "a\nb\n".to_owned()));
        assert_eq!(server.get("/?max=2").await, (200, String::new()));
        server.exit().await.unwrap();
        assert_eq!(
            state::load(&args.state_file()).unwrap().unwrap().position,
            2
        );

        let server = Running::start(&args);
        assert_eq!(server.get("/").await, (200, String::new()));
    }

    #[tokio::test]
    async fn a_start_from_an_older_snapshot_repeats_lines_and_skips_none() {
        let (_dir, args) = setup("a\nb\nc\nd\n");
        let server = Running::start(&args);
        assert_eq!(server.get("/").await, (200, "a\n".to_owned()));
        server.exit().await.unwrap();
        let snapshot = fs::read(args.state_file()).unwrap();

        let server = Running::start(&args);
        assert_eq!(server.get("/?max=2").await, (200, "b\nc\n".to_owned()));
        server.exit().await.unwrap();

        // As if the server had crashed before saving its hand-outs
        fs::write(args.state_file(), snapshot).unwrap();
        let server = Running::start(&args);
        assert_eq!(server.get("/?max=5").await, (200, "b\nc\nd\n".to_owned()));
    }
}
