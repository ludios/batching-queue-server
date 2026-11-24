//! The lock on the state file, with real server processes.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, ChildStderr, Command, Stdio};

/// Returns a TCP port that is free at the moment.
fn free_port() -> u16 {
    std::net::TcpListener::bind("[::]:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Starts the server binary for the item file at `item_file` on `port`, with stderr piped.
fn spawn_on(item_file: &Path, port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_batching-queue-server"))
        .arg("--port")
        .arg(port.to_string())
        .arg(item_file)
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Starts the server binary for the item file at `item_file`, with stderr piped.
fn spawn(item_file: &Path) -> Child {
    spawn_on(item_file, free_port())
}

/// Sends SIGTERM to `child`.
fn terminate(child: &Child) {
    let status = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(status.success());
}

/// Reads lines from `stderr` until the line that says the server has started, and returns it.
fn startup_line(stderr: &mut BufReader<ChildStderr>) -> String {
    let mut line = String::new();
    stderr.read_line(&mut line).unwrap();
    assert!(
        line.contains("the state file"),
        "the server did not start: {line:?}"
    );
    line
}

/// Starts a server and waits until it has started.
///
/// Returns the server and its stderr.
fn start(item_file: &Path) -> (Child, BufReader<ChildStderr>) {
    let mut child = spawn(item_file);
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    startup_line(&mut stderr);
    (child, stderr)
}

#[test]
fn a_killed_server_leaves_no_lock_behind() {
    let dir = tempfile::tempdir().unwrap();
    let item_file = dir.path().join("items.txt");
    fs::write(&item_file, "a\nb\n").unwrap();

    let (mut first, _first_stderr) = start(&item_file);
    let second = spawn(&item_file).wait_with_output().unwrap();
    assert_eq!(second.status.code(), Some(1));
    let message = String::from_utf8_lossy(&second.stderr);
    assert!(
        message.contains("a running server is using the state file"),
        "{message}"
    );

    first.kill().unwrap();
    first.wait().unwrap();
    assert!(dir.path().join("items.txt.bqs.json.lock").exists());

    let (mut third, mut third_stderr) = start(&item_file);
    terminate(&third);
    assert_eq!(third.wait().unwrap().code(), Some(0));
    let mut rest = String::new();
    third_stderr.read_to_string(&mut rest).unwrap();
    assert!(rest.contains("received SIGTERM"), "{rest}");
}

#[test]
fn a_server_whose_stderr_is_gone_still_exits_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let item_file = dir.path().join("items.txt");
    fs::write(&item_file, "a\nb\n").unwrap();
    let port = free_port();
    let mut server = spawn_on(&item_file, port);
    let mut stderr = BufReader::new(server.stderr.take().unwrap());
    startup_line(&mut stderr);
    // As when the terminal window is closed: every later write to stderr fails
    drop(stderr);

    // Logs a line, which fails
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.ends_with("\r\n\r\na\n"), "{response:?}");

    terminate(&server);
    assert_eq!(server.wait().unwrap().code(), Some(0));
    let state = fs::read_to_string(dir.path().join("items.txt.bqs.json")).unwrap();
    assert!(state.contains("\"position\": 1,"), "{state}");
}
