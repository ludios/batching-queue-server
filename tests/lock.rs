//! The lock on the state file, and the exit of a server whose stderr is gone, with real server
//! processes.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, ChildStderr, Command, Stdio};
use std::time::Duration;

/// Returns a TCP port that is free at the moment.
fn free_port() -> u16 {
    std::net::TcpListener::bind("[::]:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A server process. It is killed when dropped, so that a failed assertion leaves none running.
struct Server {
    child: Child,
    /// The port that the server was told to listen on.
    port: u16,
    /// The server's stderr, until `close_stderr`.
    stderr: Option<BufReader<ChildStderr>>,
}

impl Server {
    /// Starts the server binary for the item file at `item_file`, on a port that is free at the
    /// moment.
    fn spawn(item_file: &Path) -> Server {
        let port = free_port();
        let mut child = Command::new(env!("CARGO_BIN_EXE_batching-queue-server"))
            .arg("--port")
            .arg(port.to_string())
            .arg(item_file)
            .stdin(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = Some(BufReader::new(child.stderr.take().unwrap()));
        Server {
            child,
            port,
            stderr,
        }
    }

    /// Reads one line from the server's stderr, and returns it.
    fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.stderr.as_mut().unwrap().read_line(&mut line).unwrap();
        line
    }

    /// Waits until the server has started, which its first line on stderr says.
    fn wait_until_started(&mut self) {
        let line = self.read_line();
        assert!(
            line.contains("the state file"),
            "the server did not start: {line:?}"
        );
    }

    /// Reads the server's stderr until it ends, and returns what it read.
    fn read_rest_of_stderr(&mut self) -> String {
        let mut rest = String::new();
        self.stderr
            .as_mut()
            .unwrap()
            .read_to_string(&mut rest)
            .unwrap();
        rest
    }

    /// Closes our end of the server's stderr, so that every later write to it fails, as after the
    /// terminal window is closed.
    fn close_stderr(&mut self) {
        self.stderr = None;
    }

    fn terminate(&self) {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// Waits up to 10 seconds for the server to exit.
    ///
    /// Returns its exit status code, or None if a signal ended it.
    fn exit_code(&mut self) -> Option<i32> {
        for _ in 0..1000 {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status.code();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("the server did not exit within 10 seconds");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Writes an item file with the lines "a" and "b" to `dir`, and returns its path.
fn item_file(dir: &Path) -> std::path::PathBuf {
    let item_file = dir.join("items.txt");
    fs::write(&item_file, "a\nb\n").unwrap();
    item_file
}

#[test]
fn a_killed_server_leaves_no_lock_behind() {
    let dir = tempfile::tempdir().unwrap();
    let item_file = item_file(dir.path());

    let mut first = Server::spawn(&item_file);
    first.wait_until_started();
    let mut second = Server::spawn(&item_file);
    let line = second.read_line();
    assert!(
        line.contains("a running server is using the state file"),
        "{line}"
    );
    assert_eq!(second.exit_code(), Some(1));

    first.child.kill().unwrap();
    assert_eq!(first.exit_code(), None);
    assert!(dir.path().join("items.txt.bqs.json.lock").exists());

    let mut third = Server::spawn(&item_file);
    third.wait_until_started();
    third.terminate();
    assert_eq!(third.exit_code(), Some(0));
    let rest = third.read_rest_of_stderr();
    assert!(rest.contains("received SIGTERM"), "{rest}");
}

#[test]
fn a_server_whose_stderr_is_gone_still_exits_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::spawn(&item_file(dir.path()));
    server.wait_until_started();
    server.close_stderr();

    // This request is logged, and the write of the log line fails
    let mut stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.ends_with("\r\n\r\na\n"), "{response:?}");

    server.terminate();
    assert_eq!(server.exit_code(), Some(0));
    let state = fs::read_to_string(dir.path().join("items.txt.bqs.json")).unwrap();
    assert!(state.contains("\"position\": 1,"), "{state}");
}
