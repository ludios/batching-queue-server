//! The HTTP interface: one handler, which hands out lines at one path.

use crate::state::{self, Outcome, Shared, State as ServerState};
use anyhow::Result;
use axum::Router;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::header::{ALLOW, CONTENT_TYPE, USER_AGENT};
use axum::http::request::Parts;
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use jiff::Timestamp;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;

/// How long a request waits for its reply when the client is blocked, the path is wrong, the
/// server is paused, or the queue has no line for it; this slows down clients that keep
/// hammering while blocked, keep guessing the path, or poll an empty queue.
pub const DELAY: Duration = Duration::from_secs(1);

pub struct Config {
    /// The only request path at which lines are handed out.
    pub path: String,
    /// The most lines in one response.
    pub max_batch: u64,
}

#[derive(Clone)]
struct App {
    shared: Arc<Shared>,
    config: Arc<Config>,
    /// Where the handler sends an error that must stop the server.
    fatal: UnboundedSender<anyhow::Error>,
}

/// The reply to one request.
#[derive(Debug, PartialEq, Eq)]
enum Reply {
    Blocked,
    WrongPath,
    MethodNotAllowed,
    /// The `max` parameter is invalid, for the reason given.
    BadMax(&'static str),
    Paused,
    /// Lines were handed out: each line followed by `\n`, and the number of lines (possibly 0).
    Lines(Vec<u8>, u64),
}

impl Reply {
    /// Returns what came of the request, as recorded and logged.
    fn outcome(&self) -> Outcome {
        match self {
            Reply::Blocked => Outcome::Blocked,
            Reply::WrongPath => Outcome::WrongPath,
            Reply::MethodNotAllowed => Outcome::Rejected(StatusCode::METHOD_NOT_ALLOWED.as_u16()),
            Reply::BadMax(_) => Outcome::Rejected(StatusCode::BAD_REQUEST.as_u16()),
            Reply::Paused => Outcome::Paused,
            Reply::Lines(_, lines) => Outcome::Lines(*lines),
        }
    }

    /// Waits out the delay of this reply, if it has one, and returns its response.
    async fn respond(self) -> Response {
        let text_plain = [(CONTENT_TYPE, "text/plain")];
        match self {
            // A stranger cannot tell these two apart
            Reply::Blocked | Reply::WrongPath => {
                tokio::time::sleep(DELAY).await;
                StatusCode::FORBIDDEN.into_response()
            }
            Reply::MethodNotAllowed => {
                (StatusCode::METHOD_NOT_ALLOWED, [(ALLOW, "GET")]).into_response()
            }
            Reply::BadMax(reason) => {
                (StatusCode::BAD_REQUEST, text_plain, format!("{reason}\n")).into_response()
            }
            // As if the queue were empty, so that the clients just idle
            Reply::Paused | Reply::Lines(_, 0) => {
                tokio::time::sleep(DELAY).await;
                (StatusCode::OK, text_plain).into_response()
            }
            Reply::Lines(body, _) => (StatusCode::OK, text_plain, body).into_response(),
        }
    }
}

const INVALID_MAX: &str =
    "max must be a whole number from 1 to 18446744073709551615, written with the digits 0 to 9";
const REPEATED_MAX: &str = "max must not be given more than once";

/// Parses the `max` parameter of the query string `query`.
///
/// Returns the number of lines that the client asks for at most, or the reason why `max` is
/// invalid.
fn parse_max(query: Option<&str>) -> Result<u64, &'static str> {
    let mut max = None;
    for (key, value) in form_urlencoded::parse(query.unwrap_or("").as_bytes()) {
        if key != "max" {
            continue;
        }
        if max.is_some() {
            return Err(REPEATED_MAX);
        }
        max = Some(value);
    }
    let Some(max) = max else {
        return Ok(1);
    };
    // `u64::from_str` would also accept a leading `+`
    if max.is_empty() || !max.bytes().all(|b| b.is_ascii_digit()) {
        return Err(INVALID_MAX);
    }
    match max.parse() {
        Ok(0) | Err(_) => Err(INVALID_MAX),
        Ok(n) => Ok(n),
    }
}

/// Decides the reply to the request `request` from the client at `ip` (in its canonical form)
/// at `now`, and counts the request in the client's record. Takes a batch if the reply hands
/// out lines.
///
/// Returns None if the server has stopped handing out lines, or an error if reading the item
/// file failed, after stopping the server.
fn decide(
    state: &mut ServerState,
    config: &Config,
    ip: IpAddr,
    now: Timestamp,
    request: &Parts,
) -> Result<Option<Reply>> {
    if state.is_stopped() {
        return Ok(None);
    }
    let user_agent = request.headers.get(USER_AGENT).map(HeaderValue::as_bytes);
    if state.record_request(ip, now, user_agent).blocked {
        return Ok(Some(Reply::Blocked));
    }
    if request.uri.path() != config.path {
        return Ok(Some(Reply::WrongPath));
    }
    if request.method != Method::GET {
        return Ok(Some(Reply::MethodNotAllowed));
    }
    let max = match parse_max(request.uri.query()) {
        Ok(max) => max,
        Err(reason) => return Ok(Some(Reply::BadMax(reason))),
    };
    if state.is_paused() {
        return Ok(Some(Reply::Paused));
    }
    let batch = match state.take(ip, max.min(config.max_batch)) {
        Ok(batch) => batch,
        Err(error) => {
            state.stop();
            return Err(error);
        }
    };
    Ok(Some(Reply::Lines(batch.body, batch.lines)))
}

/// Handles every request (see the HTTP contract in SPEC.md): `app` is the server, and `peer` is
/// the address of the client.
///
/// Never returns when the server has stopped handing out lines, or when reading the item file
/// failed (after sending the error to `app.fatal`), so that the request gets no response.
async fn handle(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let ip = peer.ip().to_canonical();
    // Taken apart rather than extracted as `Parts`, which would copy the headers, and dropped
    // before any delay
    let (request, _) = request.into_parts();
    let decided = {
        let mut state = app.shared.state.lock().unwrap();
        let now = state::now();
        let decided = decide(&mut state, &app.config, ip, now, &request);
        if let Ok(Some(reply)) = &decided {
            state.record_outcome(ip, now, Instant::now(), reply.outcome());
        }
        decided
    };
    drop(request);
    let reply = match decided {
        Ok(Some(reply)) => reply,
        Ok(None) => return std::future::pending().await,
        Err(error) => {
            // If main has stopped receiving, it is exiting already
            let _ = app.fatal.send(error);
            return std::future::pending().await;
        }
    };
    tracing::info!("{ip} {}", reply.outcome());
    reply.respond().await
}

/// Returns the router of the server, which needs to be served with connection info (see
/// `Router::into_make_service_with_connect_info`).
///
/// `shared` is the state of the server, and `config` its settings. `fatal` is where the handler sends an error that must stop the server: a failed read of the
/// item file. The handler then stops the state, and the request gets no response.
pub fn router(
    shared: Arc<Shared>,
    config: Config,
    fatal: UnboundedSender<anyhow::Error>,
) -> Router {
    let app = App {
        shared,
        config: Arc::new(config),
        fatal,
    };
    // A fallback rather than a route, so that every request, whatever its method and path,
    // reaches the handler, which compares the path itself
    Router::new().fallback(handle).with_state(app)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Queue;
    use crate::state::{Client, View};
    use axum::body::Body;
    use axum::http::{HeaderMap, Request};
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::TempDir;
    use tokio::time::Instant;
    use tower::ServiceExt;

    /// The server under test: its router and its state, for an item file of its own.
    struct TestServer {
        _dir: TempDir,
        shared: Arc<Shared>,
        router: Router,
    }

    /// Creates a server for an item file holding `contents`, with the `--path` value `path`,
    /// the `--max-batch` value `max_batch`, and cycling as `cycle` says.
    fn server(contents: &str, path: &str, max_batch: u64, cycle: bool) -> TestServer {
        let dir = tempfile::tempdir().unwrap();
        let item_file = dir.path().join("items.txt");
        fs::write(&item_file, contents).unwrap();
        let queue = Queue::open(&item_file, 0, cycle).unwrap();
        let shared = Arc::new(Shared::new(
            ServerState::new(queue, 0, BTreeMap::new(), View::default()),
            dir.path().join("items.txt.bqs.json"),
        ));
        let (fatal, _) = tokio::sync::mpsc::unbounded_channel();
        let config = Config {
            path: path.to_owned(),
            max_batch,
        };
        let router = router(shared.clone(), config, fatal);
        TestServer {
            _dir: dir,
            shared,
            router,
        }
    }

    /// The response to one request, and how long it took.
    struct TestResponse {
        status: StatusCode,
        headers: HeaderMap,
        body: Vec<u8>,
        elapsed: Duration,
    }

    impl TestServer {
        /// Sends a request with `method` for `target` (a path and a query string) from `ip`.
        async fn request(&self, ip: &str, method: Method, target: &str) -> TestResponse {
            let request = Request::builder()
                .method(method)
                .uri(target)
                .body(Body::empty())
                .unwrap();
            self.send(ip, request).await
        }

        /// Sends a GET request for `target` from `ip`, with a `User-Agent` header for each of
        /// `user_agents`.
        async fn get_with_user_agents(
            &self,
            ip: &str,
            target: &str,
            user_agents: &[&[u8]],
        ) -> TestResponse {
            let mut request = Request::builder().uri(target);
            for user_agent in user_agents {
                request = request.header(USER_AGENT, HeaderValue::from_bytes(user_agent).unwrap());
            }
            self.send(ip, request.body(Body::empty()).unwrap()).await
        }

        /// Sends `request` from `ip`.
        async fn send(&self, ip: &str, mut request: Request<Body>) -> TestResponse {
            let peer = SocketAddr::new(ip.parse().unwrap(), 40000);
            request.extensions_mut().insert(ConnectInfo(peer));
            let start = Instant::now();
            let response = self.router.clone().oneshot(request).await.unwrap();
            let elapsed = start.elapsed();
            let status = response.status();
            let headers = response.headers().clone();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec();
            TestResponse {
                status,
                headers,
                body,
                elapsed,
            }
        }

        async fn get(&self, target: &str) -> TestResponse {
            self.request("203.0.113.7", Method::GET, target).await
        }

        fn position(&self) -> u64 {
            self.shared.state.lock().unwrap().queue().position()
        }

        fn client(&self, ip: &str) -> Client {
            self.shared.state.lock().unwrap().clients()[&ip.parse().unwrap()].clone()
        }

        fn set_blocked(&self, ip: &str, blocked: bool) {
            self.shared
                .state
                .lock()
                .unwrap()
                .set_blocked(ip.parse().unwrap(), blocked);
        }

        fn set_paused(&self, paused: bool) {
            self.shared.state.lock().unwrap().set_paused(paused);
        }
    }

    fn assert_served(response: &TestResponse, body: &str) {
        assert_eq!(response.status, StatusCode::OK);
        assert_eq!(
            response
                .headers
                .get_all(CONTENT_TYPE)
                .iter()
                .collect::<Vec<_>>(),
            ["text/plain"]
        );
        assert_eq!(String::from_utf8_lossy(&response.body), body);
        let delay = if body.is_empty() {
            DELAY
        } else {
            Duration::ZERO
        };
        assert_eq!(response.elapsed, delay);
    }

    fn assert_forbidden(response: &TestResponse) {
        assert_eq!(response.status, StatusCode::FORBIDDEN);
        assert_eq!(response.body, b"");
        assert_eq!(response.elapsed, DELAY);
    }

    fn assert_bad_max(response: &TestResponse, reason: &str) {
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        assert_eq!(response.headers[CONTENT_TYPE], "text/plain");
        assert_eq!(
            String::from_utf8_lossy(&response.body),
            format!("{reason}\n")
        );
        assert_eq!(response.elapsed, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn lines_are_handed_out_in_batches() {
        let server = server("a\nb\nc\nd\n", "/", 10000, false);
        assert_served(&server.get("/").await, "a\n");
        assert_served(&server.get("/?max=2").await, "b\nc\n");
        assert_served(&server.get("/?other=x&max=5").await, "d\n");
        assert_served(&server.get("/?max=5").await, "");
        assert_eq!(server.position(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn with_cycling_the_request_after_the_end_wraps() {
        let server = server("a\nb\n", "/", 10000, true);
        assert_served(&server.get("/?max=2").await, "a\nb\n");
        assert_served(&server.get("/?max=2").await, "");
        assert_served(&server.get("/?max=2").await, "a\nb\n");
        assert_eq!(server.shared.state.lock().unwrap().wraps(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn wrong_path() {
        let server = server("a\nb\n", "/secret", 10000, false);
        for target in [
            "/",
            "/secret/",
            "/SECRET",
            "/%73ecret",
            "/favicon.ico",
            "/?max=2",
        ] {
            assert_forbidden(&server.get(target).await);
            assert_forbidden(&server.request("203.0.113.7", Method::POST, target).await);
        }
        assert_eq!(server.position(), 0);
        assert_served(&server.get("/secret").await, "a\n");
        assert_served(&server.get("/secret?max=2").await, "b\n");
    }

    #[tokio::test(start_paused = true)]
    async fn methods_other_than_get_are_not_allowed() {
        let server = server("a\nb\n", "/", 10000, false);
        for method in [
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ] {
            let response = server.request("203.0.113.7", method, "/?max=2").await;
            assert_eq!(response.status, StatusCode::METHOD_NOT_ALLOWED);
            assert_eq!(response.headers[ALLOW], "GET");
            assert_eq!(response.body, b"");
            assert_eq!(response.elapsed, Duration::ZERO);
        }
        assert_eq!(server.position(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_max() {
        let server = server("a\nb\n", "/", 10000, false);
        for query in [
            "max=0",
            "max=+5",
            "max=%2B5",
            "max=-1",
            "max=1.5",
            "max=2.0",
            "max=1e3",
            "max=0x10",
            "max=abc",
            "max=",
            "max",
            "max=%20",
            "max=18446744073709551616",
        ] {
            assert_bad_max(&server.get(&format!("/?{query}")).await, INVALID_MAX);
        }
        assert_bad_max(&server.get("/?max=1&max=1").await, REPEATED_MAX);
        assert_bad_max(&server.get("/?max=1&m%61x=2").await, REPEATED_MAX);
        assert_eq!(server.position(), 0);

        assert_served(&server.get("/?max=18446744073709551615").await, "a\nb\n");
    }

    #[tokio::test(start_paused = true)]
    async fn max_is_lowered_to_max_batch() {
        let server = server("a\nb\nc\nd\n", "/", 3, false);
        assert_served(&server.get("/?max=18446744073709551615").await, "a\nb\nc\n");
        assert_served(&server.get("/?max=007").await, "d\n");
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_request_does_not_delay_others() {
        let server = Arc::new(server("a\n", "/", 10000, false));
        let waiting = server.get("/wrong");
        let served = server.get("/");
        let (waiting, served) = tokio::join!(waiting, served);
        assert_forbidden(&waiting);
        assert_served(&served, "a\n");
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_requests_get_every_line_exactly_once() {
        let lines: Vec<String> = (0..1000).map(|i| format!("line {i}\n")).collect();
        let server = Arc::new(server(&lines.concat(), "/", 10000, false));
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..200 {
            let server = server.clone();
            tasks.spawn(async move { server.get(&format!("/?max={}", i % 13 + 1)).await.body });
        }
        let mut handed_out: Vec<String> = Vec::new();
        while let Some(body) = tasks.join_next().await {
            let body = String::from_utf8(body.unwrap()).unwrap();
            handed_out.extend(body.split_inclusive('\n').map(str::to_owned));
        }
        handed_out.sort();
        let mut expected = lines.clone();
        expected.sort();
        assert_eq!(handed_out, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_server_does_not_respond() {
        let server = server("a\n", "/", 10000, false);
        server.shared.state.lock().unwrap().stop();
        let response = tokio::time::timeout(Duration::from_secs(60), server.get("/")).await;
        assert!(response.is_err());
        assert_eq!(server.position(), 0);
    }

    const OTHER_IP: &str = "198.51.100.9";

    #[tokio::test(start_paused = true)]
    async fn a_blocked_client_gets_403_while_others_are_served() {
        let server = server("a\nb\nc\n", "/", 10000, false);
        assert_served(&server.request(OTHER_IP, Method::GET, "/").await, "a\n");
        server.set_blocked(OTHER_IP, true);
        for (method, target) in [
            (Method::GET, "/"),
            (Method::HEAD, "/"),
            (Method::GET, "/?max=0"),
        ] {
            assert_forbidden(&server.request(OTHER_IP, method, target).await);
        }
        assert_served(&server.get("/").await, "b\n");
        server.set_blocked(OTHER_IP, false);
        assert_served(&server.request(OTHER_IP, Method::GET, "/").await, "c\n");
    }

    #[tokio::test(start_paused = true)]
    async fn a_paused_server_hands_out_no_lines() {
        let server = server("a\nb\n", "/secret", 10000, false);
        assert_served(&server.get("/secret").await, "a\n");
        server.request(OTHER_IP, Method::GET, "/secret?max=0").await;
        server.set_blocked(OTHER_IP, true);
        server.set_paused(true);

        assert_served(&server.get("/secret?max=2").await, "");
        assert_eq!(server.position(), 1);
        // The rows above "paused" in the table still apply
        assert_forbidden(&server.request(OTHER_IP, Method::GET, "/secret").await);
        assert_forbidden(&server.get("/").await);
        let head = server.request("203.0.113.7", Method::HEAD, "/secret").await;
        assert_eq!(head.status, StatusCode::METHOD_NOT_ALLOWED);
        assert_bad_max(&server.get("/secret?max=0").await, INVALID_MAX);
        assert_eq!(server.position(), 1);

        server.set_paused(false);
        assert_served(&server.get("/secret?max=2").await, "b\n");
    }

    #[tokio::test]
    async fn blocked_comes_before_wrong_path() {
        let server = server("a\n", "/", 10000, false);
        let ip = OTHER_IP.parse().unwrap();
        let config = Config {
            path: "/".to_owned(),
            max_batch: 10000,
        };
        let (request, ()) = Request::builder()
            .method(Method::POST)
            .uri("/wrong?max=0")
            .body(())
            .unwrap()
            .into_parts();
        let mut state = server.shared.state.lock().unwrap();
        let decided = decide(&mut state, &config, ip, state::now(), &request).unwrap();
        assert_eq!(decided, Some(Reply::WrongPath));
        state.set_blocked(ip, true);
        let decided = decide(&mut state, &config, ip, state::now(), &request).unwrap();
        assert_eq!(decided, Some(Reply::Blocked));
    }

    #[tokio::test(start_paused = true)]
    async fn clients_are_counted() {
        let server = server("a\nb\nc\n", "/", 10000, false);
        let ip = "203.0.113.7";
        let before = state::now();
        assert_served(
            &server.get_with_user_agents(ip, "/?max=2", &[b"one"]).await,
            "a\nb\n",
        );
        assert_forbidden(&server.get("/wrong").await);
        server.request(ip, Method::POST, "/").await;
        server.get("/?max=0").await;
        assert_served(
            &server.get_with_user_agents(ip, "/?max=5", &[b"two"]).await,
            "c\n",
        );
        assert_served(&server.get("/").await, "");
        let after = state::now();

        let client = server.client(ip);
        assert_eq!((client.items, client.requests), (3, 6));
        assert_eq!(client.user_agent.as_deref(), Some("two"));
        assert!(!client.blocked);
        assert!(before <= client.first_seen, "{before} {client:?}");
        assert!(client.first_seen <= client.last_seen, "{client:?}");
        assert!(client.last_seen <= after, "{after} {client:?}");

        // An IPv4-mapped IPv6 address is the same client
        server
            .request("::ffff:203.0.113.7", Method::GET, "/wrong")
            .await;
        assert_eq!(server.client(ip).requests, 7);
        assert_eq!(server.shared.state.lock().unwrap().clients().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn each_request_is_an_event() {
        let server = server("a\nb\nc\n", "/", 10000, false);
        server.get("/?max=2").await;
        server.request(OTHER_IP, Method::GET, "/wrong").await;
        server.request(OTHER_IP, Method::HEAD, "/").await;
        server.get("/?max=x").await;
        server.set_paused(true);
        server.get("/").await;
        server.set_paused(false);
        server.set_blocked(OTHER_IP, true);
        server.request(OTHER_IP, Method::GET, "/").await;
        server.get("/?max=5").await;
        server.get("/").await;

        let state = server.shared.state.lock().unwrap();
        let events: Vec<(String, Outcome)> = state
            .events()
            .iter()
            .map(|event| (event.ip.to_string(), event.outcome))
            .collect();
        let event = |ip: &str, outcome| (ip.to_owned(), outcome);
        assert_eq!(
            events,
            [
                event("203.0.113.7", Outcome::Lines(2)),
                event(OTHER_IP, Outcome::WrongPath),
                event(OTHER_IP, Outcome::Rejected(405)),
                event("203.0.113.7", Outcome::Rejected(400)),
                event("203.0.113.7", Outcome::Paused),
                event(OTHER_IP, Outcome::Blocked),
                event("203.0.113.7", Outcome::Lines(1)),
                event("203.0.113.7", Outcome::Lines(0)),
            ]
        );
        assert_eq!(
            state.last_minute(Instant::now()),
            state::Counts {
                requests: 8,
                lines: 3
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn user_agents_are_stored_without_control_characters() {
        let server = server("a\n", "/", 10000, false);
        let user_agents: [&[u8]; 2] = [b"a\tb\xc2\x9bc\xffd", b"second"];
        server
            .get_with_user_agents(OTHER_IP, "/", &user_agents)
            .await;
        assert_eq!(
            server.client(OTHER_IP).user_agent.as_deref(),
            Some("a?b?c\u{fffd}d")
        );
    }
}
