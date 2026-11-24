//! Reverse DNS lookups of client IP addresses, whose names the dashboard shows as a hint about
//! who each client is.

use crate::state::{self, Host, Shared};
use anyhow::{Result, anyhow};
use hickory_resolver::TokioResolver;
use hickory_resolver::proto::rr::{RData, Record};
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// The most lookups that run at the same time. Further ones wait their turn.
const MAX_RUNNING: usize = 8;

/// How long a lookup may run before it is given up. Its wait for a turn does not count.
const TIMEOUT: Duration = Duration::from_secs(5);

/// The longest that the system's resolver waits for the answer to one DNS query before it tries
/// again: short enough that a second try fits in `TIMEOUT`, so that one lost packet does not
/// cost a client its name for the whole run.
const TRY_TIMEOUT: Duration = Duration::from_secs(2);

/// The most `char`s of a name that are kept: its last ones, because the domain is at the end.
const MAX_NAME_CHARS: usize = 255;

/// Asks the DNS for the `PTR` records of an IP address.
///
/// Returns the records of the answer in their order, or the reason why there isn't an answer.
pub type LookUp =
    Arc<dyn Fn(IpAddr) -> Pin<Box<dyn Future<Output = Result<Vec<Record>>> + Send>> + Send + Sync>;

/// Returns the target name of the first `PTR` record in `answer` (skipping records of every
/// other type), in ASCII form, without its final `.`, cleaned by `state::clean_for_terminal`,
/// and cut to its last `MAX_NAME_CHARS` `char`s; or `Host::NoName` if `answer` doesn't have a
/// `PTR` record or its name is empty.
fn host_from_answer(answer: &[Record]) -> Host {
    let name = answer.iter().find_map(|record| match &record.data {
        RData::PTR(ptr) => Some(&ptr.0),
        _ => None,
    });
    let Some(name) = name else {
        return Host::NoName;
    };
    // The ASCII form keeps internationalized labels as `xn--…` and escapes unusual characters
    // in the style of a DNS zone file: as `\` and the character (`\[`), or as `\` and its byte
    // in three octal digits (`\033`). So each `char` takes one screen column.
    let ascii = name.to_ascii();
    let ascii = ascii.strip_suffix('.').unwrap_or(&ascii);
    // Doesn't change the ASCII form, but keeps the terminal safe whatever that form becomes
    let cleaned = state::clean_for_terminal(ascii.as_bytes(), usize::MAX);
    let length = cleaned.chars().count();
    let name: String = cleaned
        .chars()
        .skip(length.saturating_sub(MAX_NAME_CHARS))
        .collect();
    if name.is_empty() {
        Host::NoName
    } else {
        Host::Name(name)
    }
}

/// Looks up the reverse DNS names of client IP addresses in the background, at most
/// `MAX_RUNNING` at the same time.
#[derive(Clone)]
pub struct Resolver {
    look_up: LookUp,
    /// One permit for each lookup that may run.
    permits: Arc<Semaphore>,
}

impl Resolver {
    /// `look_up` does each lookup, once the lookup has its turn. An error from it gives the
    /// client `Host::NoName`.
    pub fn new(look_up: LookUp) -> Resolver {
        Resolver {
            look_up,
            permits: Arc::new(Semaphore::new(MAX_RUNNING)),
        }
    }

    /// Returns a resolver that uses the resolver configuration of the system, or the error that
    /// keeps it from being set up.
    pub fn system() -> Result<Resolver> {
        let mut builder = TokioResolver::builder_tokio()?;
        let options = builder.options_mut();
        options.timeout = options.timeout.min(TRY_TIMEOUT);
        let resolver = builder.build()?;
        Ok(Resolver::new(Arc::new(move |ip| {
            let resolver = resolver.clone();
            Box::pin(async move { Ok(resolver.reverse_lookup(ip).await?.answers().to_vec()) })
        })))
    }

    /// Returns a resolver that doesn't find a name for any client, for use when the system's
    /// resolver cannot be set up.
    pub fn unavailable() -> Resolver {
        Resolver::new(Arc::new(|_| {
            Box::pin(async { Err(anyhow!("the DNS resolver is not available")) })
        }))
    }

    /// Looks up the name of the client at `ip` in the background, then stores what was found in
    /// the state of `shared` (see `State::set_host`), in which `ip` must have been marked for a
    /// lookup (see `State::mark_for_lookup`).
    pub fn spawn(&self, shared: Arc<Shared>, ip: IpAddr) {
        let resolver = self.clone();
        tokio::spawn(async move {
            let permit = resolver.permits.acquire().await.expect("never closed");
            let host = match tokio::time::timeout(TIMEOUT, (resolver.look_up)(ip)).await {
                Ok(Ok(answer)) => host_from_answer(&answer),
                // Failed, or given up
                Ok(Err(_)) | Err(_) => Host::NoName,
            };
            drop(permit);
            shared.state.lock().unwrap().set_host(ip, host);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::Config;
    use crate::http::tests::{TestServer, server_with};
    use crate::state::Client;
    use axum::http::{Method, StatusCode};
    use hickory_resolver::proto::rr::rdata::{CNAME, PTR};
    use hickory_resolver::proto::rr::{Label, Name};
    use jiff::Timestamp;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Returns an answer record that holds `data`.
    fn record(data: RData) -> Record {
        Record::from_rdata(Name::root(), 300, data)
    }

    /// Returns a `PTR` record whose target is `name`.
    fn ptr(name: &str) -> Record {
        record(RData::PTR(PTR(Name::from_ascii(name).unwrap())))
    }

    /// Returns a resolver that answers the lookup of each IP address with what `answer` returns
    /// for it.
    fn resolver<F, A>(answer: F) -> Resolver
    where
        F: Fn(IpAddr) -> A + Send + Sync + 'static,
        A: Future<Output = Result<Vec<Record>>> + Send + 'static,
    {
        Resolver::new(Arc::new(move |ip| Box::pin(answer(ip))))
    }

    /// Returns the IP address of test client `n`.
    fn ip(n: u8) -> IpAddr {
        IpAddr::from([203, 0, 113, n])
    }

    /// Returns the name of test client `n`, as its `PTR` record holds it.
    fn name(n: u8) -> String {
        format!("client-{n}.example.com.")
    }

    /// Creates a server of 100 lines that looks up host names with `resolver` and starts with the
    /// client records `clients`.
    fn server(resolver: Resolver, clients: BTreeMap<IpAddr, Client>) -> TestServer {
        let contents: String = (0..100).map(|i| format!("{i}\n")).collect();
        let config = Config {
            path: "/".to_owned(),
            max_batch: 10,
        };
        server_with(&contents, false, config, clients, Some(resolver))
    }

    impl TestServer {
        /// Sends a GET from `ip`, which may be written in any form that `IpAddr` parses, and
        /// checks that it gets lines at once.
        async fn get_from(&self, ip: impl ToString) {
            let response = self.request(&ip.to_string(), Method::GET, "/").await;
            assert_eq!(response.status, StatusCode::OK);
            assert!(!response.body.is_empty());
            assert_eq!(response.elapsed, Duration::ZERO);
        }

        /// Returns the host of the client at `ip`, which must have a client record.
        fn host(&self, ip: IpAddr) -> Host {
            self.shared.state.lock().unwrap().clients()[&ip]
                .host
                .clone()
        }
    }

    async fn sleep(seconds: f64) {
        tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn each_client_is_looked_up_once_at_its_first_request_in_this_run() {
        let looked_up = Arc::new(Mutex::new(Vec::new()));
        let resolver = resolver({
            let looked_up = looked_up.clone();
            move |ip| {
                looked_up.lock().unwrap().push(ip);
                async { Ok(vec![ptr("client.example.com.")]) }
            }
        });
        let time = Timestamp::from_second(1_800_000_000).unwrap();
        let loaded = Client {
            blocked: false,
            items: 5,
            requests: 1,
            first_seen: time,
            last_seen: time,
            user_agent: None,
            host: Host::NotLookedUp,
        };
        let server = server(resolver, BTreeMap::from([(ip(2), loaded)]));

        sleep(10.0).await;
        assert!(looked_up.lock().unwrap().is_empty());
        for _ in 0..5 {
            server.get_from(ip(1)).await;
        }
        sleep(1.0).await;
        assert_eq!(*looked_up.lock().unwrap(), [ip(1)]);
        assert_eq!(
            server.host(ip(1)),
            Host::Name("client.example.com".to_owned())
        );
        assert_eq!(server.host(ip(2)), Host::NotLookedUp);
        // After the lookup has finished, and through an IPv4-mapped IPv6 address
        server.get_from(ip(1)).await;
        server.get_from("::ffff:203.0.113.1").await;
        sleep(1.0).await;
        assert_eq!(*looked_up.lock().unwrap(), [ip(1)]);

        server.get_from(ip(2)).await;
        server.get_from(ip(2)).await;
        sleep(1.0).await;
        assert_eq!(*looked_up.lock().unwrap(), [ip(1), ip(2)]);
        assert_eq!(
            server.host(ip(2)),
            Host::Name("client.example.com".to_owned())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_does_not_wait_for_its_lookup() {
        let looked_up = Arc::new(AtomicUsize::new(0));
        let resolver = resolver({
            let looked_up = looked_up.clone();
            move |_| {
                looked_up.fetch_add(1, Ordering::SeqCst);
                std::future::pending()
            }
        });
        let server = server(resolver, BTreeMap::new());
        server.get_from(ip(1)).await;
        sleep(1.0).await;
        assert_eq!(looked_up.load(Ordering::SeqCst), 1);
        assert_eq!(server.host(ip(1)), Host::NotLookedUp);
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_resolver_nothing_is_looked_up() {
        let contents = "a\nb\n";
        let config = Config {
            path: "/".to_owned(),
            max_batch: 10,
        };
        let server = server_with(contents, false, config, BTreeMap::new(), None);
        server.get_from(ip(1)).await;
        sleep(10.0).await;
        assert_eq!(server.host(ip(1)), Host::NotLookedUp);
    }

    #[tokio::test(start_paused = true)]
    async fn a_saved_name_is_shown_at_once_and_not_looked_up_again() {
        // Client 1 has a name, and client 2 doesn't
        let looked_up = Arc::new(Mutex::new(Vec::new()));
        let resolver = resolver({
            let looked_up = looked_up.clone();
            move |ip| {
                looked_up.lock().unwrap().push(ip);
                async move {
                    if ip == self::ip(1) {
                        Ok(vec![ptr(&name(1))])
                    } else {
                        Ok(vec![])
                    }
                }
            }
        });
        let first = server(resolver.clone(), BTreeMap::new());
        first.get_from(ip(1)).await;
        first.get_from(ip(2)).await;
        sleep(1.0).await;
        assert_eq!(first.host(ip(2)), Host::NoName);
        first.shared.save().unwrap();
        let saved = std::fs::read_to_string(first.shared.path()).unwrap();
        assert!(
            saved.contains(r#""host": "client-1.example.com""#),
            "{saved}"
        );
        assert!(saved.contains(r#""host": null"#), "{saved}");

        let loaded = state::load(first.shared.path()).unwrap().unwrap();
        let second = server(resolver, loaded.clients);
        let host_1 = Host::Name("client-1.example.com".to_owned());
        assert_eq!(second.host(ip(1)), host_1);
        assert_eq!(second.host(ip(2)), Host::NotLookedUp);
        second.get_from(ip(1)).await;
        second.get_from(ip(2)).await;
        sleep(1.0).await;
        assert_eq!(*looked_up.lock().unwrap(), [ip(1), ip(2), ip(2)]);
        assert_eq!(second.host(ip(1)), host_1);
        assert_eq!(second.host(ip(2)), Host::NoName);
    }

    #[tokio::test(start_paused = true)]
    async fn at_most_8_lookups_run_at_the_same_time() {
        let running = Arc::new(AtomicUsize::new(0));
        let most_running = Arc::new(AtomicUsize::new(0));
        let resolver = resolver({
            let running = running.clone();
            let most_running = most_running.clone();
            move |ip| {
                let running = running.clone();
                let most_running = most_running.clone();
                async move {
                    let now_running = running.fetch_add(1, Ordering::SeqCst) + 1;
                    most_running.fetch_max(now_running, Ordering::SeqCst);
                    sleep(1.0).await;
                    running.fetch_sub(1, Ordering::SeqCst);
                    let IpAddr::V4(ip) = ip else { unreachable!() };
                    Ok(vec![ptr(&name(ip.octets()[3]))])
                }
            }
        });
        let server = server(resolver, BTreeMap::new());
        for n in 1..=20 {
            server.get_from(ip(n)).await;
        }
        sleep(10.0).await;
        assert_eq!(most_running.load(Ordering::SeqCst), 8);
        for n in 1..=20 {
            let expected = name(n).trim_end_matches('.').to_owned();
            assert_eq!(server.host(ip(n)), Host::Name(expected));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_lookup_is_given_up_5_seconds_after_it_started_running() {
        // Clients 1 to 16 never get an answer, so client 17 waits 10 seconds for its turn, and then
        // gets its answer after 4 more seconds
        let server = server(
            resolver(|ip| async move {
                if ip != self::ip(17) {
                    std::future::pending::<()>().await;
                }
                sleep(4.0).await;
                Ok(vec![ptr(&name(17))])
            }),
            BTreeMap::new(),
        );
        for n in 1..=17 {
            server.get_from(ip(n)).await;
        }
        let hosts = || (1..=17).map(|n| server.host(ip(n))).collect::<Vec<_>>();
        let expected = |no_names: usize, name: Option<&str>| {
            let mut hosts = vec![Host::NoName; no_names];
            hosts.resize(16, Host::NotLookedUp);
            hosts.push(name.map_or(Host::NotLookedUp, |name| Host::Name(name.to_owned())));
            hosts
        };

        sleep(4.9).await;
        assert_eq!(hosts(), expected(0, None));
        sleep(0.2).await;
        assert_eq!(hosts(), expected(8, None));
        sleep(5.0).await;
        assert_eq!(hosts(), expected(16, None));
        sleep(3.8).await;
        assert_eq!(hosts(), expected(16, None));
        sleep(0.2).await;
        assert_eq!(hosts(), expected(16, Some("client-17.example.com")));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cname_record_before_the_ptr_record_is_skipped() {
        let server = server(
            resolver(|_| async {
                let alias = Name::from_ascii("1.0-25.113.0.203.in-addr.arpa.").unwrap();
                Ok(vec![record(RData::CNAME(CNAME(alias))), ptr(&name(1))])
            }),
            BTreeMap::new(),
        );
        server.get_from(ip(1)).await;
        sleep(1.0).await;
        assert_eq!(
            server.host(ip(1)),
            Host::Name("client-1.example.com".to_owned())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_unavailable_resolver_finds_no_name_and_the_server_keeps_serving() {
        let server = server(Resolver::unavailable(), BTreeMap::new());
        for n in 1..=3 {
            server.get_from(ip(n)).await;
            server.get_from(ip(n)).await;
        }
        sleep(1.0).await;
        for n in 1..=3 {
            assert_eq!(server.host(ip(n)), Host::NoName);
        }
    }

    #[test]
    fn the_name_is_the_target_of_the_first_ptr_record() {
        let cname = record(RData::CNAME(CNAME(Name::from_ascii("a.example.").unwrap())));
        let first = ptr("first.example.com.");
        let second = ptr("second.example.com.");
        assert_eq!(
            host_from_answer(&[cname.clone(), first, second]),
            Host::Name("first.example.com".to_owned())
        );
        assert_eq!(host_from_answer(&[]), Host::NoName);
        assert_eq!(host_from_answer(&[cname]), Host::NoName);
        let root = record(RData::PTR(PTR(Name::root())));
        assert_eq!(host_from_answer(&[root]), Host::NoName);
    }

    #[test]
    fn names_are_cleaned_and_cut_to_their_end() {
        let label = |bytes: &[u8]| Label::from_raw_bytes(bytes).unwrap();
        let ptr_of = |labels: Vec<Label>| {
            let name = Name::from_labels(labels).unwrap();
            assert!(name.is_fqdn());
            record(RData::PTR(PTR(name)))
        };

        // An escape sequence that would turn the text red
        let answer = ptr_of(vec![label(b"\x1b[31m"), label(b"example"), label(b"com")]);
        assert_eq!(
            host_from_answer(&[answer]),
            Host::Name(r"\033\[31m.example.com".to_owned())
        );

        // Each byte 0x9B becomes the 4 `char`s `\233`
        let mut labels = vec![label(&[0x9b; 63]); 3];
        labels.extend([label(b"example"), label(b"com")]);
        let Host::Name(cut) = host_from_answer(&[ptr_of(labels)]) else {
            panic!("a long name is a name");
        };
        assert_eq!(cut.chars().count(), 255);
        let whole = format!("{}.example.com", vec![r"\233".repeat(63); 3].join("."));
        assert!(whole.ends_with(&cut), "{cut}");
        assert!(!cut.chars().any(char::is_control), "{cut}");
    }
}
