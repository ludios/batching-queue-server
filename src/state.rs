//! The state shared by the parts of the server, and saving it to and loading it from the state
//! file.

use crate::queue::{Batch, Queue};
use anyhow::{Context, Result, bail};
use jiff::Timestamp;
use serde::de::{Error as _, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{Instant, MissedTickBehavior};

/// How often the state file is saved while something in it has changed.
pub const SAVE_INTERVAL: Duration = Duration::from_secs(30);

/// The only state file version that this server reads and writes.
const VERSION: u64 = 1;

/// The most `char`s of a `User-Agent` value that are kept.
const MAX_USER_AGENT_CHARS: usize = 200;

/// The most events that are kept.
const MAX_EVENTS: usize = 1000;

/// The number of seconds over which requests and lines handed out are counted.
const RATE_SECONDS: usize = 60;

/// Returns the current time, in whole seconds, as the state file holds it.
pub fn now() -> Timestamp {
    Timestamp::from_second(Timestamp::now().as_second())
        .expect("a whole second of a valid time is valid")
}

/// Makes text from the network or from a file safe to draw on a terminal.
///
/// `bytes` are decoded as UTF-8, with each invalid sequence replaced by U+FFFD; each control
/// character (Unicode category Cc, which includes ESC and U+0080 to U+009F) is replaced by `?`;
/// and only the first `max_chars` `char`s are kept.
pub fn clean_for_terminal(bytes: &[u8], max_chars: usize) -> String {
    String::from_utf8_lossy(bytes)
        .chars()
        .take(max_chars)
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

/// What is known about one client IP address.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Client {
    /// Whether the operator has blocked this IP address.
    pub blocked: bool,
    /// The number of lines handed to this IP address.
    pub items: u64,
    /// The number of requests from this IP address, whatever their outcome.
    pub requests: u64,
    pub first_seen: Timestamp,
    pub last_seen: Timestamp,
    /// The `User-Agent` of the most recent request that had one, cleaned by
    /// `clean_for_terminal`. (With `deserialize_with`, the field must be present in the state
    /// file, even when it is null.)
    #[serde(deserialize_with = "deserialize_user_agent")]
    pub user_agent: Option<String>,
}

/// Deserializes a `user_agent` of a state file, cleaned as a live one is.
fn deserialize_user_agent<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let user_agent = Option::<String>::deserialize(deserializer)?;
    Ok(
        user_agent
            .map(|user_agent| clean_for_terminal(user_agent.as_bytes(), MAX_USER_AGENT_CHARS)),
    )
}

/// Deserializes the `clients` of a state file, with each IP address in its canonical form (see
/// `IpAddr::to_canonical`). Two keys that are the same address are an error, so that no client
/// record is silently dropped.
fn deserialize_clients<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<IpAddr, Client>, D::Error> {
    struct ClientsVisitor;

    impl<'de> Visitor<'de> for ClientsVisitor {
        type Value = BTreeMap<IpAddr, Client>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an object whose keys are IP addresses")
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut clients = BTreeMap::new();
            while let Some(key) = map.next_key::<String>()? {
                let Ok(ip) = key.parse::<IpAddr>() else {
                    return Err(M::Error::custom(format!("{key:?} is not an IP address")));
                };
                let ip = ip.to_canonical();
                if clients.insert(ip, map.next_value()?).is_some() {
                    return Err(M::Error::custom(format!(
                        "more than one client has the IP address {ip}"
                    )));
                }
            }
            Ok(clients)
        }
    }

    deserializer.deserialize_map(ClientsVisitor)
}

/// A column of the dashboard's clients table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Column {
    /// The column that says whether the client is blocked.
    Blocked,
    Ip,
    Items,
    #[serde(rename = "first_seen")]
    First,
    #[serde(rename = "last_seen")]
    Last,
    #[serde(rename = "requests")]
    Reqs,
    Host,
    /// The `User-Agent` column, which cannot be the sort column, and so is never in a state file.
    #[serde(skip)]
    Ua,
}

/// The view settings of the dashboard, as the operator has chosen them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct View {
    /// Never `Column::Ua`.
    pub sort_column: Column,
    /// Whether the clients table is sorted in the direction opposite to the one that the sort
    /// column gets when it is first chosen.
    pub sort_inverted: bool,
    pub show_host: bool,
    pub show_ua: bool,
}

impl Default for View {
    fn default() -> View {
        View {
            sort_column: Column::Items,
            sort_inverted: false,
            show_host: true,
            show_ua: false,
        }
    }
}

/// What came of one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// This many lines were handed out (possibly 0).
    Lines(u64),
    Blocked,
    WrongPath,
    Paused,
    /// Rejected with this HTTP status code.
    Rejected(u16),
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Lines(lines) => write!(f, "{lines}"),
            Outcome::Blocked => f.write_str("blocked"),
            Outcome::WrongPath => f.write_str("wrong path"),
            Outcome::Paused => f.write_str("paused"),
            Outcome::Rejected(status) => write!(f, "rejected {status}"),
        }
    }
}

/// One request, as the dashboard lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub time: Timestamp,
    /// The IP address of the client, in its canonical form.
    pub ip: IpAddr,
    pub outcome: Outcome,
}

/// Numbers of requests and of lines handed out.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub requests: u64,
    pub lines: u64,
}

/// The counts of the last `RATE_SECONDS` seconds, one per second.
struct Rates {
    /// The counts of each second, at the index of that second modulo `RATE_SECONDS`, with the
    /// second (since the Unix epoch) they belong to.
    seconds: [(i64, Counts); RATE_SECONDS],
}

impl Rates {
    fn new() -> Rates {
        Rates {
            seconds: [(i64::MIN, Counts::default()); RATE_SECONDS],
        }
    }

    /// Counts a request at `now` that handed out `lines` lines.
    fn add(&mut self, now: Timestamp, lines: u64) {
        let second = now.as_second();
        let index = usize::try_from(second.rem_euclid(RATE_SECONDS as i64))
            .expect("a remainder of a positive divisor is not negative");
        let (counted_second, counts) = &mut self.seconds[index];
        if *counted_second != second {
            *counted_second = second;
            *counts = Counts::default();
        }
        counts.requests = counts.requests.saturating_add(1);
        counts.lines = counts.lines.saturating_add(lines);
    }

    /// Returns the counts of the `RATE_SECONDS` seconds that end with the second of `now`.
    fn sum(&self, now: Timestamp) -> Counts {
        let last = now.as_second();
        let first = last - (RATE_SECONDS as i64 - 1);
        let mut sum = Counts::default();
        for (second, counts) in &self.seconds {
            if (first..=last).contains(second) {
                sum.requests = sum.requests.saturating_add(counts.requests);
                sum.lines = sum.lines.saturating_add(counts.lines);
            }
        }
        sum
    }
}

/// Returns `path` with `suffix` appended to its last component, so that `items.txt` with
/// `.bqs.json` becomes `items.txt.bqs.json`.
pub fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(suffix);
    PathBuf::from(path)
}

/// Returns the path of the lock file of the state file at `path`.
pub fn lock_path(path: &Path) -> PathBuf {
    with_suffix(path, ".lock")
}

/// Returns the path of the temporary file that a save of the state file at `path` writes.
pub fn temporary_path(path: &Path) -> PathBuf {
    with_suffix(path, ".tmp")
}

/// The state behind the mutex in `Shared`. Its fields change only through its methods, which
/// keep track of what needs saving.
pub struct State {
    queue: Queue,
    /// How many times the queue has started a new pass at the first line.
    wraps: u64,
    /// The client records, keyed by canonical IP address: those from the state file, and one
    /// for each IP address that has made a request since.
    clients: BTreeMap<IpAddr, Client>,
    view: View,
    /// The most recent requests, at most `MAX_EVENTS`, oldest first.
    events: VecDeque<Event>,
    rates: Rates,
    /// Whether the operator has paused handing out lines.
    paused: bool,
    /// Whether the server has stopped handing out lines because it is exiting.
    stopped: bool,
    /// Whether anything that the state file holds has changed since the last snapshot.
    dirty: bool,
    /// Notified when a change must be saved without waiting for the next periodic save.
    save_requested: Arc<Notify>,
}

impl State {
    /// `queue` is the queue, opened at the position to start from; `wraps` is the wrap counter
    /// to start from; `clients` are the client records, keyed by canonical IP address; and
    /// `view` is the view settings of the dashboard.
    pub fn new(queue: Queue, wraps: u64, clients: BTreeMap<IpAddr, Client>, view: View) -> State {
        assert!(
            clients.keys().all(|ip| *ip == ip.to_canonical()),
            "every IP address must be canonical"
        );
        assert_ne!(view.sort_column, Column::Ua, "UA cannot be the sort column");
        State {
            queue,
            wraps,
            clients,
            view,
            events: VecDeque::with_capacity(MAX_EVENTS),
            rates: Rates::new(),
            paused: false,
            stopped: false,
            dirty: false,
            save_requested: Arc::new(Notify::new()),
        }
    }

    /// Stops handing out lines for good.
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    pub fn is_paused(&self) -> bool {
        self.paused
    }

    /// Counts a request from `ip` at `now` in the client record of `ip`, which is created if
    /// `ip` has none.
    ///
    /// `ip` must be in its canonical form. `user_agent` is the value of the first `User-Agent`
    /// header of the request, if it has one.
    ///
    /// Returns the client record.
    pub fn record_request(
        &mut self,
        ip: IpAddr,
        now: Timestamp,
        user_agent: Option<&[u8]>,
    ) -> &Client {
        assert_eq!(ip, ip.to_canonical(), "the IP address must be canonical");
        self.dirty = true;
        let client = self.clients.entry(ip).or_insert_with(|| Client {
            blocked: false,
            items: 0,
            requests: 0,
            first_seen: now,
            last_seen: now,
            user_agent: None,
        });
        // Saturating, because a state file may hold any u64
        client.requests = client.requests.saturating_add(1);
        client.last_seen = now;
        if let Some(user_agent) = user_agent {
            client.user_agent = Some(clean_for_terminal(user_agent, MAX_USER_AGENT_CHARS));
        }
        client
    }

    /// Takes up to `n` lines from the queue (see `Queue::take`) for the client at `ip`, which
    /// must have a client record, and counts them in its `items`. A wrap is counted, and saved
    /// without waiting for the next periodic save.
    pub fn take(&mut self, ip: IpAddr, n: u64) -> Result<Batch> {
        assert!(!self.stopped, "a batch must not be taken after stopping");
        let client = self.clients.get_mut(&ip).expect("the client has a record");
        let batch = self.queue.take(n)?;
        if batch.lines > 0 {
            client.items = client.items.saturating_add(batch.lines);
            self.dirty = true;
        }
        if batch.wrapped {
            self.dirty = true;
            self.wraps = self.wraps.saturating_add(1);
            self.save_requested.notify_one();
            tracing::info!(
                "reached the end of the item file; starting a new pass (wrap {}, {} lines)",
                self.wraps,
                self.queue.total()
            );
        }
        Ok(batch)
    }

    /// Records what came of a request from `ip` (in its canonical form) at `now`, in the events
    /// and in the rates.
    pub fn record_outcome(&mut self, ip: IpAddr, now: Timestamp, outcome: Outcome) {
        if self.events.len() == MAX_EVENTS {
            self.events.pop_front();
        }
        self.events.push_back(Event {
            time: now,
            ip,
            outcome,
        });
        let lines = if let Outcome::Lines(lines) = outcome {
            lines
        } else {
            0
        };
        self.rates.add(now, lines);
    }

    /// Returns a copy of what the state file holds, and counts the state as saved.
    fn snapshot(&mut self) -> Saved {
        self.dirty = false;
        Saved {
            version: VERSION,
            position: self.queue.position(),
            wraps: self.wraps,
            view: self.view,
            clients: self.clients.clone(),
        }
    }

    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    pub fn wraps(&self) -> u64 {
        self.wraps
    }

    pub fn clients(&self) -> &BTreeMap<IpAddr, Client> {
        &self.clients
    }

    /// Returns the most recent requests, oldest first.
    pub fn events(&self) -> &VecDeque<Event> {
        &self.events
    }

    /// Returns the requests and the lines handed out in the last minute before `now`, give or
    /// take a second.
    pub fn last_minute(&self, now: Timestamp) -> Counts {
        self.rates.sum(now)
    }

    pub fn view(&self) -> View {
        self.view
    }

    /// Changes the view settings of the dashboard to `view`. A change is saved at the next
    /// periodic save.
    pub fn set_view(&mut self, view: View) {
        assert_ne!(view.sort_column, Column::Ua, "UA cannot be the sort column");
        if view != self.view {
            self.view = view;
            self.dirty = true;
        }
    }

    /// Pauses handing out lines, or resumes it. A pause is not saved.
    pub fn set_paused(&mut self, paused: bool) {
        self.paused = paused;
    }

    /// Blocks or unblocks the client at `ip` (in its canonical form), which must have a client
    /// record. A change is saved without waiting for the next periodic save.
    pub fn set_blocked(&mut self, ip: IpAddr, blocked: bool) {
        let client = self
            .clients
            .get_mut(&ip)
            .expect("only a known client is blocked or unblocked");
        if client.blocked == blocked {
            return;
        }
        client.blocked = blocked;
        self.dirty = true;
        self.save_requested.notify_one();
    }
}

/// The contents of a state file, as saved.
#[derive(Serialize)]
struct Saved {
    version: u64,
    position: u64,
    wraps: u64,
    view: View,
    clients: BTreeMap<IpAddr, Client>,
}

/// The `version` field of a state file, read before the rest so that a file of another version
/// is reported as such.
#[derive(Deserialize)]
struct Version {
    version: u64,
}

/// The contents of a state file, as loaded.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Loaded {
    /// Checked by `load` through `Version`
    #[serde(rename = "version")]
    _version: u64,
    /// The number of lines already handed out in the current pass.
    pub position: u64,
    pub wraps: u64,
    /// The default view settings if the state file has none.
    #[serde(default)]
    pub view: View,
    #[serde(deserialize_with = "deserialize_clients")]
    pub clients: BTreeMap<IpAddr, Client>,
}

/// Takes the lock that belongs to the state file at `path`, so that no other server uses that
/// state file.
///
/// Returns the open lock file. It holds the lock until it is closed, at the latest when the
/// process ends.
pub fn lock(path: &Path) -> Result<File> {
    let lock_path = lock_path(path);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| format!("cannot open the lock file {}", lock_path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => bail!(
            "a running server is using the state file {}; its lock on {} cannot be left over \
             from a crash, because the system releases it when that server's process ends",
            path.display(),
            lock_path.display()
        ),
        Err(TryLockError::Error(error)) => {
            Err(error).with_context(|| format!("cannot lock the lock file {}", lock_path.display()))
        }
    }
}

/// Loads the state file at `path`.
///
/// Returns None if there is no file at `path`.
pub fn load(path: &Path) -> Result<Option<Loaded>> {
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("cannot read the state file {}", path.display()));
        }
    };
    let invalid = || format!("the state file {} is not valid", path.display());
    let version: Version = serde_json::from_slice(&contents).with_context(invalid)?;
    if version.version != VERSION {
        bail!(
            "the state file {} has version {}, but this server reads only version {VERSION}",
            path.display(),
            version.version
        );
    }
    let loaded: Loaded = serde_json::from_slice(&contents).with_context(invalid)?;
    Ok(Some(loaded))
}

/// Replaces the file at `path` with one that holds `contents`, so that after a crash or a power
/// loss the file holds either its old contents or `contents`. The new file gets the permissions
/// of the old one. On failure, no temporary file is left behind.
fn write_atomically(path: &Path, contents: &[u8]) -> Result<()> {
    let temporary = temporary_path(path);
    let directory = match path.parent() {
        Some(parent) if parent != OsStr::new("") => parent,
        _ => Path::new("."),
    };
    let write = || -> std::io::Result<()> {
        // Opened before the rename, so that a directory that cannot be opened fails the save
        // before it changes anything
        let directory = File::open(directory)?;
        let mut file = File::create(&temporary)?;
        // Set before any contents are written, so that a state file that its operator has made
        // private (it holds client IP addresses) stays private
        match fs::metadata(path) {
            Ok(metadata) => file.set_permissions(metadata.permissions())?,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        directory.sync_all()
    };
    let written = write();
    if written.is_err() {
        // It may not exist, or may already have been renamed
        let _ = fs::remove_file(&temporary);
    }
    written.with_context(|| format!("cannot save the state file {}", path.display()))
}

/// The state, behind its lock, and the state file that it is saved to.
pub struct Shared {
    /// Never hold this lock across an `.await`.
    pub state: Mutex<State>,
    /// The path of the state file.
    path: PathBuf,
    /// Held during each save, so that saves never interleave and each snapshot is written
    /// before the next is taken.
    save_lock: Mutex<()>,
}

impl Shared {
    /// `path` is the path of the state file.
    pub fn new(state: State, path: PathBuf) -> Shared {
        Shared {
            state: Mutex::new(state),
            path,
            save_lock: Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Saves a snapshot of the state to the state file. Blocks until it is on disk, but holds
    /// the state mutex only while copying the state, which is much faster than serializing it.
    pub fn save(&self) -> Result<()> {
        let _saving = self.save_lock.lock().unwrap();
        let snapshot = self.state.lock().unwrap().snapshot();
        let mut contents = serde_json::to_vec_pretty(&snapshot).expect("the state serializes");
        contents.push(b'\n');
        write_atomically(&self.path, &contents)
    }
}

/// Does `Shared::save` on a thread where blocking is fine.
pub async fn save_async(shared: &Arc<Shared>) -> Result<()> {
    let shared = shared.clone();
    tokio::task::spawn_blocking(move || shared.save())
        .await
        .expect("saving does not panic")
}

/// Saves the state every `SAVE_INTERVAL` if it has changed, and whenever a change asks to be
/// saved at once.
///
/// Returns only when a save fails, with its error.
pub async fn save_periodically(shared: Arc<Shared>) -> anyhow::Error {
    let save_requested = shared.state.lock().unwrap().save_requested.clone();
    let mut interval = tokio::time::interval_at(Instant::now() + SAVE_INTERVAL, SAVE_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if !shared.state.lock().unwrap().dirty {
                    continue;
                }
            }
            () = save_requested.notified() => {}
        }
        if let Err(error) = save_async(&shared).await {
            return error;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;
    use tempfile::TempDir;

    /// Creates an item file with `lines` lines in a new temporary directory, and the shared
    /// state for it, with cycling on and its state file next to it.
    fn setup(lines: usize) -> (TempDir, Arc<Shared>) {
        let dir = tempfile::tempdir().unwrap();
        let item_file = dir.path().join("items.txt");
        fs::write(&item_file, "x\n".repeat(lines)).unwrap();
        let queue = Queue::open(&item_file, 0, true).unwrap();
        let state_file = dir.path().join("items.txt.bqs.json");
        let state = State::new(queue, 0, BTreeMap::new(), View::default());
        let shared = Arc::new(Shared::new(state, state_file));
        (dir, shared)
    }

    const IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, 7));

    fn time(text: &str) -> Timestamp {
        text.parse().unwrap()
    }

    /// Records a request from `IP` and takes up to `n` lines for it.
    fn take(state: &mut State, n: u64) -> Batch {
        state.record_request(IP, time("2026-11-24T12:00:00Z"), None);
        state.take(IP, n).unwrap()
    }

    #[test]
    fn clean_for_terminal_leaves_no_control_characters() {
        assert_eq!(clean_for_terminal(b"curl/8.9.1", 200), "curl/8.9.1");
        assert_eq!(
            clean_for_terminal(b"\x1b]0;title\x07\x1b[31mred\tx\r\n", 200),
            "?]0;title??[31mred?x??"
        );
        assert_eq!(clean_for_terminal("a\u{9b}31m".as_bytes(), 200), "a?31m");
        assert_eq!(
            clean_for_terminal(b"a\xff\xc3(b", 200),
            "a\u{fffd}\u{fffd}(b"
        );
        assert_eq!(clean_for_terminal("ééé".as_bytes(), 2), "éé");
    }

    #[test]
    fn requests_are_counted_per_client() {
        let (_dir, shared) = setup(3);
        let mut state = shared.state.lock().unwrap();
        state.record_request(IP, time("2026-11-24T12:00:00Z"), Some(b"first"));
        state.record_request(IP, time("2026-11-24T12:00:05Z"), None);
        let client = state.record_request(IP, time("2026-11-24T11:00:00Z"), Some(b"\x1bsecond"));
        assert_eq!(
            *client,
            Client {
                blocked: false,
                items: 0,
                requests: 3,
                first_seen: time("2026-11-24T12:00:00Z"),
                // The clock may jump back
                last_seen: time("2026-11-24T11:00:00Z"),
                user_agent: Some("?second".to_owned()),
            }
        );
        state.take(IP, 2).unwrap();
        assert_eq!(state.clients[&IP].items, 2);

        let long = "é".repeat(MAX_USER_AGENT_CHARS + 1);
        let client = state.record_request(IP, time("2026-11-24T12:00:00Z"), Some(long.as_bytes()));
        assert_eq!(
            client.user_agent.as_ref().unwrap().chars().count(),
            MAX_USER_AGENT_CHARS
        );
    }

    #[test]
    fn outcomes_as_shown() {
        let shown = [
            Outcome::Lines(100),
            Outcome::Blocked,
            Outcome::WrongPath,
            Outcome::Paused,
            Outcome::Rejected(405),
        ]
        .map(|outcome| outcome.to_string());
        assert_eq!(
            shown,
            ["100", "blocked", "wrong path", "paused", "rejected 405"]
        );
    }

    #[test]
    fn events_and_rates() {
        let (_dir, shared) = setup(3);
        let mut state = shared.state.lock().unwrap();
        let at = |second: i64| Timestamp::from_second(1_795_000_000 + second).unwrap();
        // 10 requests in each of 150 seconds, every other one with 2 lines
        for i in 0..1500 {
            let outcome = if i % 2 == 0 {
                Outcome::Lines(2)
            } else {
                Outcome::Blocked
            };
            state.record_outcome(IP, at(i / 10), outcome);
        }
        assert_eq!(state.events().len(), MAX_EVENTS);
        assert_eq!(state.events()[0].time, at(50));
        assert_eq!(state.events()[MAX_EVENTS - 1].time, at(149));

        let counts = |requests, lines| Counts { requests, lines };
        assert_eq!(state.last_minute(at(149)), counts(600, 600));
        assert_eq!(state.last_minute(at(200)), counts(90, 90));
        assert_eq!(state.last_minute(at(300)), counts(0, 0));
        // The clock jumped back: only seconds 90 to 100 are still counted
        assert_eq!(state.last_minute(at(100)), counts(110, 110));
        state.record_outcome(IP, at(-10), Outcome::Lines(5));
        assert_eq!(state.last_minute(at(-10)), counts(1, 5));
    }

    #[test]
    fn with_suffix_appends_to_the_whole_path() {
        assert_eq!(
            with_suffix(Path::new("/data/items.txt"), ".bqs.json"),
            Path::new("/data/items.txt.bqs.json")
        );
    }

    #[test]
    fn counters_stop_at_their_maximum() {
        let (_dir, shared) = setup(3);
        let mut state = shared.state.lock().unwrap();
        state.wraps = u64::MAX;
        state.record_request(IP, time("2026-11-24T12:00:00Z"), None);
        let client = state.clients.get_mut(&IP).unwrap();
        client.requests = u64::MAX;
        client.items = u64::MAX;
        take(&mut state, 3);
        take(&mut state, 3);
        let client = &state.clients[&IP];
        assert_eq!(
            (client.requests, client.items, state.wraps),
            (u64::MAX, u64::MAX, u64::MAX)
        );
    }

    /// View settings that differ from the default ones in every field.
    const VIEW: View = View {
        sort_column: Column::Host,
        sort_inverted: true,
        show_host: false,
        show_ua: true,
    };

    #[test]
    fn a_state_file_without_view_settings_gets_the_default_ones() {
        let (_dir, shared) = setup(3);
        let contents = r#"{"version": 1, "position": 0, "wraps": 0, "clients": {}}"#;
        fs::write(shared.path(), contents).unwrap();
        assert_eq!(load(shared.path()).unwrap().unwrap().view, View::default());
        assert_eq!(Loaded::default().view, View::default());
        assert_eq!(
            View::default(),
            View {
                sort_column: Column::Items,
                sort_inverted: false,
                show_host: true,
                show_ua: false
            }
        );
    }

    #[test]
    fn sort_columns_in_the_state_file() {
        let sortable = [
            Column::Blocked,
            Column::Ip,
            Column::Items,
            Column::First,
            Column::Last,
            Column::Reqs,
            Column::Host,
        ];
        let names = sortable.map(|column| serde_json::to_string(&column).unwrap());
        assert_eq!(
            names,
            [
                r#""blocked""#,
                r#""ip""#,
                r#""items""#,
                r#""first_seen""#,
                r#""last_seen""#,
                r#""requests""#,
                r#""host""#
            ]
        );
        for (column, name) in sortable.iter().zip(&names) {
            assert_eq!(serde_json::from_str::<Column>(name).unwrap(), *column);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_changed_view_alone_is_saved_at_the_next_periodic_save() {
        let (_dir, shared) = setup(3);
        shared.save().unwrap();
        let saved = inode(shared.path());
        tokio::spawn(save_periodically(shared.clone()));

        // Setting the same view is no change. (The extra second keeps the steps below off the
        // instants of the periodic saves.)
        shared.state.lock().unwrap().set_view(View::default());
        tokio::time::sleep(SAVE_INTERVAL + Duration::from_secs(1)).await;
        assert_eq!(inode(shared.path()), saved);

        shared.state.lock().unwrap().set_view(VIEW);
        tokio::time::sleep(SAVE_INTERVAL / 2).await;
        assert_eq!(inode(shared.path()), saved);
        tokio::time::sleep(SAVE_INTERVAL / 2).await;
        assert_eq!(load(shared.path()).unwrap().unwrap().view, VIEW);
    }

    #[test]
    fn save_and_load() {
        let (_dir, shared) = setup(3);
        assert!(load(shared.path()).unwrap().is_none());

        let ipv6: IpAddr = "2001:db8::17".parse().unwrap();
        {
            let mut state = shared.state.lock().unwrap();
            take(&mut state, 3);
            take(&mut state, 3);
            take(&mut state, 2);
            state.record_request(ipv6, time("2026-11-24T12:01:02Z"), Some(b"curl/8.9.1"));
            state.set_blocked(ipv6, true);
            state.set_view(VIEW);
        }
        shared.save().unwrap();
        let loaded = load(shared.path()).unwrap().unwrap();
        assert_eq!(loaded.position, 2);
        assert_eq!(loaded.wraps, 1);
        assert_eq!(loaded.view, VIEW);
        assert_eq!(loaded.clients, shared.state.lock().unwrap().clients);
        assert_eq!(
            fs::read_to_string(shared.path()).unwrap(),
            r#"{
  "version": 1,
  "position": 2,
  "wraps": 1,
  "view": {
    "sort_column": "host",
    "sort_inverted": true,
    "show_host": false,
    "show_ua": true
  },
  "clients": {
    "203.0.113.7": {
      "blocked": false,
      "items": 5,
      "requests": 3,
      "first_seen": "2026-11-24T12:00:00Z",
      "last_seen": "2026-11-24T12:00:00Z",
      "user_agent": null
    },
    "2001:db8::17": {
      "blocked": true,
      "items": 0,
      "requests": 1,
      "first_seen": "2026-11-24T12:01:02Z",
      "last_seen": "2026-11-24T12:01:02Z",
      "user_agent": "curl/8.9.1"
    }
  }
}
"#
        );
    }

    #[test]
    fn loaded_clients_are_treated_like_live_ones() {
        let (_dir, shared) = setup(3);
        let client = |user_agent: &str| {
            format!(
                r#"{{"blocked": true, "items": 1, "requests": 2, "first_seen": "2026-11-24T12:00:00Z",
                    "last_seen": "2026-11-24T12:00:00Z", "user_agent": {user_agent}}}"#
            )
        };
        let contents = format!(
            r#"{{"version": 1, "position": 0, "wraps": 0, "clients": {{"::ffff:203.0.113.7": {}, "::1": {}}}}}"#,
            client(r#""\u001b[2Jx""#),
            client("null"),
        );
        fs::write(shared.path(), contents).unwrap();
        let loaded = load(shared.path()).unwrap().unwrap();
        assert_eq!(
            loaded.clients.keys().collect::<Vec<_>>(),
            [&IP, &"::1".parse::<IpAddr>().unwrap()]
        );
        assert_eq!(loaded.clients[&IP].user_agent.as_deref(), Some("?[2Jx"));
        assert!(loaded.clients[&IP].blocked);
    }

    /// Writes `contents` to the state file and returns the message of the error from loading it.
    fn load_error(contents: &str) -> String {
        let (_dir, shared) = setup(3);
        fs::write(shared.path(), contents).unwrap();
        format!("{:#}", load(shared.path()).err().unwrap())
    }

    #[test]
    fn invalid_state_files() {
        let valid = r#"{"version": 1, "position": 0, "wraps": 0, "clients": {}}"#;
        let without_user_agent = r#"{"blocked": false, "items": 1, "requests": 1,
            "first_seen": "2026-11-24T12:00:00Z", "last_seen": "2026-11-24T12:00:00Z"}"#;
        let client = without_user_agent.replace('}', r#", "user_agent": null}"#);
        // A valid state file with view settings that end with `rest`
        let with_view = |sort_column: &str, rest: &str| {
            let view = format!(
                r#"{{"sort_column": "{sort_column}", "sort_inverted": false, "show_host": true{rest}}}"#
            );
            valid.replace("{}", &format!(r#"{{}}, "view": {view}"#))
        };
        for (contents, error) in [
            ("", "EOF while parsing"),
            (&valid[..20], "EOF while parsing"),
            ("1234\n", "invalid type: integer `1234`"),
            (&valid.replace("1,", "2,"), "has version 2"),
            (
                &valid.replace(r#""wraps": 0, "#, ""),
                "missing field `wraps`",
            ),
            (
                &valid.replace(r#""version": 1, "#, ""),
                "missing field `version`",
            ),
            (
                &valid.replace(r#", "clients": {}"#, ""),
                "missing field `clients`",
            ),
            (&valid.replace("{}", "[]"), "invalid type: sequence"),
            (&valid.replace("0,", "-1,"), "invalid value: integer `-1`"),
            (&valid.replace("0,", "\"0\","), "invalid type: string \"0\""),
            (
                &valid.replace("{}", r#"{"1.2.3.4": {}}"#),
                "missing field `blocked`",
            ),
            (
                &valid.replace("{}", &format!(r#"{{"1.2.3.4": {}}}"#, without_user_agent)),
                "missing field `user_agent`",
            ),
            (
                &valid.replace(
                    "{}",
                    &format!(
                        r#"{{"1.2.3.4": {}}}"#,
                        client.replace("null", r#"null, "x": 1"#)
                    ),
                ),
                "unknown field `x`",
            ),
            (
                &valid.replace(
                    "{}",
                    &format!(r#"{{"1.2.3.4": {}}}"#, client.replace("1,", "\"1\",")),
                ),
                "invalid type: string \"1\"",
            ),
            (
                &valid.replace(
                    "{}",
                    &format!(r#"{{"1.2.3.4": {}}}"#, client.replace("12:00:00Z", "noon")),
                ),
                "failed to parse two digit integer as hour",
            ),
            (
                &valid.replace("{}", &format!(r#"{{"host": {client}}}"#)),
                "\"host\" is not an IP address",
            ),
            (
                &valid.replace(
                    "{}",
                    &format!(r#"{{"1.2.3.4": {client}, "1.2.3.4": {client}}}"#),
                ),
                "more than one client has the IP address 1.2.3.4",
            ),
            (
                &valid.replace(
                    "{}",
                    &format!(r#"{{"1.2.3.4": {client}, "::ffff:1.2.3.4": {client}}}"#),
                ),
                "more than one client has the IP address 1.2.3.4",
            ),
            (
                &valid.replace("{}", r#"{}, "other": 1"#),
                "unknown field `other`",
            ),
            (
                &with_view("ua", ", \"show_ua\": true"),
                "unknown variant `ua`",
            ),
            (
                &with_view("IP", ", \"show_ua\": true"),
                "unknown variant `IP`",
            ),
            (
                &with_view("ip", ", \"show_ua\": 1"),
                "invalid type: integer `1`, expected a boolean",
            ),
            (&with_view("ip", ""), "missing field `show_ua`"),
            (
                &with_view("reqs", ", \"show_ua\": true"),
                "unknown variant `reqs`",
            ),
            (
                &valid.replace("{}", r#"{}, "view": null"#),
                "invalid type: null, expected struct View",
            ),
            (
                &with_view("ip", ", \"show_ua\": true, \"x\": 1"),
                "unknown field `x`",
            ),
        ] {
            let message = load_error(contents);
            assert!(message.contains(error), "{contents:?} gave {message:?}");
        }
    }

    #[test]
    fn a_directory_at_the_state_file_path_is_an_error() {
        let (_dir, shared) = setup(3);
        fs::create_dir(shared.path()).unwrap();
        let error = format!("{:#}", load(shared.path()).err().unwrap());
        assert!(error.contains("cannot read the state file"), "{error}");
    }

    #[test]
    fn a_state_file_in_a_missing_directory_cannot_be_saved() {
        let (dir, shared) = setup(3);
        let shared = Shared {
            path: dir.path().join("missing").join("state.json"),
            ..Arc::into_inner(shared).unwrap()
        };
        let error = format!("{:#}", shared.save().err().unwrap());
        assert!(error.contains("cannot save the state file"), "{error}");
    }

    #[test]
    fn a_save_keeps_the_permissions_of_the_state_file() {
        use std::os::unix::fs::PermissionsExt;
        let (_dir, shared) = setup(3);
        shared.save().unwrap();
        fs::set_permissions(shared.path(), fs::Permissions::from_mode(0o600)).unwrap();
        shared.save().unwrap();
        let mode = fs::metadata(shared.path()).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn only_one_holder_of_the_lock_at_a_time() {
        let (_dir, shared) = setup(3);
        let first = lock(shared.path()).unwrap();
        let error = format!("{:#}", lock(shared.path()).err().unwrap());
        assert!(
            error.contains("a running server is using the state file"),
            "{error}"
        );
        drop(first);
        lock(shared.path()).unwrap();
        assert!(with_suffix(shared.path(), ".lock").exists());
    }

    #[test]
    fn a_lock_file_with_content_is_not_truncated() {
        let (_dir, shared) = setup(3);
        let lock_path = with_suffix(shared.path(), ".lock");
        fs::write(&lock_path, "keep").unwrap();
        let _lock = lock(shared.path()).unwrap();
        assert_eq!(fs::read_to_string(&lock_path).unwrap(), "keep");
    }

    /// The inode number of the file at `path`, which changes with each save.
    fn inode(path: &Path) -> u64 {
        fs::metadata(path).unwrap().ino()
    }

    #[tokio::test(start_paused = true)]
    async fn a_change_is_saved_at_the_next_periodic_save() {
        let (_dir, shared) = setup(3);
        shared.save().unwrap();
        let saved = inode(shared.path());
        tokio::spawn(save_periodically(shared.clone()));

        // Nothing changed: no save. (The extra second keeps the steps below off the instants of
        // the periodic saves.)
        tokio::time::sleep(SAVE_INTERVAL * 3 + Duration::from_secs(1)).await;
        assert_eq!(inode(shared.path()), saved);

        take(&mut shared.state.lock().unwrap(), 1);
        tokio::time::sleep(SAVE_INTERVAL / 2).await;
        assert_eq!(inode(shared.path()), saved);
        take(&mut shared.state.lock().unwrap(), 1);
        tokio::time::sleep(SAVE_INTERVAL / 2).await;
        assert_eq!(load(shared.path()).unwrap().unwrap().position, 2);

        // A request that hands out no line changes the client record, which is saved too
        let now = time("2026-11-24T12:00:00Z");
        shared.state.lock().unwrap().record_request(IP, now, None);
        tokio::time::sleep(SAVE_INTERVAL).await;
        assert_eq!(
            load(shared.path()).unwrap().unwrap().clients[&IP].requests,
            3
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_wrap_is_saved_at_once() {
        let (_dir, shared) = setup(3);
        shared.save().unwrap();
        tokio::spawn(save_periodically(shared.clone()));

        take(&mut shared.state.lock().unwrap(), 3);
        take(&mut shared.state.lock().unwrap(), 3);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let loaded = load(shared.path()).unwrap().unwrap();
        assert_eq!((loaded.position, loaded.wraps), (0, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn a_block_and_an_unblock_are_saved_at_once() {
        let (_dir, shared) = setup(3);
        shared
            .state
            .lock()
            .unwrap()
            .record_request(IP, time("2026-11-24T12:00:00Z"), None);
        shared.save().unwrap();
        tokio::spawn(save_periodically(shared.clone()));

        for blocked in [true, false] {
            shared.state.lock().unwrap().set_blocked(IP, blocked);
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(
                load(shared.path()).unwrap().unwrap().clients[&IP].blocked,
                blocked
            );
        }
    }
}
