//! The state shared by the parts of the server, and saving it to and loading it from the state
//! file.

use crate::queue::{Batch, Queue};
use anyhow::{Context, Result, bail};
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{Instant, MissedTickBehavior};

/// How often the state file is saved while something in it has changed.
pub const SAVE_INTERVAL: Duration = Duration::from_secs(30);

/// The only state file version that this server reads and writes.
const VERSION: u64 = 1;

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

/// The state behind the mutex in `Shared`.
pub struct State {
    pub queue: Queue,
    /// How many times the queue has started a new pass at the first line.
    pub wraps: u64,
    /// Whether the server has stopped handing out lines because it is exiting.
    stopped: bool,
    /// Whether anything that the state file holds has changed since the last snapshot.
    dirty: bool,
    /// Notified when a change must be saved without waiting for the next periodic save.
    save_requested: Arc<Notify>,
}

impl State {
    /// `wraps` is the wrap counter to start from.
    pub fn new(queue: Queue, wraps: u64) -> State {
        State {
            queue,
            wraps,
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

    /// Takes up to `n` lines from the queue (see `Queue::take`). A wrap is counted, and saved
    /// without waiting for the next periodic save.
    pub fn take(&mut self, n: u64) -> Result<Batch> {
        assert!(!self.stopped, "a batch must not be taken after stopping");
        let batch = self.queue.take(n)?;
        if batch.lines > 0 {
            self.dirty = true;
        }
        if batch.wrapped {
            self.dirty = true;
            self.wraps += 1;
            self.save_requested.notify_one();
            tracing::info!(
                "reached the end of the item file; starting again at its first line (wrap {}, {} lines)",
                self.wraps,
                self.queue.total()
            );
        }
        Ok(batch)
    }

    /// Returns the contents of a state file that holds the current state, and counts the state
    /// as saved.
    fn snapshot(&mut self) -> Vec<u8> {
        self.dirty = false;
        let saved = Saved {
            version: VERSION,
            position: self.queue.position(),
            wraps: self.wraps,
            clients: BTreeMap::new(),
        };
        let mut contents = serde_json::to_vec_pretty(&saved).expect("the state serializes");
        contents.push(b'\n');
        contents
    }
}

/// The contents of a state file, as saved.
#[derive(Serialize)]
struct Saved {
    version: u64,
    position: u64,
    wraps: u64,
    clients: BTreeMap<String, ()>,
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
    clients: BTreeMap<String, IgnoredAny>,
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
    if !loaded.clients.is_empty() {
        bail!(
            "the state file {} has clients, which this server cannot keep",
            path.display()
        );
    }
    Ok(Some(loaded))
}

/// Replaces the file at `path` with one that holds `contents`, so that after a crash or a power
/// loss the file holds either its old contents or `contents`. On failure, no temporary file is
/// left behind.
fn write_atomically(path: &Path, contents: &[u8]) -> Result<()> {
    let temporary = with_suffix(path, ".tmp");
    let directory = match path.parent() {
        Some(parent) if parent != OsStr::new("") => parent,
        _ => Path::new("."),
    };
    let write = || -> std::io::Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(directory)?.sync_all()
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
    /// the state lock only while taking the snapshot.
    pub fn save(&self) -> Result<()> {
        let _saving = self.save_lock.lock().unwrap();
        let snapshot = self.state.lock().unwrap().snapshot();
        write_atomically(&self.path, &snapshot)
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
        let shared = Arc::new(Shared::new(State::new(queue, 0), state_file));
        (dir, shared)
    }

    #[test]
    fn with_suffix_appends_to_the_whole_path() {
        assert_eq!(
            with_suffix(Path::new("/data/items.txt"), ".bqs.json"),
            Path::new("/data/items.txt.bqs.json")
        );
    }

    #[test]
    fn save_and_load() {
        let (_dir, shared) = setup(3);
        assert!(load(shared.path()).unwrap().is_none());

        {
            let mut state = shared.state.lock().unwrap();
            state.take(3).unwrap();
            state.take(3).unwrap();
            state.take(2).unwrap();
        }
        shared.save().unwrap();
        let loaded = load(shared.path()).unwrap().unwrap();
        assert_eq!(loaded.position, 2);
        assert_eq!(loaded.wraps, 1);
        assert_eq!(
            fs::read_to_string(shared.path()).unwrap(),
            "{\n  \"version\": 1,\n  \"position\": 2,\n  \"wraps\": 1,\n  \"clients\": {}\n}\n"
        );
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
                &valid.replace("{}", r#"{}, "other": 1"#),
                "unknown field `other`",
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

        shared.state.lock().unwrap().take(1).unwrap();
        tokio::time::sleep(SAVE_INTERVAL / 2).await;
        assert_eq!(inode(shared.path()), saved);
        shared.state.lock().unwrap().take(1).unwrap();
        tokio::time::sleep(SAVE_INTERVAL / 2).await;
        assert_eq!(load(shared.path()).unwrap().unwrap().position, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_wrap_is_saved_at_once() {
        let (_dir, shared) = setup(3);
        shared.save().unwrap();
        tokio::spawn(save_periodically(shared.clone()));

        shared.state.lock().unwrap().take(3).unwrap();
        shared.state.lock().unwrap().take(3).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let loaded = load(shared.path()).unwrap().unwrap();
        assert_eq!((loaded.position, loaded.wraps), (0, 1));
    }
}
