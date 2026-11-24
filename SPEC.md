# Spec: port batching-queue-server to Rust and add a terminal dashboard

## Goal

`batching-queue-server` hands out the lines of one text file to HTTP clients, in file order, a batch per request. Today it is 75 lines of Node ([index.js](index.js)) plus a vendored line reader ([readlines.js](readlines.js)). It knows nothing about who took what.

Replace it with a single Rust binary that:

1. Serves the same HTTP contract to existing clients (with the bug fixes listed below).
2. Tracks every client IP address: items taken, requests made, first seen, last seen, most recent User-Agent.
3. Lets the operator block and unblock an IP address.
4. Shows all of this in a compact terminal dashboard (TUI) drawn by the server process itself. The operator runs many instances, each in its own small terminal window, so every row of the screen matters.
5. Keeps the block list and the per-IP numbers across restarts.

The operator will read and maintain this code. Prefer the plainest design that meets the spec: few modules, few types, no cleverness. Read [index.js](index.js) first; it is short.

## Decisions already made by the operator (do not reopen)

- The client's address is the TCP peer address. Do not read `X-Forwarded-For` or any other header for it.
- A blocked IP receives `403` with an empty body, after a 1 second delay.
- No claim tracking, no "done" reports, no re-queueing of items. Handing out a line is the whole protocol.
- No rate limiting. No automatic blocking. No client nicknames. No block list shared between instances.
- The dashboard is a TUI inside the server process. No web UI.
- The User-Agent column is hidden by default and can be toggled.
- Sorting works like `htop`: click a column header, or use function keys.

## Command line

```
batching-queue-server ITEM_FILE STATE_FILE
```

| Environment variable | Default | Meaning |
|---|---|---|
| `PORT` | `31417` | TCP port to listen on. |
| `CYCLE` | `true` | `true` or `false`. Whether to start again at the first line after the last line was handed out. Any other value is a startup error. |
| `MAX_BATCH` | `10000` | Upper limit on the number of lines in one response. Must be at least 1. |

Listen on `[::]:PORT`, accepting IPv4 and IPv6, as the Node version does.

Existing launch scripts pass two positional arguments and the `PORT` and `CYCLE` variables. Keep them working unchanged: the second argument used to be the Node version's position file, and a file in that old format is accepted as a state file (see "State file"). Use `clap` (derive, with its `env` feature).

Every startup error (bad arguments, unreadable item file, invalid state file, port in use) prints one clear message to stderr and exits with status 1, before the terminal is switched to the dashboard.

## The item queue

### Item file

- A line is a sequence of bytes ended by `\n`. A last line without a final `\n` is also a line. A file that ends with `\n` has no extra empty line after it.
- Lines are bytes, not text: never validate or convert them. Empty lines are items too. A `\r` before `\n` is part of the line.
- At startup, scan the file once to count its lines and to find the byte offset of the next line to serve. Then keep the file open and read forward from that offset.
- Precondition: nobody modifies the item file while the server runs. If the number of lines read up to end of file differs from the startup count, log a warning and correct the total; do nothing more elaborate.

### Position

The position is the number of lines already handed out in the current pass over the file. This equals the 0-based index of the next line to serve.

The queue holds the position in memory. It is saved in the state file together with the client state (see "State file"); there is no separate position file. The queue itself does not read or write the state file: it is opened at a given position and reports its current position.

If the starting position is greater than the number of lines in the item file, exit with an error.

### Taking a batch

To take up to `n` lines:

1. Repeat up to `n` times: read the next line. If there is one, add it to the batch and increase the position by 1. If the end of the file was reached, stop.
2. If the end of the file was reached, `CYCLE` is `true`, and the position is greater than 0: set the position to 0, seek to the start of the file, and increase the wrap counter by 1. Do not continue reading in this request; the lines from the top are served to the next request.

So a request that reaches the end of the file returns the lines it collected before the end (possibly none). With `CYCLE=false`, the position stays at the end and every later batch is empty.

All batches are taken one at a time under one lock: two requests must never receive the same line within one pass, and lines are handed out in file order.

The position advances when the batch is read, not when the client has received it. A client that disconnects before reading its response loses those lines until the next pass. This is accepted.

## HTTP contract

For each request, the first matching row applies:

| Condition | Status | Headers | Body | Delay before responding | Lines consumed |
|---|---|---|---|---|---|
| The client IP is blocked | `403` | | empty | 1 second | none |
| The path is not exactly `/` | `404` | | empty | none | none |
| The method is not `GET` | `405` | `Allow: GET` | empty | none | none |
| The `max` query parameter is invalid | `400` | `Content-Type: text/plain` | one line saying what is wrong | none | none |
| The batch has at least one line | `200` | `Content-Type: text/plain` | each line followed by `\n` | none | the batch |
| The batch is empty | `200` | `Content-Type: text/plain` | empty | 1 second | none |

- `max` is optional; when absent it is 1. It is valid when it appears at most once and is a decimal integer of at least 1. A valid `max` greater than `MAX_BATCH` is lowered to `MAX_BATCH` (the client asked for "at most `max`" lines, so fewer is within the contract). Everything else is invalid: `0`, negative numbers, `1.5`, an empty value, non-digits, a repeated parameter. An integer too large for `u64` may also be treated as invalid.
- Other query parameters are ignored.
- `HEAD /` must get `405` and must not consume lines. Verified on docs.rs for axum 0.8.9: a route registered with `routing::get` is also called for `HEAD` requests. Register the handler so that this cannot happen, and test it.
- The `Content-Type` value is exactly `text/plain`, with no `charset`, as in the Node version.
- The 1 second delay slows down clients that poll an empty queue and clients that keep hammering while blocked. Waiting requests must not delay other requests.
- A block or unblock applies to requests that arrive after it. Requests already waiting out their delay finish as they started.

### Differences from the Node version (all intended)

These were observed by running the Node version:

- `GET /?max=1.5` and `GET /?max=-1` returned every remaining line of the file in one response. Now `400`.
- There was no upper limit on `max`. Now `MAX_BATCH`.
- A `GET` to any path, for example `/favicon.ico`, consumed a line. Now `404`.
- A request with any method other than `GET` never received a response. Now `405`.
- The position file was rewritten in place on every request, and an empty or damaged position file silently restarted the queue at line 0. Now the position is saved in the state file about once per second, and a damaged state file is a startup error.

For valid requests (`GET /` with `max` absent or an integer from 1 to `MAX_BATCH`), the response bodies must be byte-identical to the Node version's, and the position must equal the number in the Node version's position file.

## Client tracking

Every request, whatever its outcome, belongs to a client identified by its IP address. Convert IPv4-mapped IPv6 addresses (`::ffff:1.2.3.4`) to plain IPv4 (`IpAddr::to_canonical`), so that one client is one row.

Per client:

| Field | Meaning |
|---|---|
| `blocked` | Whether the operator has blocked this IP. |
| `items` | Total number of lines handed to this IP. |
| `requests` | Total number of requests from this IP, including blocked and rejected ones. |
| `first_seen` | Time of the first request from this IP. |
| `last_seen` | Time of the most recent request from this IP. |
| `user_agent` | The `User-Agent` header of the most recent request that had one; absent if none ever had one. |

A client record is created by the first request of any kind from that IP, so that scanners appear in the dashboard and can be blocked. Records are never removed.

The `User-Agent` value comes from the network and is drawn on the operator's terminal. Before storing it: decode it as UTF-8 with replacement of invalid sequences, replace every control character with `?`, and truncate it to 200 characters. No terminal escape sequence from a client may ever reach the terminal.

### Events

Keep the most recent 1000 events in memory. One event per request:

- time
- client IP
- outcome: the number of lines handed out (possibly 0), or `blocked`, or `rejected` together with the status code (`400`, `404`, `405`)

### Rates

Keep enough to show, for the last 60 seconds, the number of lines handed out and the number of requests. Sixty one-second buckets are enough.

### Time

Use wall-clock timestamps (`jiff::Timestamp`) for `first_seen`, `last_seen`, and event times. The clock may jump. An age that would be negative is shown as `0s`; nothing may panic or overflow because of a timestamp in the future.

Pass the current time into the functions that need it instead of reading the clock inside them, so that tests control time.

## State file

One file, at the path given as `STATE_FILE`, holds everything that survives a restart: the position, the wrap counter, and the clients.

```json
{
  "version": 1,
  "position": 1234567,
  "wraps": 3,
  "clients": {
    "203.0.113.7": {
      "blocked": false,
      "items": 12400,
      "requests": 124,
      "first_seen": "2026-11-24T12:01:02Z",
      "last_seen": "2026-11-24T18:40:11Z",
      "user_agent": "curl/8.9.1"
    }
  }
}
```

### Loading at startup

- The file does not exist: position 0, wrap counter 0, no clients.
- The file, after trimming ASCII whitespace, consists only of ASCII decimal digits: it is a position file written by the Node version. Use the number as the position, with wrap counter 0 and no clients. The first save replaces the file with the JSON format.
- The file is JSON with `"version": 1`: use it.
- Anything else (an empty file, damaged JSON, another `version`, a number too large for `u64`): exit with an error. Never silently restart at line 0 and never silently discard a block list.

### Saving

- Save by writing a temporary file in the same directory and renaming it over the state file. Do not call `fsync`.
- Save once per second if anything changed since the last save, immediately when a block or unblock happens, and at a clean exit.
- Every save is one consistent snapshot: the position, the wrap counter, and the counters are all from the same instant.
- Two saves must never interleave, and an older snapshot must never overwrite a newer one.
- Events and rates are not saved.

The state is deliberately not saved on every request, because each save rewrites every client record. The consequence: after a crash, a kill, or a power loss, the server restarts from a snapshot that is up to about 1 second old, and the lines handed out after that snapshot are handed out a second time. This is accepted. Lines are never skipped because of it.

## Failures while running

If writing the state file or reading the item file fails, the server must stop: restore the terminal, print the error to stderr, and exit with status 1. A server that hands out lines it cannot record is worse than a stopped server. The request that hit the failure gets no response.

A panic anywhere must end the whole process, never leave a half-working server. Set `panic = "abort"` in the Cargo profiles, and make sure the panic hook restores the terminal (including mouse capture) first.

## Dashboard

### When it runs

If both stdin and stdout are terminals, the server draws the dashboard. Otherwise it runs without one and writes one log line per event to stderr with `tracing`. In dashboard mode, nothing else may write to the terminal.

### Layout

From top to bottom. The numbers are examples.

```
items.txt :31417  1,234,567/9,876,543 12.5%  wrap 3  412 items/min  38 req/min  ETA 14h
           IP             ITEMS▼  FIRST  LAST    REQS
[block]    203.0.113.7    12,400     3d    2s     124
[unblock]  198.51.100.9      300     1h    0s   1,215
[block]    2001:db8::17        0     5m    5m       1
12:01:01 198.51.100.9  blocked
12:01:02 203.0.113.7   100
12:01:02 2001:db8::17  rejected 404
F2 UA  F6 Sort  F7 Invert  F9 Block  F10 Quit
```

1. **Status line** (1 row): item file name without its directory; port; position `/` total lines; percent with one decimal; wrap counter (omit when `CYCLE=false`); lines handed out in the last 60 seconds; requests in the last 60 seconds; estimated time until the end of the current pass, computed from the remaining lines and the 60 second rate, shown as `-` when that rate is 0.
2. **Clients table**: one header row, then one row per client. It takes the rows that the other parts leave free.
3. **Events pane**: the most recent events, newest at the bottom, as many as fit. Give it about one third of the rows below the status line, and at least 3 rows when the window has room.
4. **Key bar** (1 row).

No borders and no titles; they cost rows and columns. Numbers of items and requests use `,` as thousands separator. The dashboard must draw without panicking at every terminal size, down to 1×1; in a window that is too small, parts are cut off.

### Clients table

Columns, left to right:

| Header | Content | Sorts by | Direction when first chosen |
|---|---|---|---|
| (blank) | `[block]` for an unblocked IP, `[unblock]` for a blocked IP | blocked or not | blocked first |
| `IP` | the address | address, numerically (IPv4 before IPv6), not as a string | ascending |
| `ITEMS` | `items` | number | largest first |
| `FIRST` | age of `first_seen` | time | most recent first |
| `LAST` | age of `last_seen` | time | most recent first |
| `REQS` | `requests` | number | largest first |
| `UA` | `user_agent`, or `-` | not sortable | |

- The `UA` column is hidden at startup. When shown, it takes the remaining width and is truncated.
- Ages show only their largest unit: `42s`, `17m`, `5h` (below 48 hours), `12d`.
- Column widths adapt to their content. Columns that do not fit in the window are cut off on the right.
- Rows of blocked IPs are drawn in a clearly different style (for example red), so that the block state is visible without reading the first column.
- The initial sort is `ITEMS`, largest first. The header of the sort column carries `▼` or `▲`.
- Rows that compare equal are ordered by IP address ascending, so that rows never jitter.
- One row is selected and highlighted. The table scrolls to keep it visible.

**The selection is an IP address, not a row number.** When the rows re-sort, the same IP stays selected. Blocking by keyboard acts on the selected IP. A mouse click acts on what the most recently drawn frame showed at the clicked cell: record, while drawing, which IP each screen row holds and where each column and key-bar label is, and resolve clicks against that record. Do not recompute the layout from the current state when a click arrives. Use one description of the column geometry for both drawing and click resolution. Violating this blocks the wrong client.

### Events pane

Each row: local time as `HH:MM:SS`, client IP, outcome. The outcome is the number of lines handed out, or `blocked`, or `rejected` followed by the status code.

### Keys and mouse

| Input | Action |
|---|---|
| `Up`, `Down`, `PageUp`, `PageDown`, `Home`, `End` | Move the selection. |
| Mouse wheel over the clients table | Move the selection by 3 rows. |
| Left click on a client row, outside its first column | Select that client. |
| Left click on `[block]` or `[unblock]` | Block or unblock that row's client. |
| `F9` or `b` | Block or unblock the selected client. |
| Left click on a column header | Sort by that column. If it is already the sort column, invert the direction. |
| `F6` or `s` | Sort by the next sortable column, left to right, wrapping around. |
| `F7` or `i` | Invert the sort direction. |
| `F2` or `u` | Show or hide the `UA` column. |
| `F10` or `q` | Ask "Quit the server? y/n" in the key bar. `y` quits; any other key cancels. |
| `Ctrl-C` | Quit immediately. |
| Left click on a key-bar label | Same as that key. |

Every function key has a letter alternative because terminal emulators often keep some function keys for themselves. React only to the press of the left mouse button and to the wheel; ignore drags, releases, and movement.

Quitting the dashboard stops the server; that is why `q` asks first. `SIGTERM` and `SIGHUP` (sent when the terminal window is closed) cause a clean exit: restore the terminal, save the state file, exit with status 0.

Redraw after every input event, and at least 4 times per second.

## Implementation guidance

The operator's other Rust projects use edition 2024, `anyhow`, `clap`, `tokio`, `axum`, `serde`, `jiff`, and `tracing`; use the same here. Versions current on 2026-11-24: `ratatui` 0.30, `crossterm` 0.29, `axum` 0.8, `tokio` 1, `jiff` 1. `ratatui` 0.30 is split into several crates and its API has changed between releases, so read the docs.rs pages for the exact versions you pin instead of relying on memory. Take `crossterm` through the version that `ratatui` uses, so that there is only one copy.

Verified on docs.rs: `ratatui::init()` enables raw mode and the alternate screen and installs a panic hook that restores the terminal. It does not enable mouse capture; enable that yourself and disable it on every exit path, including panics. `axum::extract::ConnectInfo` requires serving with `Router::into_make_service_with_connect_info`.

Suggested shape (adjust if something simpler emerges):

- `queue.rs`: the item file reader and the in-memory position. No knowledge of HTTP, clients, or the state file.
- `state.rs`: clients, events, rates, and loading and saving the state file.
- `http.rs`: the router and its one handler.
- `tui.rs`: drawing as a function of (view settings, state, current time); input handling as a function from an input event to an action.
- `main.rs`: arguments, startup checks, wiring, shutdown.

One `std::sync::Mutex` around the shared state is enough; never hold it across an `.await`. The HTTP handler may do its small synchronous reads of the item file while holding it; that is a deliberate simplification. The dashboard runs on its own thread with a blocking input loop.

Cargo profiles, as in the operator's other projects, plus `panic = "abort"`:

```toml
[profile.dev]
debug = false
panic = "abort"

[profile.dev.package."*"]
opt-level = 3

[profile.release]
lto = true
codegen-units = 1
panic = "abort"
```

The repository's `AGENTS.md` applies. Its "Libraries to use" section names JavaScript libraries; the Rust counterparts are `assert!` and friends for invariants, `tracing` for logs, the built-in test harness for unit tests, and `proptest` for property-based tests. The local `cargo` wrapper supports only nightly Rust.

## Tests

Required, at minimum:

**Queue**

- Batches in file order; a short batch at the end of the file; wrap with `CYCLE=true`; no wrap with `CYCLE=false`; a last line without `\n`; empty lines; an empty file; bytes that are not valid UTF-8.
- Restart: stop after some batches, open a new queue at the position that the old one reported, continue with the next line.
- A starting position greater than the line count: error.
- Property test: for arbitrary file bytes and an arbitrary sequence of batch sizes, with a simulated restart (as above) at arbitrary points, the concatenated batches of one pass equal the file's lines, each followed by `\n`, in order, with none missing and none repeated.

**HTTP** (drive the router directly and supply the peer address yourself, so that tests can act as several client IPs; use tokio's paused time so that the 1 second delays cost nothing)

- Each row of the HTTP contract table, including that the non-`200` rows and `HEAD /` consume no lines.
- Each kind of invalid `max`, and lowering to `MAX_BATCH`.
- A blocked IP gets `403` while another IP is served normally; unblocking restores service.
- Counters: `items`, `requests`, `first_seen`, `last_seen`, `user_agent` after a mix of outcomes.
- A `User-Agent` containing an escape sequence is stored without control characters.

**State file**

- Save and load round trip, including the position and the wrap counter. Missing file: position 0 and no clients. A file holding only digits: that position and no clients. An empty file, damaged JSON, or a wrong version: startup error.
- Through the whole server: hand out some lines, exit cleanly, start again with the same state file, and the next request continues with the next line.

**Dashboard** (with ratatui's `TestBackend`)

- Clicking a header sorts by that column; clicking it again inverts; `F6`, `F7` do the same by keyboard.
- Clicking `[block]` blocks exactly the IP drawn on that row, also when new data arrived after the frame was drawn and would re-sort the rows.
- The selection stays on the same IP when the rows re-sort.
- `F2` shows and hides the `UA` column.
- Property test: drawing never panics for arbitrary terminal sizes from 1×1 upward and arbitrary client tables.

**One-time checks, not kept in the repository**

- Before deleting the Node version: run both versions against the same item file with the same sequence of valid requests and confirm byte-identical bodies, and that the position in the state file after a clean exit equals the number in the Node version's position file, including across a wrap. Also start the Rust version on a position file left by the Node version and confirm that it continues with the next line.
- Run the real binary in a detached `tmux` session, drive it with `tmux send-keys`, send requests with `curl`, and read the screen with `tmux capture-pane -p`. Confirm that the dashboard looks like the layout above, that keys work, and that the terminal is restored after quitting, after `SIGTERM`, and after a forced panic.

## Deliverables

1. The Rust crate at the repository root, building without warnings and passing `cargo clippy` and `cargo test`.
2. `README.md`: what the server does, how to run it, the HTTP contract, the files it writes, the dashboard keys. The repository has no README today.
3. Removal of `index.js`, `readlines.js`, and `package.json`. Keep `LICENSE`.
4. `.gitignore` with `target/`.

Suggested commit sequence, one logical change each:

1. The Rust server without client tracking or dashboard, at parity with the Node version plus the listed fixes, with a state file that holds only the position and the wrap counter, and with the queue and HTTP tests.
2. Client tracking, blocking in the HTTP handler, and the clients in the state file.
3. The dashboard.
4. `README.md` and removal of the Node files.

## Not included

Do not build these; mention them in your final report if you see a strong reason for one:

- Re-queueing the lines that a blocked IP took.
- Blocking an IP that has never made a request (the operator can edit the state file while the server is stopped).
- Collapsing repeated events from one IP into one row.
- Filtering or searching the clients table.
- Removing old client records.
- Appending to the item file while the server runs.
- Remembering the sort column or the `UA` toggle across restarts.
