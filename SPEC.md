# Spec: port batching-queue-server to Rust and add a terminal dashboard

## Goal

`batching-queue-server` hands out the lines of one text file to HTTP clients, in file order, a batch per request. Today it is a short Node script ([index.js](index.js)) plus a vendored line reader ([readlines.js](readlines.js)). It knows nothing about who took what.

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
- Lines are handed out at one configurable path only (`--path`, for example `/secretword`). A request to any other path receives `403`.
- No claim tracking, no "done" reports, no re-queueing of items. Handing out a line is the whole protocol.
- No rate limiting. No automatic blocking. No client nicknames. No block list shared between instances.
- The dashboard is a TUI inside the server process. No web UI.
- The User-Agent column is hidden by default and can be toggled.
- The clients table has a column with the reverse DNS name of each IP.
- Sorting works like `htop`: click a column header, or use function keys.
- Every setting is a command-line option. No environment variable is read.
- Cycling is off unless `--cycle` is given. (In the Node version it was on by default.)

## Command line

```
batching-queue-server [OPTIONS] ITEM_FILE
```

| Option | Default | Meaning |
|---|---|---|
| `--state-file FILE` | the `ITEM_FILE` path with `.bqs.json` appended | Where the state file is. For `/data/items.txt` the default is `/data/items.txt.bqs.json`, in the same directory as the item file. |
| `--port PORT` | `31417` | TCP port to listen on. |
| `--cycle` | cycling is off | Turn cycling on. With cycling off, the server stops handing out lines at the end of the file. With cycling on, it starts again at the first line after the last line was handed out. |
| `--max-batch N` | `10000` | Upper limit on the number of lines in one response. Must be at least 1. |
| `--path PATH` | `/` | The only request path at which lines are handed out; every other path gets `403`. A hard-to-guess value such as `/secretword` keeps strangers out. It must start with `/` and consist of printable ASCII characters other than space, `?`, and `#`; any other value is an argument error. |

Use `clap` (derive). Every setting is an option; no environment variable is read.

The Node version read the environment variables `PORT` and `CYCLE`. If either of them is set, exit with a startup error that names the option replacing it (`--port`, `--cycle`) and, for `CYCLE`, says that cycling is now off unless `--cycle` is given. Otherwise a launch script that was only partly updated would run on the wrong port, or with a cycling setting that its author did not intend.

`ITEM_FILE` is the only positional argument. The Node version took a second positional argument, the position file. A second positional argument is now a startup error, so that an old launch script cannot run with its position silently ignored. The error message must say that the position file argument was removed and that cycling is now off unless `--cycle` is given, and must point to the README section about moving from the Node version. Every old launch script hits this error, so it is the place to announce both changes.

Two servers that serve the same item file need different state files; give at least one of them `--state-file`.

### Startup order

1. Parse the arguments, and check that the environment variables `PORT` and `CYCLE` are not set.
2. Open and scan the item file.
3. Load the state file (see "State file") and check it against the item file.
4. Bind the port, without accepting connections yet. Listen on the IPv6 wildcard address `[::]` at the `--port` value, for both IPv4 and IPv6. Do not rely on the system default for this: clear `IPV6_V6ONLY` on the socket explicitly (for example with the `socket2` crate). There is no fallback to IPv4 only.
5. Save the state file once. This makes a state file location that cannot be written a startup error. Because it comes after every other check, a startup that fails changes nothing on disk.
6. Print one line to stderr that names the state file and says either which position was loaded from it or that it was newly created. Then start the dashboard and accept connections.

Argument errors are reported by `clap` in its usual way (its own message, exit status 2). Every other startup error (`PORT` or `CYCLE` set, unreadable item file, invalid or unreadable state file, state file that cannot be written, port in use) prints one clear message to stderr and exits with status 1, before the terminal is switched to the dashboard.

## The item queue

### Item file

- A line is a sequence of bytes ended by `\n`. A last line without a final `\n` is also a line. A file that ends with `\n` has no extra empty line after it.
- Lines are bytes, not text: never validate or convert them. Empty lines are items too. A `\r` before `\n` is part of the line.
- At startup, and again at each wrap, open the item file by its path and scan it once to count its lines. At startup the scan also finds the byte offset of the next line to serve. Then read forward from there. A scan of a very large file takes seconds, during which no request is served; this is accepted.
- Because the path is opened again at each wrap, an item file that was replaced during a pass (by renaming a new file over it) is served from the next pass on, as in the Node version.
- Precondition: nobody modifies the item file in place while the server runs. If the end of the file is reached after a number of lines that differs from the count of the last scan, set the total to the number actually read; do nothing more elaborate.

### Position

The position is the number of lines already handed out in the current pass over the file. This equals the 0-based index of the next line to serve.

The queue holds the position in memory. It is saved in the state file together with the client state (see "State file"); there is no separate position file. The queue itself does not read or write the state file: it is opened at a given position, reports its current position, and reports each wrap. The wrap counter is kept with the rest of the state, not in the queue.

If the starting position is greater than the number of lines in the item file, exit with an error.

### Taking a batch

To take up to `n` lines:

1. Repeat up to `n` times: try to read the next line. If there is one, add it to the batch and increase the position by 1. If there is none, the end of the file is reached; stop.
2. If the end of the file was reached, cycling is on, and the position is greater than 0: set the position to 0, open the item file again (see "Item file"), and report a wrap, which increases the wrap counter by 1. Do not continue reading in this request; the lines from the top are served to the next request.

The end of the file is reached only when an attempt to read a line finds none. A batch that takes exactly the last line has not reached the end; the next request reaches it, gets an empty batch, and causes the wrap. This is what the Node version does.

So a request that reaches the end of the file returns the lines it collected before the end (possibly none). With cycling off, the position stays at the end and every later batch is empty.

All batches are taken one at a time under one lock. Within one run of the server, two requests must never receive the same line in the same pass, and lines are handed out in file order. (After a crash, some lines are handed out again; see "Saving".)

The position advances when the batch is read, not when the client has received it. A client that disconnects before reading its response loses those lines until the next pass. This is accepted.

## HTTP contract

For each request, the first matching row applies:

| Condition | Status | Headers | Body | Delay before responding | Lines consumed |
|---|---|---|---|---|---|
| The client IP is blocked | `403` | | empty | 1 second | none |
| The path is not exactly the `--path` value | `403` | | empty | 1 second | none |
| The method is not `GET` | `405` | `Allow: GET` | empty | none | none |
| The `max` query parameter is invalid | `400` | `Content-Type: text/plain` | one line saying what is wrong | none | none |
| The server is paused | `200` | `Content-Type: text/plain` | empty | 1 second | none |
| The batch has at least one line | `200` | `Content-Type: text/plain` | each line followed by `\n` | none | the batch |
| The batch is empty | `200` | `Content-Type: text/plain` | empty | 1 second | none |

- `max` is optional; when absent it is 1. It is valid when the parameter appears at most once and its URL-decoded value consists of one or more ASCII digits `0` to `9` that form a number from 1 to 2^64 − 1. A valid `max` greater than the `--max-batch` value is lowered to that value (the client asked for "at most `max`" lines, so fewer is within the contract). Everything else is invalid: `0`, a leading `+` or `-`, `1.5`, `1e3`, `0x10`, an empty value, a number above 2^64 − 1, a repeated parameter.
- Other query parameters are ignored.
- The path of the request is compared with the `--path` value byte for byte, as received: no URL-decoding, no special treatment of a trailing `/`, no case folding. The query string is not part of the path.
- A wrong path gets exactly the reply that a blocked client gets. A stranger then cannot tell the two apart, and the delay slows down guessing. Do not show or log the `--path` value or the paths of requests.
- A request is counted, and its client record created, when the handler receives it. Requests that the HTTP library rejects before the handler runs (malformed requests) are not counted.
- `HEAD` on the right path from a client that is not blocked must get `405` and must not consume lines. Verified on docs.rs for axum 0.8.9: a route registered with `routing::get` is also called for `HEAD` requests. Register the handler so that this cannot happen, and test it.
- The `Content-Type` value is exactly `text/plain`, with no `charset`, as in the Node version.
- The 1 second delay slows down clients that poll an empty queue and clients that keep hammering while blocked. Waiting requests must not delay other requests.
- A block or unblock applies to every request whose handler starts after it. Requests already waiting out their delay finish as they started. The same holds for a pause or resume.
- The operator can pause the server from the dashboard (see "Keys and mouse"). A paused server answers exactly as if the queue were empty, so clients just idle, and no line is handed out. The pause is not saved: a restart always starts unpaused.

### Differences from the Node version (all intended)

These were observed by running the Node version:

- `GET /?max=1.5` and `GET /?max=-1` returned every remaining line of the file in one response. Now `400`.
- `max=` and `max` without a value served 1 line. `max=1e3`, `max=0x10`, `max=+5`, and `max=2.0` served 1000, 16, 5, and 2 lines. `max=0`, `max=abc`, and a repeated `max` got an empty `200` after 1 second. All of these are now `400`.
- There was no upper limit on `max`. Now the `--max-batch` value.
- A `GET` to any path, for example `/favicon.ico`, consumed a line. Now only the `--path` value (by default `/`) hands out lines, and every other path gets `403`.
- A request with a method other than `GET` never received a response. Now `405`.
- Lines that are not valid UTF-8 were sent with each invalid sequence replaced by U+FFFD. Now every line is sent unchanged.
- The position file was rewritten in place on every `GET`. An empty position file silently restarted the queue at line 0, and a damaged one made the Node version write `NaN` into it. Now the position is saved in the state file every 30 seconds, and a damaged state file is a startup error.
- The position file was the second positional argument. Now there is a state file, given with `--state-file` or found next to the item file.
- The port and the cycling setting came from the environment variables `PORT` and `CYCLE`. Now they are the options `--port` and `--cycle`.
- Cycling was on unless `CYCLE` was exactly `false`. Now cycling is off unless `--cycle` is given.

For valid requests from clients that are not blocked (`GET` on the `--path` value, with `max` absent or a number from 1 to the `--max-batch` value), served from an item file that is valid UTF-8, the response bodies must be byte-identical to the Node version's. After the same sequence of such requests, with the same cycling setting in both versions, the position must equal the number in the Node version's position file.

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

The `User-Agent` value comes from the network and is drawn on the operator's terminal. Before storing it: decode it as UTF-8 with replacement of invalid sequences, replace every control character (Unicode category Cc, which includes U+0080 to U+009F) with `?`, and truncate it to 200 `char`s. If a request has several `User-Agent` headers, use the first. No terminal escape sequence from a client may ever reach the terminal.

### Reverse DNS names

The dashboard shows the reverse DNS name (the PTR record) of each client IP, as a hint about who the client is.

- Look up each client once per run of the server: when its record is created, or at startup for a record loaded from the state file. Never repeat a lookup, also not after a failure.
- Lookups run in the background. No request ever waits for one.
- At most 8 lookups run at the same time; further ones wait their turn. A lookup that has not finished after 5 seconds is given up.
- Use the resolver configuration of the system (`/etc/resolv.conf`).
- The result is the first name in the answer, without its final `.`. The owner of the address controls this name, and it is drawn on the operator's terminal, so clean it in the same way as a `User-Agent` value, and truncate it to 100 `char`s.
- So a client is in one of three states: not looked up yet; looked up, with a name; looked up, without a name (no PTR record, an error, or given up).
- Names are kept in memory only. They are not saved in the state file.
- Without a dashboard, do no lookups.
- The name is not verified: nothing checks that the name resolves back to the IP, so the owner of an address can publish any name. The README must say this.

### Events

Keep the most recent 1000 events in memory. One event per request:

- time
- client IP
- outcome: the number of lines handed out (possibly 0), or `blocked`, or `wrong path`, or `paused`, or `rejected` together with the status code (`400` or `405`)

### Rates

Keep enough to show, for approximately the last 60 seconds, the number of lines handed out and the number of requests. Sixty one-second buckets are enough; being off by up to one second is fine.

### Time

Use wall-clock timestamps (`jiff::Timestamp`) for `first_seen`, `last_seen`, and event times. The clock may jump. An age that would be negative is shown as `0s`; nothing may panic or overflow because of a timestamp in the future.

Pass the current time into the functions that need it instead of reading the clock inside them, so that tests control time.

## State file

One file holds everything that survives a restart: the position, the wrap counter, and the clients. Its path is the value of `--state-file`, or by default the `ITEM_FILE` path with `.bqs.json` appended.

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

Timestamps are UTC in the RFC 3339 form shown. Counters and the position are unsigned 64-bit integers.

### Loading at startup

- Opening the file fails because it does not exist: position 0, wrap counter 0, no clients. Any other error from opening or reading it is a startup error. (Do not use a check like `Path::exists`, which also reports "does not exist" for a file that exists but cannot be read.)
- Otherwise the file must be a JSON object with all four fields shown above: `version` equal to 1, `position`, `wraps`, and `clients`. Every client must have all six fields; `user_agent` is `null` for a client without one. An empty file, damaged JSON, another `version`, a missing field, or a value of the wrong type is a startup error. Never silently restart at line 0 and never silently discard a block list.
- A `position` greater than the number of lines in the item file is a startup error.
- Loaded data is treated like live data: convert every IP key with `IpAddr::to_canonical` (two keys that become equal are a startup error), and pass every `user_agent` through the same cleaning as a live header.
- If `--state-file` names the item file itself, exit with an error before reading or writing anything.

There is no automatic conversion of a Node position file. The README explains how to create a state file from one (see "Deliverables").

### Saving

- Save by writing a temporary file in the same directory, calling `fsync` on it, and renaming it over the state file. Without the `fsync`, a power loss can leave an empty state file on some filesystems.
- Save once at startup (step 5 of "Startup order").
- After that, save every 30 seconds if anything in the saved data changed since the last save, immediately when a block or unblock happens, and at every clean exit (see "Exiting").
- Every save is one consistent snapshot: the position, the wrap counter, and every client record are from the same instant. Take the snapshot while holding the state lock; write it to disk after releasing that lock, so that a slow disk does not stall requests.
- Two saves must never interleave, and an older snapshot must never overwrite a newer one.
- Events, rates, and reverse DNS names are not saved.

The state is deliberately not saved on every request, because each save rewrites every client record. The consequence: after a crash, a kill, or a power loss, the server restarts from the last saved snapshot, which is normally at most about 30 seconds old, and the lines handed out after that snapshot are handed out a second time. This is accepted. No line is skipped because of it. A clean exit loses nothing.

## Exiting

Each of these is a clean exit: `q` confirmed with `y`, `Ctrl-C` in the dashboard, and the signals `SIGINT`, `SIGTERM`, and `SIGHUP` (sent when the terminal window is closed). A clean exit does these steps in this order:

1. Stop handing out lines. From this moment no batch is taken; requests still arriving may be left unanswered.
2. Save the state file. This snapshot is therefore the final state.
3. Restore the terminal. After `SIGHUP` the terminal is already gone, so restoring it can fail; ignore that failure. It must not cause a panic, and it comes after the save so that it cannot prevent the save. Check how the restore function you use behaves when the terminal is gone.
4. Exit with status 0, or with status 1 if the save failed.

## Failures while running

If saving the state file fails, or reading the item file fails, the server must stop: stop handing out lines, restore the terminal, print the error to stderr, and exit with status 1. A server that kept handing out lines while its saves fail would hand out an unbounded number of them again after a restart. A request whose read of the item file failed gets no response.

A panic anywhere must end the whole process, never leave a half-working server. Set `panic = "abort"` in the Cargo profiles, and make sure the panic hook restores the terminal (including mouse capture) first.

## Dashboard

### When it runs

If both stdin and stdout are terminals, the server draws the dashboard. Otherwise it runs without one and writes one log line per event to stderr with `tracing`. In dashboard mode, nothing but the dashboard may write to the terminal, so do not install a `tracing` output there.

### Layout

From top to bottom. The numbers are examples.

```
items.txt :31417  1,234,567/9,876,543 12.5%  ETA 14d  412 items/min  38 req/min  wrap 3  3 IPs, 1 blocked
           IP             ITEMS▼  FIRST  LAST    REQS  HOST
[block]    203.0.113.7    12,400     3d    2s     124  crawler-7.example.net
[unblock]  198.51.100.9      300     1h    0s   1,215  -
[block]    2001:db8::17        0     5m    5m       1  scanner.example.org
11:56:10 2001:db8::17  wrong path
12:01:02 203.0.113.7   100
12:01:04 198.51.100.9  blocked
F2 UA  F3 Host  F5 Pause  F6 Sort  F7 Invert  F9 Block  F10 Quit
```

1. **Status line** (1 row). Its parts come in this order, most important first, because a narrow window cuts off the right end:
   - item file name without its directory
   - port
   - position `/` total lines
   - percent with one decimal, or `-` when the item file has no lines
   - `ETA` and the estimated time until the end of the current pass: the remaining lines divided by the 60 second rate, shown in the same form as an age (see "Clients table"), or as `-` when that rate is 0. When cycling is off and the position is at the end of the file, show `DONE` in place of `ETA` and the estimate.
   - lines handed out in the last 60 seconds
   - requests in the last 60 seconds
   - wrap counter (omit when cycling is off)
   - number of known client IPs, and how many of them are blocked
2. **Clients table**: one header row, then one row per client.
3. **Events pane**: the most recent events, newest at the bottom, as many as fit.
4. **Key bar** (1 row).

While the server is paused, the status line starts with `PAUSED` in a standout style (for example reverse video), and the key bar shows `F5 Resume` in place of `F5 Pause`.

Let R be the number of rows between the status line and the key bar. The events pane gets R / 3 rows, rounded down; the clients table, including its header row, gets the rest.

No borders and no titles; they cost rows and columns. Numbers of items and requests use `,` as thousands separator. The dashboard must draw without panicking at every terminal size, down to 1×1, and with an empty item file and with no clients. In a window that is too small, parts are cut off; which parts is up to you.

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
| `HOST` | the reverse DNS name; `-` when the lookup found none; empty while the client has not been looked up yet | the name with its labels in reverse order (`host.example.com` sorts as `com.example.host`), ignoring case, so that clients of one domain are adjacent; clients without a name come after all names | ascending |
| `UA` | `user_agent`, or `-` | not sortable | |

- The `HOST` column is shown at startup and can be hidden. Its width is that of the longest name in it, but at most 40 columns; longer names are truncated.
- The `UA` column is hidden at startup. When shown, it takes the remaining width and is truncated.
- A hidden column cannot become the sort column. If the sort column gets hidden, the order stays as it is.
- Ages show only their largest unit: `42s`, `17m`, `5h` (below 48 hours), `12d`.
- Column widths adapt to their content. Columns that do not fit in the window are cut off on the right.
- Rows of blocked IPs are drawn in a clearly different style (for example red), so that the block state is visible without reading the first column.
- The initial sort is `ITEMS`, largest first. The header of the sort column carries `▼` when the largest value is first (for the time columns: the most recent; for the first column: blocked) and `▲` for the opposite order. So `IP` ascending and `HOST` ascending carry `▲`.
- Making a column the sort column, by click or by key, always starts with its "direction when first chosen", also when it has been the sort column before.
- Rows that compare equal are ordered by IP address ascending, so that rows never jitter.
- One row is selected and highlighted, and the table scrolls to keep it visible. With no clients, nothing is selected. When nothing is selected and a client exists, the first row becomes selected.

**The selection is an IP address, not a row number.** When the rows re-sort, the same IP stays selected. Blocking always acts on the selected IP.

**A mouse click never blocks a client that was not already selected.** Rows re-sort several times per second, faster than a hand moves, so the row under the pointer can change just before a click lands. Therefore a click on any part of a client row selects that client, and only a click on `[block]` or `[unblock]` in the row of the client that is already selected blocks or unblocks it. The operator has then seen the highlight on that client before the second click.

A mouse click is resolved against what the most recently drawn frame showed at the clicked cell: record, while drawing, which IP each screen row holds and where each column and key-bar label is, and resolve clicks against that record. Do not recompute the layout from the current state when a click arrives. Use one description of the column geometry for both drawing and click resolution.

### Terminal title

While the dashboard runs, set the title of the terminal window, so that a list of windows shows the state of every instance. The title is the first of these that applies:

| Situation | Title |
|---|---|
| Cycling is off and the position is at the end of the file | `DONE items.txt` |
| The server is paused | `PAUSED 12.5% items.txt` |
| Otherwise | `12.5% items.txt` |

The percent is the one from the status line. Write the title only when its text changes. At every exit that restores the terminal (a clean exit, or a failure while running), set the title to `stopped items.txt`.

The item file name comes from the command line and is written to the terminal, in the title and in the status line. Clean it first in the same way as a `User-Agent` value.

### Events pane

Each row: local time as `HH:MM:SS`, client IP, outcome. The outcome is the number of lines handed out, or `blocked`, or `wrong path`, or `paused`, or `rejected` followed by the status code.

### Keys and mouse

| Input | Action |
|---|---|
| `Up`, `Down`, `PageUp`, `PageDown`, `Home`, `End` | Move the selection. |
| Mouse wheel over the clients table | Move the selection by 3 rows. |
| Left click on a client row | Select that client. |
| Left click on `[block]` or `[unblock]` in the row of the selected client | Block or unblock that client. |
| `F9` or `b` | Block or unblock the selected client. |
| Left click on a column header | Sort by that column. If it is already the sort column, invert the direction. A click on the `UA` header does nothing. |
| `F6` or `s` | Sort by the next sortable column that is shown, left to right, wrapping around. |
| `F7` or `i` | Invert the sort direction. |
| `F2` or `u` | Show or hide the `UA` column. |
| `F3` or `h` | Show or hide the `HOST` column. |
| `F5` or `p` | Pause the server, or resume it. |
| `F10` or `q` | Ask "Quit the server? y/n" in the key bar. `y` quits; any other key cancels. |
| `Ctrl-C` | Quit immediately, also while the quit question is shown. |
| Left click on a key-bar label | Same as that key. |

Every function key has a letter alternative because terminal emulators often keep some function keys for themselves. React to key presses and key repeats, not to key releases. React only to the press of the left mouse button and to the wheel; ignore drags, releases, and movement.

Quitting the dashboard stops the server; that is why `q` asks first. Every way of quitting is a clean exit (see "Exiting").

Redraw after every input event, and at least 4 times per second. Wait for input with a timeout, so that neither a redraw nor an exit has to wait for a key.

## Implementation guidance

The operator's other Rust projects use edition 2024, `anyhow`, `clap`, `tokio`, `axum`, `serde`, `jiff`, and `tracing`; use the same here. Versions current on 2026-11-24: `ratatui` 0.30, `crossterm` 0.29, `axum` 0.8, `tokio` 1, `jiff` 1 (its `serde` feature is needed for the timestamps in the state file). `ratatui` 0.30 is split into several crates and its API has changed between releases, so read the docs.rs pages for the exact versions you pin instead of relying on memory. Take `crossterm` through the version that `ratatui` uses, so that there is only one copy.

For reverse DNS, use `hickory-resolver` (0.26 is current). Verified on docs.rs for 0.26.4: `Resolver::builder_tokio()` builds a resolver from the system configuration, and `Resolver::reverse_lookup` exists; check their exact signatures there. Make the lookup function replaceable, so that tests supply their own and never send a DNS query.

Verified on docs.rs: `ratatui::init()` enables raw mode and the alternate screen and installs a panic hook that restores the terminal. It does not enable mouse capture; enable that yourself and disable it on every exit path, including panics. `axum::extract::ConnectInfo` works only when the server is started with connection info, for example through `Router::into_make_service_with_connect_info`.

Suggested shape (adjust if something simpler emerges):

- `queue.rs`: the item file reader and the in-memory position. No knowledge of HTTP, clients, or the state file.
- `state.rs`: clients, events, rates, and loading and saving the state file.
- `http.rs`: the router and its one handler.
- `tui.rs`: drawing as a function of (view settings, state, current time); input handling as a function from an input event to an action.
- `main.rs`: arguments, startup checks, wiring, shutdown.

One `std::sync::Mutex` around the shared state is enough; never hold it across an `.await`. The HTTP handler may do its small synchronous reads of the item file while holding it; that is a deliberate simplification. The dashboard runs on its own thread. For each frame it copies what it needs while holding the lock, and writes to the terminal after releasing it: a terminal that stops reading (for example a stalled SSH session) must not stall requests.

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

- Batches in file order; a short batch at the end of the file; a batch that takes exactly the last line, followed by a request that gets an empty batch and wraps; no wrap when cycling is off, which is the default; a last line without `\n`; empty lines; an empty file; bytes that are not valid UTF-8.
- An item file replaced during a pass (by renaming a new file over it) is served from the next pass on.
- Restart: stop after some batches, open a new queue at the position that the old one reported, continue with the next line.
- A starting position greater than the line count: error.
- Property test: for arbitrary file bytes and an arbitrary sequence of batch sizes, with a simulated restart (as above) at arbitrary points, the concatenation of all batches taken before the first wrap is a prefix of the file's lines, each followed by `\n`; if a wrap happened, it is all of them.

**HTTP** (drive the router directly and supply the peer address yourself, so that tests can act as several client IPs; use tokio's paused time so that the 1 second delays cost nothing)

- Each row of the HTTP contract table, including that the non-`200` rows and `HEAD` consume no lines.
- With `--path /secret`: `/secret` and `/secret?max=2` are served; `/`, `/secret/`, `/SECRET`, and `/%73ecret` get `403` after 1 second and consume no lines. A `--path` value without a leading `/`, or containing `?`, is an argument error.
- Each kind of invalid `max` listed above, and lowering to the `--max-batch` value.
- A blocked IP gets `403` while another IP is served normally; unblocking restores service.
- While paused: a valid request gets an empty `200` after 1 second and consumes no lines; a blocked IP still gets `403`. After resuming, the next request gets the next line.
- Many concurrent requests: every line of the file is handed out exactly once.
- Counters: `items`, `requests`, `first_seen`, `last_seen`, `user_agent` after a mix of outcomes.
- A `User-Agent` containing a tab, the control character U+009B, and bytes that are not valid UTF-8 is stored without control characters. The HTTP library itself rejects most ASCII control bytes in a header, so also test the cleaning function directly with an escape sequence.
- One test through a real TCP listener on `127.0.0.1` with port 0: a `GET` is served and attributed to `127.0.0.1`, and a `HEAD` gets `405`. This proves the wiring of the peer address, which the other tests bypass.

**Reverse DNS** (with a lookup function supplied by the test, and tokio's paused time)

- Many requests from one IP cause one lookup. A client loaded from the state file is looked up at startup.
- A request is answered while the lookup for its client has not finished.
- With more than 8 new clients at once, at most 8 lookups run at the same time, and all of them finish eventually.
- A lookup that never answers is given up after 5 seconds, and the client then counts as looked up without a name.
- A name with a final `.`, with control characters, or longer than 100 `char`s is stored without the `.`, cleaned, and truncated.

**State file** (every test uses an item file in its own fresh temporary directory, because the default state file is written next to the item file)

- Save and load round trip, including the position and the wrap counter. Missing file: position 0 and no clients.
- Startup errors: an empty file; damaged JSON; a wrong `version`; a missing field; a file holding only digits (a Node position file); a directory at the state file path; a `position` greater than the line count; `--state-file` naming the item file; a state file path whose parent directory does not exist (this is how to test "cannot be written"; a read-only directory is still writable for root).
- A startup that fails leaves an existing state file unchanged.
- Through the whole server: hand out some lines, exit cleanly, start again with the same state file, and the next request continues with the next line; the wrap counter is kept too.
- With paused time: after a hand-out, the file on disk holds the new position within 30 seconds, without an exit. After a block, the file on disk holds it immediately.
- A start from a snapshot that is older than the last hand-outs repeats those lines and skips none.
- Without `--state-file`, the state file for `items.txt` is `items.txt.bqs.json` (append to the whole path; `Path::with_extension` would give `items.bqs.json`). A second positional argument is an error whose message mentions `--cycle` and points to the README. `PORT` or `CYCLE` set in the environment is a startup error whose message names the replacing option.

**Dashboard** (with ratatui's `TestBackend`)

- Clicking a header sorts by that column; clicking it again inverts; `F6`, `F7` do the same by keyboard.
- Clicking `[block]` in a row that is not selected only selects that client. Clicking `[block]` in the selected row blocks exactly that IP, also when new data arrived after the frame was drawn and would re-sort the rows.
- The selection stays on the same IP when the rows re-sort.
- `F2` shows and hides the `UA` column; `F3` shows and hides the `HOST` column.
- Sorting by `HOST` puts `b.example.com` next to `a.example.com` and before `a.example.net`, and puts clients without a name last. The `HOST` cell is `-` for a client looked up without a name and empty for a client not looked up yet.
- `F5` pauses and resumes; while paused, the status line starts with `PAUSED` and the key bar shows `F5 Resume`.
- The status line shows the number of client IPs and of blocked ones, and shows `DONE` when cycling is off and the position is at the end of the file.
- The title text for each row of the "Terminal title" table, and for an item file without lines.
- Drawing works with an empty item file and with no clients.
- Property test: drawing never panics for arbitrary terminal sizes from 1×1 to 300×100 and arbitrary client tables.

**One-time checks, not kept in the repository**

- Before deleting the Node version: give each version its own copy of one item file that is valid UTF-8, start both at the same position, send both the same sequence of valid requests, and confirm byte-identical bodies. After a clean exit, the position in the Rust version's state file must equal the number in the Node version's position file. Do this once with cycling on in both versions (the Node version's default; `--cycle` for the Rust version), including a wrap, and once with cycling off in both (`CYCLE=false` for the Node version), including requests after the end of the file. Then follow the README's instructions for moving from the Node version, using that position file, and confirm that the Rust version continues with the next line.
- Run the real binary in a detached `tmux` session, drive it with `tmux send-keys`, send requests with `curl`, and read the screen with `tmux capture-pane -p`. Confirm that the dashboard looks like the layout above and that keys work. Confirm that the terminal is restored after quitting, after `SIGTERM`, and after a forced panic. Confirm that the state file is saved when the `tmux` session is killed while the server runs (this sends `SIGHUP`).

## Deliverables

1. The Rust crate at the repository root, with its `Cargo.lock` committed, building without warnings and passing `cargo clippy` and `cargo test`.
2. `README.md`: what the server does, how to run it, the HTTP contract, the state file, the dashboard keys, and that reverse DNS names are not verified. The repository has no README today. Include a section "Moving from the Node version": replace the environment variable `PORT` with `--port`; add `--cycle` unless the old launch set `CYCLE=false`, because cycling is now off by default; remove `CYCLE`; remove the second positional argument; to continue a queue that is in progress, stop the Node version and create the state file from the number in the old position file, for example `echo '{"version":1,"position":'"$(cat OLD_POSITION_FILE)"',"wraps":0,"clients":{}}' > ITEM_FILE.bqs.json`. The old position file stays untouched, so going back to the Node version remains possible. Also say that the position is changed by editing `position` in the state file while the server is stopped.
3. Removal of `index.js`, `readlines.js`, `package.json`, and `LICENSE`. That `LICENSE` file is the license of the vendored `readlines.js` only. Do not add a license for the Rust crate; the operator has not chosen one.
4. `.gitignore` with `target/`.

Suggested commit sequence, one logical change each:

1. The Rust server without client tracking or dashboard, at parity with the Node version plus the listed fixes, with the state file (its `clients` always `{}` at this step, so that the format does not change later), and with the tests that apply at this step.
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
- Detecting two servers that use the same state file.
- Remembering the sort column or the `UA` and `HOST` toggles across restarts.
- Verifying a reverse DNS name with a forward lookup, and saving names across restarts.
