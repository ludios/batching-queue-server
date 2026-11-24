# Spec: port batching-queue-server to Rust and add a terminal dashboard

## Goal

`batching-queue-server` hands out the lines of one text file to HTTP clients, in file order, a batch per request. Before the port, it was a short Node script (`index.js`) plus a vendored line reader (`readlines.js`), which were removed after commit 24e616cf1c82 (see `git show 24e616cf1c82:index.js`). It knew nothing about who took what.

Replace it with a single Rust binary that:

1. Serves the same HTTP contract to existing clients (with the bug fixes listed below).
2. Tracks every client IP address: items taken, requests made, first seen, last seen, most recent User-Agent.
3. Lets the operator block and unblock an IP address.
4. Shows all of this in a compact terminal dashboard (TUI) drawn by the server process itself. The operator runs many instances, each in its own small terminal window, so every row of the screen matters.
5. Keeps the block list and the per-IP numbers across restarts.

The operator will read and maintain this code. Prefer the plainest design that meets the spec: few modules, few types, no cleverness. Read `index.js` (`git show 24e616cf1c82:index.js`) first; it is short.

## Decisions already made by the operator (do not reopen)

- The client's address is the TCP peer address. Do not read `X-Forwarded-For` or any other header for it.
- A blocked IP receives `403` with an empty body, after a 1 second delay.
- Lines are handed out at one configurable path only (`--path`, for example `/secretword`). A request to any other path receives `403`.
- No claim tracking, no "done" reports, no re-queueing of items. Handing out a line is the whole protocol.
- No rate limiting. No automatic blocking. No client nicknames. No block list shared between instances.
- The dashboard is a TUI inside the server process. No web UI.
- The User-Agent column is hidden by default and can be toggled.
- The sort column, the sort direction, and whether the `HOST` and `UA` columns are shown are remembered across restarts.
- The clients table has a column with the reverse DNS name of each IP.
- Sorting works like `htop`: click a column header, or use function keys.
- Every setting of the server is a command-line option. No setting is taken from an environment variable. (The view settings of the dashboard are in the state file.)
- Cycling is off unless `--cycle` is given. (In the Node version it was on by default.)

## Command line

```
batching-queue-server [OPTIONS] ITEM_FILE
```

| Option | Default | Meaning |
|---|---|---|
| `--state-file FILE` | the `ITEM_FILE` path with `.bqs.json` appended | Where the state file is. For `/data/items.txt` the default is `/data/items.txt.bqs.json`, in the same directory as the item file. |
| `--port PORT` | `31417` | TCP port to listen on, from 1 to 65535. |
| `--cycle` | cycling is off | Turn cycling on. With cycling off, the server stops handing out lines when it has handed out the last line of the file. With cycling on, it then continues with the first line of the file again. |
| `--max-batch N` | `10000` | Upper limit on the number of lines in one response. Must be at least 1. |
| `--path PATH` | `/` | The only request path at which lines are handed out; every other path gets `403`. A long random value such as `/k3v9x2m7q4w8` keeps strangers out. It must start with `/`, consist only of `/` and the characters `A` to `Z`, `a` to `z`, `0` to `9`, `-`, `_`, `.`, and `~`, and have no segment that is `.` or `..`; any other value is an argument error. (Clients send exactly these characters unchanged. Other characters are rewritten or rejected on the way, so a path containing them could never match.) |

Use `clap` (derive). Every setting of the server is an option; no setting is taken from an environment variable.

`ITEM_FILE` is the only positional argument; a second one is an argument error.

Two servers that serve the same item file need different state files; give at least one of them `--state-file`. A server refuses to start while another running server uses the same state file path (see "Lock file").

### Startup order

1. Parse the arguments. If the state file path or the lock file path names the same existing file as the item file path (compare them after `std::fs::canonicalize`), exit with an error.
2. Take the lock of the lock file (see "Lock file"). This comes before every slow or writing step, so that a second server is refused at once.
3. Load the state file (see "State file").
4. Open and scan the item file, and check that the loaded position is not greater than its number of lines.
5. Bind the port, without accepting connections yet. Listen on the IPv6 wildcard address `[::]` at the `--port` value, for both IPv4 and IPv6. Do not rely on the system default for this: clear `IPV6_V6ONLY` on the socket explicitly (for example with the `socket2` crate). There is no fallback to IPv4 only.
6. Save the state file once. This makes a state file location that cannot be written a startup error. Because it comes after every other check, a startup that fails changes nothing on disk, except that it may have created the empty lock file.
7. Print one line to stderr that names the state file and says either which position was loaded from it or that it was newly created, and whether cycling is on. Then start the dashboard and accept connections.

Argument errors are reported by `clap` in its usual way (its own message, exit status 2). Every other startup error (unreadable item file, lock file that cannot be opened or locked, state file in use by another server, invalid or unreadable state file, state file path that names the item file, state file that cannot be written, port that cannot be bound) prints one clear message to stderr and exits with status 1, before the terminal is switched to the dashboard.

## The item queue

### Item file

- A line is a sequence of bytes ended by `\n`. A last line without a final `\n` is also a line. A file that ends with `\n` has no extra empty line after it.
- Lines are bytes, not text: never validate or convert them. Empty lines are items too. A `\r` before `\n` is part of the line.
- At startup, and again at each wrap, open the item file by its path and scan it once to count its lines. At startup the scan also finds the byte offset of the next line to serve. Then read forward from there. A scan of a very large file takes seconds, during which no request is served; this is accepted.
- Because the path is opened again at each wrap, an item file that was replaced during a pass (by renaming a new file over it) is served from the next pass on, as in the Node version. This needs cycling to be on; with cycling off there is no wrap.
- Precondition: nobody modifies the item file in place while the server runs. If the end of the file is reached after a number of lines that differs from the count of the last scan, set the total to the number actually read; do nothing more elaborate.
- A restart applies the saved position to whatever file is at the path then. The server cannot notice that the item file was replaced while it was stopped, or replaced during a pass that a restart interrupted. To serve a new file from its first line, set `position` to 0 in the state file while the server is stopped.

### Position

The position is the number of lines already handed out in the current pass over the file. This equals the 0-based index of the next line to serve.

The queue holds the position in memory. It is saved in the state file together with the client state (see "State file"); there is no separate position file. The queue itself does not read or write the state file: it is opened at a given position, reports its current position, and reports each wrap. The wrap counter is kept with the rest of the state, not in the queue.

If the starting position is greater than the number of lines in the item file, exit with an error.

### Taking a batch

To take up to `n` lines:

1. Repeat up to `n` times: try to read the next line. If there is one, add it to the batch and increase the position by 1. If there is none, the end of the file is reached; stop.
2. If the end of the file was reached, cycling is on, and the position is greater than 0: set the position to 0, open the item file again (see "Item file"), and report a wrap, which increases the wrap counter by 1. Do not continue reading in this request; the lines from the top are served to the next request.

The end of the file is reached only when an attempt to read a line finds none. A batch that takes exactly the last line has not reached the end; the next request reaches it, gets an empty batch, and (with cycling on) causes the wrap. This is what the Node version does.

So a request that reaches the end of the file returns the lines it collected before the end (possibly none). With cycling off, the position stays at the end and every later batch is empty.

All batches are taken one at a time while holding the state mutex (the one `std::sync::Mutex` around the shared state; not to be confused with the lock file). Within one run of the server, two requests must never receive the same line in the same pass, and lines are handed out in file order. (After a crash, some lines are handed out again; see "Saving".)

The position advances when the batch is read, not when the client has received it. A client that disconnects before reading its response loses those lines; with cycling on they are handed out again in the next pass. This is accepted.

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
- A wrong path gets exactly the reply that a blocked client gets, so that a stranger cannot tell the two apart. Only a long random path keeps strangers out; the delay does not limit a guesser who sends many requests at once. Do not show the `--path` value in the dashboard, and do not log it or the paths of requests. Hiding it from other users of the same machine is not a goal: a command line is visible in the process list.
- Handle every request in one handler (the router's fallback) that compares the path of the request with the `--path` value itself. Never register the `--path` value as a route pattern.
- A request is counted, and its client record created, when the handler receives it. Requests that the HTTP library rejects before the handler runs (malformed requests) are not counted.
- `HEAD` on the right path from a client that is not blocked must get `405` and must not consume lines. Verified on docs.rs for axum 0.8.9: a route registered with `routing::get` is also called for `HEAD` requests. Register the handler so that this cannot happen, and test it.
- The `Content-Type` value is exactly `text/plain`, with no `charset`, as in the Node version.
- The 1 second delay slows down clients that poll an empty queue and clients that keep hammering while blocked. Waiting requests must not delay other requests.
- The handler decides the outcome of a request in one step, while holding the state mutex: it checks the rows of the table in order and, if none of the first five applies, takes the batch. A block, unblock, pause, or resume applies to every request whose decision comes after it. Requests already waiting out their delay finish as they started.
- The operator can pause the server from the dashboard (see "Keys and mouse"). A paused server answers exactly as if the queue were empty, so clients just idle, and no line is handed out. The pause is not saved: a restart always starts unpaused.

### Differences from the Node version (all intended)

These were observed by running the Node version:

- `GET /?max=1.5` and `GET /?max=-1` returned every remaining line of the file in one response. Now `400`.
- `max=` and `max` without a value served 1 line. `max=1e3`, `max=0x10`, `max=+5`, and `max=2.0` served 1000, 16, 5, and 2 lines. `max=0`, `max=abc`, and a repeated `max` got an empty `200` after 1 second. All of these are now `400`.
- There was no upper limit on `max`. Now the `--max-batch` value.
- A `GET` to any path, for example `/favicon.ico`, consumed a line. Now only the `--path` value (by default `/`) hands out lines, and every other path gets `403`.
- A request with a method other than `GET` never received a response. Now `405` (on the right path, from a client that is not blocked).
- Lines that are not valid UTF-8 were sent with each invalid sequence replaced by U+FFFD. Now every line is sent unchanged.
- The position file was rewritten in place on every `GET`. An empty position file silently restarted the queue at line 0, and a damaged one made the Node version write `NaN` into it. Now the position is saved in the state file at most about 30 seconds after it changes and at every clean exit (see "Saving"), and a damaged state file is a startup error.
- The position file was the second positional argument. Now there is a state file, given with `--state-file` or found next to the item file.
- The port and the cycling setting came from the environment variables `PORT` and `CYCLE`. Now they are the options `--port` and `--cycle`.
- Cycling was on unless `CYCLE` was exactly `false`. Now cycling is off unless `--cycle` is given.

For valid requests from clients that are not blocked (`GET` on the `--path` value, with `max` absent or a number from 1 to the `--max-batch` value), served from an item file that is valid UTF-8 by a server that is not paused, with the same cycling setting in both versions: the response bodies must be byte-identical to the Node version's, and after the same sequence of such requests the position must equal the number in the Node version's position file.

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

- Look up each client without a name at most once per run of the server: when it makes its first request in this run. A client that was loaded from the state file and makes no request in this run is not looked up. Never repeat a lookup in the same run, also not after a failure. A client with a name, found in this run or loaded from the state file, is never looked up again.
- Lookups run in the background. No request ever waits for one.
- At most 8 lookups run at the same time; further ones wait their turn, for as long as that takes. A lookup is given up 5 seconds after it started running (not 5 seconds after it began to wait).
- Use the resolver configuration of the system (`/etc/resolv.conf`).
- If the resolver cannot be set up (for example because `/etc/resolv.conf` is missing), that is not an error: the server runs, and every client that would be looked up counts as looked up without a name.
- The result is the target name of the first `PTR` record in the answer, in hickory's ASCII form (`Name::to_ascii`), without its final `.`. Skip records of every other type; an answer can contain a `CNAME` record before the `PTR` record. A result that is empty counts as no name. The ASCII form keeps internationalized labels as `xn--…`, and escapes unusual characters in the style of a DNS zone file: as `\` and the character (`\[`), or as `\` and its byte in three octal digits (`\033` for ESC). So each `char` of it takes one screen column. The owner of the reverse zone controls this name, and it is drawn on the operator's terminal, so also replace each control character in it with `?`, as in a `User-Agent` value; the ASCII form doesn't have any, but this keeps the terminal safe whatever that form becomes. Don't cut it to its first 200 `char`s, as a `User-Agent` value is cut: if it is longer than 255 `char`s, keep its last 255, because the domain is at the end.
- So a client is in one of three states: without a name and not looked up yet in this run (or its lookup has not finished); with a name, found in this run or loaded from the state file; looked up in this run, without a name (no `PTR` record, an error, or given up).
- Save a found name in the client's record in the state file (see "State file"), so that the dashboard shows it from the start of the next run without another lookup. Don't save that a lookup found no name: the cause may have been a failure that doesn't last, so the client is looked up again in the next run.
- Without a dashboard, do no lookups.
- With a dashboard, lookups run whether or not the `HOST` column is shown.
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

One file holds everything that survives a restart: the position, the wrap counter, the clients, and the view settings of the dashboard. Its path is the value of `--state-file`, or by default the `ITEM_FILE` path with `.bqs.json` appended.

```json
{
  "version": 1,
  "position": 1234567,
  "wraps": 3,
  "view": {
    "sort_column": "items",
    "sort_inverted": false,
    "show_host": true,
    "show_ua": false
  },
  "clients": {
    "203.0.113.7": {
      "blocked": false,
      "items": 12400,
      "requests": 124,
      "first_seen": "2026-11-24T12:01:02Z",
      "last_seen": "2026-11-24T18:40:11Z",
      "user_agent": "curl/8.9.1",
      "host": "crawler-7.example.net"
    }
  }
}
```

Timestamps are UTC in the RFC 3339 form shown. Counters and the position are unsigned 64-bit integers.

`view` holds the view settings of the dashboard (see "Clients table"). They are part of the shared state, like the clients, so that every save includes them. A server running without a dashboard keeps the loaded view settings and saves them back unchanged.

| Field | Meaning | Value without a saved `view` |
|---|---|---|
| `sort_column` | The sort column: one of `blocked` (the first column), `ip`, `items`, `first_seen`, `last_seen`, `requests`, `host`. | `items` |
| `sort_inverted` | `false` when the order is the sort column's "direction when first chosen", `true` when it is the opposite. | `false` |
| `show_host` | Whether the `HOST` column is shown. | `true` |
| `show_ua` | Whether the `UA` column is shown. | `false` |

### Lock file

Two servers writing the same state file would overwrite each other's position and block list. A lock file prevents this.

- The lock file's path is the state file's path with `.lock` appended (`items.txt.bqs.json.lock`). The server never writes to it, so it stays empty.
- At startup, before the state file is read, open the lock file, creating it if it does not exist, and try to take an exclusive advisory lock on it without waiting. Open it so that an existing file is never truncated (`OpenOptions` with `create(true)` and `truncate(false)`, not `File::create`).
- If the lock is held (`TryLockError::WouldBlock`), exit with a startup error that names the state file and says that a running server is using it. The message must also say that the lock cannot be left over from a crash, so that nobody deletes the lock file to get past it.
- Every other failure to open or lock the lock file is a startup error that names the lock file and shows the error of the operating system. Do not report it as "in use", and do not continue without the lock.
- Keep the lock for as long as the server exists, until after its last save. In the binary that is until the process ends; in a test that runs a server inside the test process, until that server is dropped. The kernel releases the lock when the process ends, however it ends, so a crash cannot leave a lock behind.
- Never delete the lock file, not at exit either: deleting it would let two servers lock two different files at the same path.
- The lock file is locked, not the state file, because every save replaces the state file with a new file.
- Limits, which the README must state: the lock belongs to the path as given, so two different paths to one state file (through a symlink) are not detected; and it is reliable only on a local filesystem.

Use `File::try_lock` from the standard library.

### Loading at startup

- Opening the file fails because it does not exist: position 0, wrap counter 0, no clients, and the view settings of the table above. Any other error from opening or reading it is a startup error. (Do not use a check like `Path::exists`, which also reports "does not exist" for a file that exists but cannot be read.)
- Otherwise the file must be a JSON object with these four fields: `version` equal to 1, `position`, `wraps`, and `clients`. Every client must have the fields shown above, except `host`; `user_agent` is `null` for a client without one. `host` is the reverse DNS name, or `null` for a client without one; a client without `host`, as written by an earlier version of the server, has no name. A loaded `host` is a name only if it has the form of a found name: one or more characters, each of them printable ASCII other than space. Any other string, also an empty one, loads as no name, so that the client is looked up again; this keeps the column widths of the dashboard right, which count one column per `char`. `view` is the only field of the file that may be absent; then the values of the table above apply. If `view` is present, it must have all four of its fields, and `sort_column` must be one of the listed names. `"view": null` is not accepted. A saved view whose sort column is hidden (`"sort_column": "host"` with `"show_host": false`) is valid and is used as saved. An empty file, damaged JSON, another `version`, a missing field (other than `view` and `host`), an unknown field at any level, or a value of the wrong type is a startup error. Take care with `user_agent`: with serde's derive, a missing `Option` field loads as `None` without an error, so it needs explicit handling to be required. Never silently restart at line 0 and never silently discard a block list.
- A `position` greater than the number of lines in the item file is a startup error.
- Loaded data is treated like live data: convert every IP key with `IpAddr::to_canonical` (two keys that become equal are a startup error), and pass every `user_agent` through the same cleaning as a live header.
- If `--state-file` names the item file itself, exit with an error before reading or writing anything.

There is no automatic conversion of a Node position file. The README explains how to create a state file from one (see "Deliverables").

### Saving

- Save by writing a temporary file in the same directory, calling `fsync` on it, renaming it over the state file, and calling `fsync` on the directory. Without these, a power loss can leave an empty state file, or the previous one, on some filesystems. If a save fails, remove its temporary file.
- Save once at startup (step 6 of "Startup order").
- After that, save every 30 seconds if anything in the saved data changed since the last save; immediately when a block or unblock happens; immediately at each wrap; and at every clean exit (see "Exiting"). A changed view setting and a found name are changes like any other: they wait for the next of these saves. Measure the 30 seconds with tokio's clock, not with the wall clock, which may jump.
- Every save is one consistent snapshot: the position, the wrap counter, the view settings, and every client record are from the same instant. Take the snapshot while holding the state mutex; write it to disk after releasing the mutex, so that a slow disk does not stall requests.
- Two saves must never interleave, and an older snapshot must never overwrite a newer one.
- Events and rates are not saved, and neither is a lookup that found no name.

The state is deliberately not saved on every request, because each save rewrites every client record. The consequence: after a crash, a kill, or a power loss, the server restarts from the last saved snapshot, which is normally at most about 30 seconds old, and the lines handed out after that snapshot are handed out a second time. This is accepted. No line is skipped because of it. A clean exit loses nothing.

## Exiting

Each of these is a clean exit: `q` confirmed with `y`, `Ctrl-C` in the dashboard, and the signals `SIGINT`, `SIGTERM`, and `SIGHUP` (sent when the terminal window is closed). A clean exit does these steps in this order:

1. Stop handing out lines. From this moment no batch is taken; requests still arriving may be left unanswered.
2. Save the state file. This snapshot is therefore the final state.
3. Set the terminal title to its `stopped` form (see "Terminal title") and restore the terminal. After `SIGHUP` the terminal is already gone, so both can fail; ignore those failures. They must not cause a panic, and they come after the save so that they cannot prevent the save. Check how the restore function you use behaves when the terminal is gone.
4. Exit with status 0, or with status 1 if the save failed.

## Failures while running

If saving the state file fails, or reading the item file fails, the server must stop: stop handing out lines; save the state file, unless a failed save is the reason for stopping; set the `stopped` title and restore the terminal; print the error to stderr; and exit with status 1. A server that kept handing out lines while its saves fail would hand out an unbounded number of them again after a restart. A request whose read of the item file failed gets no response.

A panic anywhere must end the whole process, never leave a half-working server. Set `panic = "abort"` in the Cargo profiles, and make sure the panic hook first sets the `stopped` title and restores the terminal (including mouse capture).

## Dashboard

### When it runs

If both stdin and stdout are terminals, and the server is in the foreground process group of the terminal at stdin, the server draws the dashboard. (The kernel stops a background job that switches its terminal to the dashboard.) Otherwise it runs without one and writes one log line per event to stderr with `tracing`. In dashboard mode, nothing but the dashboard may write to the terminal, so install a `tracing` output only if stderr is not a terminal (for example, redirected to a file).

### Layout

From top to bottom. The numbers are examples, from a server started with `--cycle`.

```
items.txt :31417  1,234,567/9,876,536 12.5%  ETA 14d  412 items/min  38 req/min  wrap 3  3 IPs, 1 blocked

         IP            ITEMS▼   FIRST   LAST   REQS  HOST
allowed  203.0.113.7   12,400      3d     2s    124  crawler-7.example.net
blocked  198.51.100.9     300      1h     0s  1,215  -
allowed  2001:db8::17       0      5m     5m      1  scanner.example.org
12:01:02 203.0.113.7   100
12:01:04 198.51.100.9  blocked
F2 UA  F3 Host  F5 Pause  F6 Sort  F7 Invert  F9 Block  F10 Quit
```

1. **Status line** (1 row), then a blank row. Its parts come in this order, most important first, because a narrow window cuts off the right end:
   - item file name without its directory
   - port
   - position `/` total lines
   - percent with one decimal, or `-` when the item file has no lines
   - `ETA` and the estimated time until the end of the current pass: the remaining lines divided by the 60 second rate of lines handed out, shown in the same form as an age (see "Clients table"), or as `-` when that rate is 0. When cycling is off and the position equals the total number of lines, show `DONE` in place of `ETA` and the estimate.
   - lines handed out in the last 60 seconds
   - requests in the last 60 seconds
   - wrap counter (omit when cycling is off)
   - number of known client IPs, and how many of them are blocked
2. **Clients table**: one header row, then one row per client.
3. **Events pane**: the most recent events, newest at the bottom, as many as fit.
4. **Key bar** (1 row). As in `htop`, the key names are in the terminal's colors and the rest of the row is black on cyan.

In the status line, values are bold and the words between them are dim (not dark gray, which some palettes make the background color), so that the values stand out; a count of blocked IPs above 0 is red. While the server is paused, the status line starts with `PAUSED` in a standout style (for example reverse video), and the key bar shows `F5 Resume` in place of `F5 Pause`.

Let R be the number of rows between the blank row and the key bar. The events pane gets R / 3 rows, rounded down; the clients table, including its header row, gets the rest.

No borders and no titles; they cost rows and columns. Numbers of items and requests use `,` as thousands separator. The dashboard must draw without panicking at every terminal size, down to 1×1, and with an empty item file and with no clients. In a window that is too small, parts are cut off; which parts is up to you.

### Clients table

Columns, left to right:

| Header | Content | Sorts by | Direction when first chosen |
|---|---|---|---|
| (blank) | `allowed` for an unblocked IP, `blocked` for a blocked IP | blocked or not | blocked first |
| `IP` | the address | address, numerically (IPv4 before IPv6), not as a string | ascending |
| `ITEMS` | `items` | number | largest first |
| `FIRST` | age of `first_seen` | time | most recent first |
| `LAST` | age of `last_seen` | time | most recent first |
| `REQS` | `requests` | number | largest first |
| `HOST` | the reverse DNS name; `-` when the lookup found none; empty while the client has not been looked up | the whole name (not the shortened form on screen), ignoring case, compared label by label from the last label to the first: `host.example.com` compares as the list `com`, `example`, `host`. So clients of one domain are adjacent. Compare lists, not joined strings, because `-` sorts before `.`. Clients without a name are equal to each other and come after all clients with a name, in both directions. | ascending |
| `UA` | `user_agent`, or `-` | not sortable | |

- At startup, the `HOST` column is shown unless the saved view settings say otherwise. It can be hidden and shown again with a key. Its width is that of the longest name in it, but at least that of its header with the sort arrow. If the rest of the screen width is less, the column gets only that rest, and the `UA` column gets nothing. A name longer than the column is shown as `…` followed by its end, because the domain is at the end.
- At startup, the `UA` column is hidden unless the saved view settings say otherwise. It can be shown and hidden again with a key. When shown, it takes the remaining width and is truncated.
- A hidden column cannot become the sort column. If the sort column gets hidden, the rows stay sorted by it in the same direction, and no header carries `▼` or `▲`. `F6` then moves on to the next shown sortable column to the right of it, wrapping around.
- Ages show only their largest unit: `42s`, `17m`, `5h` (below 48 hours), `12d`.
- Column widths adapt to their content. Columns that do not fit in the window are cut off on the right.
- Rows of blocked IPs are drawn in a clearly different style (for example red), so that the block state is visible without reading the first column.
- The sort column and direction at startup come from the saved view settings; without them the sort is `ITEMS`, largest first. The header of the sort column carries `▼` when the largest value is first (for the time columns: the most recent; for the first column: blocked) and `▲` for the opposite order. So `IP` ascending and `HOST` ascending carry `▲`. As in `htop`, the header row is black on green across the whole width, and the header of the sort column is black on cyan across the width of its column.
- Making a column the sort column, by click or by key, always starts with its "direction when first chosen", also when it has been the sort column before.
- Rows that compare equal are ordered by IP address ascending, so that rows never jitter.
- One row is selected and highlighted, and the table scrolls to keep it visible. As in `htop`, the selected row is black on cyan across the whole width; if its client is blocked, it is black on red, so that the block state stays visible. Draw it as reverse video of cyan (or red) on black, so that it stays in reverse video when colors are turned off (`NO_COLOR`). With no clients, nothing is selected. When nothing is selected and a client exists, the first row becomes selected.

**The selection is an IP address, not a row number.** When the rows re-sort, the same IP stays selected. Blocking always acts on the selected IP.

**A mouse click never blocks a client that was not already selected.** Rows re-sort several times per second, faster than a hand moves, so the row under the pointer can change just before a click lands. Therefore a click on any part of a client row selects that client, and only a click on `allowed` or `blocked` in the row of the client that is already selected blocks or unblocks it. The operator has then seen the highlight on that client before the second click.

A mouse click is resolved against what the most recently drawn frame showed at the clicked cell: record, while drawing, which IP each screen row holds and where each column and key-bar label is, and resolve clicks against that record. Do not recompute the layout from the current state when a click arrives. Use one description of the column geometry for both drawing and click resolution.

### Terminal title

While the dashboard runs, set the title of the terminal window, so that a list of windows shows the state of every instance. The title is the first of these that applies:

| Situation | Title |
|---|---|
| Cycling is off and the position equals the total number of lines | `DONE items.txt` |
| The server is paused | `PAUSED 12.5% items.txt` |
| Otherwise | `12.5% items.txt` |

The percent is the one from the status line. Write the title only when its text changes. For an item file without lines the percent is `-`, as in the status line, so with cycling on the title is `- items.txt`, or `PAUSED - items.txt` while paused. (With cycling off, the position 0 equals the total 0, so the first row of the table applies and the title is `DONE items.txt`.) At every exit that restores the terminal (a clean exit, a failure while running, or a panic), set the title to `stopped items.txt`.

The item file name comes from the command line and is written to the terminal, in the title and in the status line. Clean it first in the same way as a `User-Agent` value.

### Events pane

Each row: local time as `HH:MM:SS`, client IP, outcome. The outcome is the number of lines handed out, or `blocked`, or `wrong path`, or `paused`, or `rejected` followed by the status code.

### Keys and mouse

| Input | Action |
|---|---|
| `Up`, `Down`, `PageUp`, `PageDown`, `Home`, `End` | Move the selection. |
| Mouse wheel over the clients table | Move the selection by 3 rows. |
| Left click on a client row | Select that client. |
| Left click on `allowed` or `blocked` in the row of the selected client | Block or unblock that client. |
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

Every function key has a letter alternative because terminal emulators often keep some function keys for themselves. The keys that move the selection react to key presses and key repeats. Every other key reacts to key presses only, so that a held key does not block and unblock, or pause and resume, over and over. No key reacts to a key release. React only to the press of the left mouse button and to the wheel; ignore drags, releases, and movement.

Quitting the dashboard stops the server; that is why `q` asks first. Every way of quitting is a clean exit (see "Exiting").

Redraw after every input event, and at least 4 times per second. Wait for input with a timeout, so that neither a redraw nor an exit has to wait for a key.

## Implementation guidance

The operator's other Rust projects use edition 2024, `anyhow`, `clap`, `tokio`, `axum`, `serde`, `jiff`, and `tracing`; use the same here. Versions current on 2026-11-24: `ratatui` 0.30, `crossterm` 0.29, `axum` 0.8, `tokio` 1, `jiff` 1 (its `serde` feature is needed for the timestamps in the state file). `ratatui` 0.30 is split into several crates and its API has changed between releases, so read the docs.rs pages for the exact versions you pin instead of relying on memory. Take `crossterm` through the version that `ratatui` uses, so that there is only one copy.

For reverse DNS, use `hickory-resolver` (0.26 is current; it needs its `tokio` and `system-config` features). Verified on docs.rs for 0.26.4: `Resolver::builder_tokio()` returns a builder that takes the system configuration, building can fail, and `Resolver::reverse_lookup` exists; check their exact signatures there. Make the lookup function replaceable, so that tests supply their own and never send a DNS query.

Verified on docs.rs: `ratatui::init()` enables raw mode and the alternate screen and installs a panic hook that restores the terminal. It does not enable mouse capture; enable that yourself and disable it on every exit path, including panics. `axum::extract::ConnectInfo` works only when the server is started with connection info, for example through `Router::into_make_service_with_connect_info`.

Suggested shape (adjust if something simpler emerges):

- `queue.rs`: the item file reader and the in-memory position. No knowledge of HTTP, clients, or the state file.
- `state.rs`: clients, events, rates, the saved view settings, and loading and saving the state file.
- `http.rs`: the one handler that serves every request.
- `tui.rs`: drawing as a function of (state, selection, current time); input handling as a function from an input event to an action.
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

The repository's `AGENTS.md` applies. Use `assert!` and its relatives for invariants, `tracing` for logs, the built-in test harness for unit tests, and `proptest` for property-based tests. The local `cargo` wrapper supports only nightly Rust.

## Tests

Required, at minimum:

**Queue**

- Batches in file order; a short batch at the end of the file; a batch that takes exactly the last line, followed by a request that gets an empty batch and wraps; no wrap when cycling is off, which is the default; a last line without `\n`; empty lines; an empty file; bytes that are not valid UTF-8.
- With cycling on: an item file replaced during a pass (by renaming a new file over it) is served from the next pass on.
- Restart: stop after some batches, open a new queue at the position that the old one reported, continue with the next line.
- A starting position greater than the line count: error.
- Property test, with cycling on: for arbitrary file bytes and an arbitrary sequence of batch sizes, with a simulated restart (as above) at arbitrary points, the concatenation of all batches taken before the first wrap is a prefix of the file's lines, each followed by `\n`; if a wrap happened, it is all of them.

**HTTP** (drive the router directly and supply the peer address yourself, so that tests can act as several client IPs; use tokio's paused time so that the 1 second delays cost nothing)

- Each row of the HTTP contract table, including that the non-`200` rows and `HEAD` consume no lines.
- Precedence when several rows apply: a blocked client on a wrong path, a wrong path with a method other than `GET`, and an invalid `max` while paused each get the reply of the first matching row.
- With `--path /secret`: `/secret` and `/secret?max=2` are served; `/`, `/secret/`, `/SECRET`, and `/%73ecret` get `403` after 1 second and consume no lines. A `--path` value without a leading `/`, or containing `?`, is an argument error.
- Each kind of invalid `max` listed above, and lowering to the `--max-batch` value.
- A blocked IP gets `403` while another IP is served normally; unblocking restores service.
- While paused: a valid request gets an empty `200` after 1 second and consumes no lines; a blocked IP still gets `403`. After resuming, the next request gets the next line.
- Many concurrent requests: every line of the file is handed out exactly once.
- Counters: `items`, `requests`, `first_seen`, `last_seen`, `user_agent` after a mix of outcomes.
- A `User-Agent` containing a tab, the control character U+009B, and bytes that are not valid UTF-8 is stored without control characters. The HTTP library itself rejects most ASCII control bytes in a header, so also test the cleaning function directly with an escape sequence.
- One test through a real TCP listener on `127.0.0.1` with port 0: a `GET` is served and attributed to `127.0.0.1`, and a `HEAD` gets `405`. This proves the wiring of the peer address, which the other tests bypass.

**Reverse DNS** (with a lookup function supplied by the test, and tokio's paused time)

- Many requests from one IP cause one lookup. A client loaded from the state file is not looked up at startup; it is looked up when it makes its first request in this run.
- A request is answered while the lookup for its client has not finished.
- Without a dashboard, no lookup is made.
- A found name is in the state file after the next save. A server started from that file shows the name before the client's first request and doesn't look the client up again. A client whose lookup found no name is saved with `"host": null`, and the next run looks it up again.
- A loaded `host` is a name only if it has the form of a found name; a client without `host`, or with an empty one, or with one that has a space, a control character, or a character outside ASCII, loads without a name.
- With more than 8 new clients at once, at most 8 lookups run at the same time, and all of them finish eventually.
- A lookup that never answers is given up 5 seconds after it started running, and the client then counts as looked up without a name. A ninth lookup that waited longer than 5 seconds for its turn still runs and can still get its name.
- An answer with a `CNAME` record before the `PTR` record gives the `PTR` target. A lookup function that reports "resolver not available" makes every client count as looked up without a name, and the server keeps serving.
- A name with a final `.`, with control characters, or longer than 255 `char`s is stored without the `.`, with each control character escaped, and cut to its last 255 `char`s.

**State file** (every test uses an item file in its own fresh temporary directory, because the default state file is written next to the item file)

- Save and load round trip, including the position, the wrap counter, and view settings that all differ from the defaults. Missing file: position 0, no clients, and the default view settings. A file without `view`: the default view settings.
- Startup errors: an empty file; damaged JSON; a wrong `version`; a missing field; a `view` with an unknown `sort_column`, with `"sort_column": "ua"`, without one of its four fields, or with a field of the wrong type; `"view": null`; an unknown field; a client without `user_agent`; a file holding only digits (a Node position file); a directory at the state file path; a `position` greater than the line count; `--state-file` naming the item file; a state file path whose parent directory does not exist (the lock file cannot be created; do not test with a read-only directory, which is still writable for root); a lock file path that names the item file.
- A startup that fails leaves an existing state file unchanged.
- While one server runs, a second server with the same state file and a different `--port` exits with the "in use" startup error, without reading the item file and without changing the state file; the first server keeps serving. After the first server has ended, a new server starts normally although the lock file still exists. Test both endings: a clean exit, and a child process killed with `SIGKILL`.
- A lock file that already has content is not truncated.
- Through the whole server: hand out some lines, exit cleanly, start again with the same state file, and the next request continues with the next line; with `--cycle` and at least one wrap, the wrap counter is kept too. Without `--cycle`, a request after the last line gets an empty batch and the position stays at the end.
- With paused time: after a hand-out, once 30 seconds have passed and the save that started then has finished, the file on disk holds the new position, without an exit. The file is not rewritten between two such saves, and not at all while nothing in the saved data changes. A change of a view setting alone, with no request, is on disk after the next 30 second save. After a block, an unblock, or a wrap, the file on disk holds the change without waiting for the 30 seconds.
- A server without a dashboard that loads non-default view settings writes the same view settings in its saves.
- A start from a snapshot that is older than the last hand-outs repeats those lines and skips none.
- Without `--state-file`, the state file for `items.txt` is `items.txt.bqs.json` (append to the whole path; `Path::with_extension` would give `items.bqs.json`). A second positional argument is an argument error.

**Dashboard** (with ratatui's `TestBackend`)

- Clicking a header sorts by that column; clicking it again inverts; `F6`, `F7` do the same by keyboard.
- Clicking `allowed` in a row that is not selected only selects that client. Clicking `allowed` in the selected row blocks exactly that IP, also when new data arrived after the frame was drawn and would re-sort the rows.
- The selection stays on the same IP when the rows re-sort.
- A dashboard started from saved view settings shows that sort column, that direction, and those columns, with the rows in that order, also when the saved sort column is hidden; changing them changes what the next save writes.
- `F2` shows and hides the `UA` column; `F3` shows and hides the `HOST` column.
- Sorting by `HOST` puts `b.example.com` next to `a.example.com` and before `a.example.net`, keeps `a.foo-bar.com` and `b.foo-bar.com` adjacent although `c.foo.com` exists, and puts clients without a name last in both directions. The `HOST` cell is `-` for a client looked up without a name and empty for a client not looked up. A name of 72 characters is shown whole on a wide screen, with the `UA` column after it in every row; on a narrower screen it shows its end.
- `F5` pauses and resumes; while paused, the status line starts with `PAUSED` and the key bar shows `F5 Resume`.
- The status line shows the number of client IPs and of blocked ones, and shows `DONE` when cycling is off and the position is at the end of the file.
- The title text for each row of the "Terminal title" table, and for an item file without lines.
- Drawing works with an empty item file and with no clients.
- Property test: drawing never panics for arbitrary terminal sizes from 1×1 to 300×100 and arbitrary client tables.

**One-time checks, not kept in the repository**

- Before deleting the Node version: give each version its own copy of one item file that is valid UTF-8, start both at the same position, send both the same sequence of valid requests, and confirm byte-identical bodies. After a clean exit, the position in the Rust version's state file must equal the number in the Node version's position file. Do this once with cycling on in both versions (the Node version's default; `--cycle` for the Rust version), including a wrap, and once with cycling off in both (`CYCLE=false` for the Node version), including requests after the end of the file. Then stop a Node run in the middle of a pass, follow the README's instructions for moving from the Node version with that run's position file, and confirm that the Rust version continues with the next line.
- Run the real binary in a detached `tmux` session, drive it with `tmux send-keys`, send requests with `curl`, and read the screen with `tmux capture-pane -p`. Confirm that the dashboard looks like the layout above and that keys work. Confirm that the terminal is restored after quitting, after `SIGTERM`, and after a forced panic. Check the default end state too: without `--cycle`, after the last line the status line and the title show `DONE`. Confirm that the state file is saved when the `tmux` session is killed while the server runs (this sends `SIGHUP`): kill it a few seconds after a hand-out, well inside the 30 second interval, and check that the saved `position` equals the number of lines handed out.

## Deliverables

1. The Rust crate at the repository root, with its `Cargo.lock` committed, building without warnings and passing `cargo clippy` and `cargo test`.
2. `README.md`: what the server does, how to run it, the HTTP contract, the state file, its lock file (it is permanent and empty, it is never stale, and it must not be deleted while a server runs) with the limits named in "Lock file", the dashboard keys, that reverse DNS names are not verified, that the lookups send client IPs to the system's DNS resolver, and that the `--path` value is visible to other users of the machine in the process list. Include a section "Moving from the Node version": replace the environment variable `PORT` with `--port`; add `--cycle` unless the old launch set `CYCLE=false`, because cycling is now off by default; remove `CYCLE`; remove the second positional argument; to continue a queue that is in progress, stop the Node version and create the state file from the number in the old position file, for example `echo '{"version":1,"position":'"$(cat OLD_POSITION_FILE)"',"wraps":0,"clients":{}}' > ITEM_FILE.bqs.json`. The old position file stays untouched, so going back to the Node version remains possible. Also say that the position is changed by editing `position` in the state file while the server is stopped.
3. Removal of `index.js`, `readlines.js`, `package.json`, and `LICENSE`. That `LICENSE` file is the license of the vendored `readlines.js` only. Do not add a license for the Rust crate; the operator has not chosen one.
4. `.gitignore` with `target/`.

Suggested commit sequence, one logical change each:

1. The Rust server without client tracking or dashboard, at parity with the Node version plus the listed fixes, with `--path`, with the state file and its lock file (`clients` always `{}` at this step, so that the required fields do not change later; `view` is added in step 3), and with the tests that apply at this step.
2. Client tracking, blocking and pausing in the HTTP handler, and the clients in the state file.
3. The dashboard, including the pause key, the terminal title, and the saved view settings, with an empty `HOST` column.
4. Reverse DNS lookups.
5. `README.md` and removal of the Node files.

## Not included

Do not build these; mention them in your final report if you see a strong reason for one:

- Re-queueing the lines that a blocked IP took.
- Blocking an IP that has never made a request (the operator can edit the state file while the server is stopped).
- Collapsing repeated events from one IP into one row.
- Filtering or searching the clients table.
- Removing old client records.
- Appending to the item file while the server runs.
- Verifying a reverse DNS name with a forward lookup, and saving names across restarts.
