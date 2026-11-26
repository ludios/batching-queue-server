# batching-queue-server

The server hands out the lines of one text file to HTTP clients in file order, one batch per request. It remembers its position across restarts and counts the lines that it hands to each client IP address. A compact terminal dashboard shows the queue and its clients, and lets the operator block and unblock IP addresses.

## Building

```
cargo build --release
```

The binary is `target/release/batching-queue-server`.

## Running

```
batching-queue-server [OPTIONS] ITEM_FILE
```

| Option | Default | Meaning |
|---|---|---|
| `--state-file FILE` | the `ITEM_FILE` path with `.bqs.json` appended | The path of the state file (see "The state file"). For `/data/items.txt` the default is `/data/items.txt.bqs.json`. |
| `--port PORT` | `31417` | The TCP port that the server listens on, at every IPv4 and IPv6 address. The system must support IPv6. |
| `--cycle` | off | After handing out the last line of the file, the server starts again at its first line. Without `--cycle`, the server stops handing out lines after the last one. |
| `--max-batch N` | `10000` | The maximum number of lines in one response. Must be at least 1. |
| `--path PATH` | `/` | The only path at which the server hands out lines; every other path gets `403`. It must start with `/` and consist of `/`, `A` to `Z`, `a` to `z`, `0` to `9`, `-`, `_`, `.`, and `~`, and it must not have a segment that is `.` or `..`. |

For example:

```
batching-queue-server --cycle --path /k3v9x2m7q4w8 /data/items.txt
```

A client then gets up to 100 lines with `curl 'http://SERVER:31417/k3v9x2m7q4w8?max=100'`.

A long random `--path` value keeps out strangers, who don't know it. Other users of the same machine can see it in the server's command line, with `ps`. Anyone who can watch the network traffic can see it too, because requests are plain HTTP.

At startup, the server prints one line to stderr that names the state file and gives the position it starts at. Then, if stdin and stdout are terminals and the server runs in the foreground, it shows the dashboard (see "The dashboard"). The server writes one line to stderr for each request if it runs without the dashboard, or if stderr is not a terminal (for example, redirected to a file).

Two servers that serve the same item file need different state files, so give at least one of them `--state-file`. A server refuses to start while another running server uses the same state file path (see "The lock file").

## The item file

- A line is a sequence of bytes, possibly empty, ended by `\n`. If the file has bytes after its last `\n`, they are a line too. A `\r` before the `\n` is part of the line.
- Each line is sent exactly as it is in the file, whatever its bytes, followed by `\n`.
- Do not change the item file in place while the server runs. To replace it, rename a new file over it. With `--cycle`, the server opens the path again at each wrap, so it serves the new file from the next pass on. A server that doesn't cycle, or whose old file is empty and so never wraps, serves the new file only after a restart, which applies the saved position to it (see "The state file").
- At startup and at each wrap, the server reads the whole file once to count its lines. For a very large file, this takes seconds, and the server doesn't serve any request in that time.

## HTTP contract

For each request, the first row that applies decides the reply:

| Condition | Status | Body | Delay | Lines handed out |
|---|---|---|---|---|
| The client IP address is blocked | `403` | empty | 1 second | none |
| The path is not exactly the `--path` value | `403` | empty | 1 second | none |
| The method is not `GET` (`HEAD` gets `405` too) | `405`, with `Allow: GET` | empty | none | none |
| The `max` query parameter is invalid | `400` | one line saying what is wrong | none | none |
| The server is paused | `200` | empty | 1 second | none |
| There is at least one line to hand out | `200` | each line followed by `\n` | none | up to `max` |
| There is no line to hand out | `200` | empty | 1 second | none |

- Every `200` and `400` reply has the header `Content-Type: text/plain`.
- `max` is the most lines that the client wants, and it defaults to 1. After URL-decoding, it must consist of the digits `0` to `9` only, and its value must be from 1 to 18446744073709551615. A value above `--max-batch` is lowered to `--max-batch`. A repeated `max` is invalid. Other query parameters are ignored.
- The path is compared with `--path` byte for byte, as received: without URL-decoding, without case folding, and without special treatment of a final `/` (`/secret/` is not `/secret`).
- A wrong path gets exactly the reply that a blocked client gets, so that a stranger cannot tell the two apart.
- The client's IP address is the remote address of the TCP connection, and an IPv4-mapped IPv6 address (`::ffff:203.0.113.7`) counts as the IPv4 address (`203.0.113.7`). Headers such as `X-Forwarded-For` are ignored.
- With `--cycle`, a request whose `max`, lowered to `--max-batch`, is more than the number of lines left gets the lines that are left, possibly none (then as an empty `200` after 1 second), and the next line handed out is the first line. So with `--cycle`, an empty reply doesn't mean that the queue is done.
- The server counts a line as handed out when it reads the line for a response, not when the client has received it. A client that disconnects early loses its lines; with `--cycle`, they are handed out again in the next pass.

## The state file

The state file holds everything that survives a restart: the position, the wrap counter, the client records, and the view settings of the dashboard.

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

- `position` is the number of lines already handed out in the current pass over the item file, which is also the 0-based index of the next line to hand out.
- `wraps` counts how many times the server has started again at the first line.
- `clients` has one record for each IP address that has ever made a request, whatever the reply was. `items` is the number of lines handed to that IP address. `user_agent` is the `User-Agent` header of its most recent request that had this header, or `null`. `host` is the reverse DNS name that a lookup found, or `null`; set it to `null` to have the client looked up again at its first request in the next run.
- `view` holds the dashboard's sort column (one of `blocked`, `ip`, `items`, `first_seen`, `last_seen`, `requests`, `host`), whether the sort direction is inverted, and whether the `HOST` and `UA` columns are shown. If `view` is left out, the values shown above apply.

If the state file does not exist, the server starts at position 0 and creates it. So if a restarted server reports that it created the state file, check the path: a mistyped `--state-file` makes the server start again at line 0 without its block list, and so does moving or renaming the item file, which changes the default state file path. The server refuses to start if a state file exists but cannot be read, if it is not exactly in this form, or if its `position` is greater than the number of lines in the item file.

Every 30 seconds, the server saves the state file if anything in it has changed. It also saves at once after a block, an unblock, or a wrap, and at every clean exit (see "Exiting"). A save never leaves a partly written state file. After a crash, a `kill -9`, or a power loss, the server starts again from the last save, so the lines handed out after that save are handed out a second time; normally, those are at most about 30 seconds' worth of lines. No line is skipped.

The pause, the events, and the rates are not saved.

To change the state, stop the server, edit the state file, and start the server again. The server applies the saved position to whatever item file is at the path when it starts, so to serve a new item file from its first line, set `position` to 0. To block an IP address that has never made a request, add a record for it with `"blocked": true`; every record needs all of the fields shown above except `host`.

### The lock file

The server locks a file whose path is the state file's path with `.lock` appended (`items.txt.bqs.json.lock`), so that a second server with the same state file path refuses to start.

- The lock file is permanent. The server creates it, empty, if it is missing, and never deletes it.
- The lock is never stale: the operating system releases it when the server's process ends, even by a crash or a `kill -9`. So an error that the state file is in use always means that a running process holds the lock.
- Do not delete the lock file while a server runs: a second server could then start, and the two would overwrite each other's state file.
- A symlink or a hard link to the state file under another name gets a different lock file, so the lock doesn't stop two servers that use the two names, and both hand out the same lines.
- The lock is reliable only on a local filesystem.

## The dashboard

```
items.txt                                                                                          :31417
         IP            ITEMS▼   FIRST   LAST   REQS           HOST
allowed  203.0.113.7   12,400      3d     2s    124  crawler-7.example.net
blocked  198.51.100.9     300      1h     0s  1,215                      -
allowed  2001:db8::17       0      5m     5m      1    scanner.example.org
   12.5%  1,234,567 / 9,876,536   ETA 14d   412 items/min   38 req/min   wrap 3   3 IPs    1 blocked
12:01:02 203.0.113.7   100
12:01:04 198.51.100.9  blocked
F2UA    F3Host  F5Pause F6Sort  F7InvertF9Block F10Quit
```

From top to bottom:

1. The name line: the item file name, and the port at the right end.
2. The clients table, one row per client IP address. `FIRST` and `LAST` are the times since the client's first and most recent request. `HOST` is the reverse DNS name (the `PTR` record) of the client IP address, as a hint about who the client is. A client without a name is looked up at its first request in each run of the server; its cell is empty until that lookup has finished, and `-` if the lookup didn't find a name. `UA` is the most recent `User-Agent`; this column is hidden unless the operator has chosen to show it, and it gets only the room that the other columns leave. Rows of blocked IP addresses are drawn in red. In the rows that are not selected, the word `allowed` is drawn in gray, so that a `blocked` stands out. The selected row has a gray background, or is black on red if its client is blocked.
3. The progress bar. The part of it at the left end that is blue grows as lines are handed out; the rest is dark gray. Its text: the percent done, the position, and the number of lines in the file; the estimated time until the end of the pass, at the rate of the last minute (or `DONE` when cycling is off and every line has been handed out); the lines handed out and the requests in the last minute; the wrap counter (with `--cycle` only); and the number of client IP addresses and how many of them are blocked. While the server is paused, the text starts with `PAUSED` and the blue part is dark yellow instead. When cycling is off and every line has been handed out, the whole bar is green, also while the server is paused.
4. The events pane: the most recent requests, newest at the bottom, each with its time, its client, and its outcome (the number of lines handed out, `blocked`, `wrong path`, `paused`, or `rejected` with the status code). A `blocked` row is red, and a `wrong path`, `paused`, or `rejected` row is yellow.
5. The key bar.

The terminal window's title shows the state too: `12.5% items.txt`, `PAUSED 12.5% items.txt`, `DONE items.txt`, and `stopped items.txt` after the server has exited (but not after a `kill -9`).

| Key or mouse | Action |
|---|---|
| `Up`, `Down`, `PageUp`, `PageDown`, `Home`, `End` | Move the selection. |
| Mouse wheel over the clients table | Move the selection by 3 rows. |
| Left click on a client row | Select that client. |
| Left click on `allowed` or `blocked` in the selected row | Block or unblock that client. |
| `F9` or `b` | Block or unblock the selected client. |
| Left click on a column header | Sort by that column. A click on the header of the sort column inverts the direction. A click on `UA` does nothing. |
| `F6` or `s` | Sort by the next shown column to the right, skipping `UA` and wrapping around. |
| `F7` or `i` | Invert the sort direction. |
| `F2` or `u` | Show or hide the `UA` column. |
| `F3` or `h` | Show or hide the `HOST` column. |
| `F5` or `p` | Pause or resume handing out lines. A paused server answers as if it didn't have any lines left, so clients just wait. |
| `F10` or `q` | Ask whether to quit: `y` or `Ctrl-C` quits, and any other key cancels. |
| `Ctrl-C` | Quit at once. |
| Left click on a key name in the key bar, or on the 6 columns after it that hold its label | Same as that key. |

A click on `allowed` or `blocked` in a row that is not selected only selects that client. The rows re-sort several times per second, so another client may have moved under the pointer just before the click.

Quitting the dashboard stops the server. The state file keeps the sort column, the sort direction, and whether `HOST` and `UA` are shown.

The reverse DNS names are not verified: the owner of an IP address can publish any name for it. The server looks names up only while it shows the dashboard (also while the `HOST` column is hidden), and the lookups send client IP addresses to the system's DNS resolver (`/etc/resolv.conf`). The section "Reverse DNS names" of SPEC.md describes the lookups in detail.

## Exiting

These are clean exits: `q` followed by `y`, `Ctrl-C` in the dashboard, and the signals `SIGINT`, `SIGTERM`, and `SIGHUP` (sent when the terminal window is closed). At a clean exit, the server stops handing out lines, saves the state file, restores the terminal, and exits with status 0, or with status 1 if the save failed. Before the server has started (for example, during a long scan of the item file), these signals end it at once, which loses nothing.

If saving the state file or reading the item file fails while the server runs, the server stops handing out lines, saves the state file (unless saving is what failed), prints the error to stderr, and exits with status 1.
