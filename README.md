# batching-queue-server

Hands out the lines of one text file to HTTP clients, in file order, a batch of lines per request. It remembers how far it got across restarts, counts what each client IP address took, lets the operator block and unblock IP addresses, and shows all of this in a compact terminal dashboard.

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
| `--state-file FILE` | the `ITEM_FILE` path with `.bqs.json` appended | Where the state file is (see "The state file"). For `/data/items.txt` the default is `/data/items.txt.bqs.json`. |
| `--port PORT` | `31417` | The TCP port to listen on, on every IPv4 and IPv6 address. The system must support IPv6. |
| `--cycle` | off | After the last line of the file is handed out, continue with its first line again. Without `--cycle`, the server doesn't hand out any more lines after the last one. |
| `--max-batch N` | `10000` | The most lines in one response. Must be at least 1. |
| `--path PATH` | `/` | The only request path at which lines are handed out; every other path gets `403`. It must start with `/` and consist of `/`, `A`-`Z`, `a`-`z`, `0`-`9`, `-`, `_`, `.`, and `~`, without a segment that is `.` or `..`. |

For example:

```
batching-queue-server --cycle --path /k3v9x2m7q4w8 /data/items.txt
```

A client then gets up to 100 lines with `curl 'http://SERVER:31417/k3v9x2m7q4w8?max=100'`.

A long random `--path` value keeps out strangers, who do not know it. It does not keep out other users of the same machine: a command line, and so the `--path` value, is visible to them in the process list (`ps`). Nor does it keep out anyone who can watch the network traffic, because requests are plain HTTP.

At startup, the server prints one line to stderr that names the state file and says the position it starts at. Then, if stdin and stdout are terminals and the server runs in the foreground, it shows the dashboard (see "The dashboard"). Without the dashboard, and also with it if stderr is not a terminal (for example, redirected to a file), the server writes one line to stderr for each request.

Two servers that serve the same item file need different state files, so give at least one of them `--state-file`. A server refuses to start while another running server uses the same state file path (see "The lock file").

## The item file

- A line is a sequence of bytes ended by `\n`. A last line without a final `\n` is also a line. Empty lines are lines too, and a `\r` before the `\n` is part of the line.
- Each line is sent exactly as it is in the file, whatever its bytes, followed by `\n`.
- Do not change the item file in place while the server runs. To replace it, rename a new file over it: the server opens the path again at each wrap, so with `--cycle` the new file is served from the next pass on. (A server whose item file doesn't have any lines never wraps; restart it.)
- At startup and at each wrap, the server reads the whole file once to count its lines. For a very large file, this takes seconds, during which the server doesn't serve any request.

## HTTP contract

For each request, the first row that applies decides the reply:

| Condition | Status | Body | Delay | Lines handed out |
|---|---|---|---|---|
| The client IP address is blocked | `403` | empty | 1 second | none |
| The path is not exactly the `--path` value | `403` | empty | 1 second | none |
| The method is anything but `GET`, `HEAD` too | `405`, with `Allow: GET` | empty | none | none |
| The `max` query parameter is invalid | `400` | one line saying what is wrong | none | none |
| The server is paused | `200` | empty | 1 second | none |
| There is at least one line to hand out | `200` | each line followed by `\n` | none | up to `max` |
| There is no line to hand out | `200` | empty | 1 second | none |

- Every `200` and `400` reply has the header `Content-Type: text/plain`.
- `max` is the most lines the client wants; without it, the client gets at most 1 line. After URL-decoding, it must consist of the digits `0` to `9` only and be from 1 to 18446744073709551615. A value above `--max-batch` is lowered to `--max-batch`. A repeated `max` is invalid. Other query parameters are ignored.
- The path is compared with `--path` byte for byte, as received: no URL-decoding, no case folding, and `/secret/` is not `/secret`.
- A wrong path gets exactly the reply that a blocked client gets, so that a stranger cannot tell the two apart.
- The client's IP address is the address of the TCP connection, with an IPv4-mapped IPv6 address (`::ffff:203.0.113.7`) taken as the IPv4 address. Headers such as `X-Forwarded-For` are not read.
- With `--cycle`, the request that reaches the end of the file gets the lines that were left, possibly none (then as an empty `200` after 1 second), and the next request gets the first line. So with `--cycle`, an empty reply doesn't mean that the queue is done.
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
- `clients` has one record for each IP address that has ever made a request, whatever the reply was. `items` is the number of lines handed to that IP address, and `user_agent` is the `User-Agent` of its most recent request that had one, or `null`. `host` is the reverse DNS name that a lookup found, or `null`; set it to `null` to have the client looked up again.
- `view` holds the dashboard's sort column (one of `blocked`, `ip`, `items`, `first_seen`, `last_seen`, `requests`, `host`), whether the sort direction is inverted, and whether the `HOST` and `UA` columns are shown. It may be left out, and then the values shown above apply.

If the state file does not exist, the server starts at position 0 and creates it. So check the path if a restarted server reports that it created the state file: a mistyped `--state-file`, or an item file that was moved or renamed (which moves the default state file path), makes the server start again at line 0 without its block list. A state file that exists but cannot be read, or that is not exactly in this form, is a startup error, and so is a `position` greater than the number of lines in the item file.

The server saves the state file every 30 seconds while anything in it has changed, at once after a block, an unblock, or a wrap, and at every clean exit (see "Exiting"). A save never leaves a partly written state file. After a crash, a `kill -9`, or a power loss, the server starts again from the last save, which is normally at most about 30 seconds old: the lines handed out after that save are handed out a second time. No line is skipped.

The pause, the events, and the rates are not saved.

To change the state, stop the server, edit the state file, and start the server again. For example, to serve a new item file from its first line, set `position` to 0. To block an IP address that has never made a request, add a record for it with `"blocked": true`; every record needs all of the fields shown above except `host`. The server cannot notice that the item file was replaced while it was stopped: it applies the saved position to whatever file is at the path when it starts.

### The lock file

The server locks a file whose path is the state file's path with `.lock` appended (`items.txt.bqs.json.lock`), so that a second server with the same state file path refuses to start.

- The lock file is permanent and empty.
- The lock is never stale: the operating system releases it when the server's process ends, however it ends, also after a crash or a `kill -9`. So the error that the state file is in use always means that a running process holds the lock.
- Do not delete the lock file while a server runs: a second server could then start, and the two would overwrite each other's state file.
- A symlink or a hard link to the state file under another name gets a different lock file, so two servers that use the two names are not detected.
- The lock is reliable only on a local filesystem.

## The dashboard

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

From top to bottom:

1. The status line: the item file name, the port, the position and the number of lines in the file, the percent done, the estimated time until the end of the pass at the rate of the last minute (or `DONE` when cycling is off and every line has been handed out), the lines handed out and the requests in the last minute, the wrap counter (with `--cycle` only), and the number of client IP addresses and how many of them are blocked. While the server is paused, the line starts with `PAUSED`.
2. The clients table, one row per client IP address. `FIRST` and `LAST` are the times since the first and the most recent request. `HOST` is the reverse DNS name (the `PTR` record) of the client IP address, as a hint about who the client is: `-` if its lookup in this run didn't find a name, and empty while the client doesn't have a name and its lookup in this run hasn't finished. `UA` is the most recent `User-Agent`; this column is hidden unless the operator has chosen to show it. Rows of blocked IP addresses are drawn in red. The selected row is black on cyan, or black on red if its client is blocked.
3. The events pane: the most recent requests, newest at the bottom, each with its time, its client, and its outcome (the number of lines handed out, `blocked`, `wrong path`, `paused`, or `rejected` with the status code).
4. The key bar.

The terminal window's title shows the state too: `12.5% items.txt`, `PAUSED 12.5% items.txt`, `DONE items.txt`, and `stopped items.txt` after the server has exited (but not after a `kill -9`).

| Key or mouse | Action |
|---|---|
| `Up`, `Down`, `PageUp`, `PageDown`, `Home`, `End` | Move the selection. |
| Mouse wheel over the clients table | Move the selection by 3 rows. |
| Left click on a client row | Select that client. |
| Left click on `allowed` or `blocked` in the selected row | Block or unblock that client. |
| `F9` or `b` | Block or unblock the selected client. |
| Left click on a column header | Sort by that column. A click on the header of the sort column inverts the direction. A click on `UA` does nothing. |
| `F6` or `s` | Sort by the next shown column to the right, wrapping around. (`UA` is not sortable.) |
| `F7` or `i` | Invert the sort direction. |
| `F2` or `u` | Show or hide the `UA` column. |
| `F3` or `h` | Show or hide the `HOST` column. |
| `F5` or `p` | Pause handing out lines, or resume. A paused server answers as if it didn't have any lines left, so clients just wait. |
| `F10` or `q` | Ask whether to quit: `y` quits, `Ctrl-C` quits too, and any other key cancels. |
| `Ctrl-C` | Quit at once. |
| Left click on a key-bar label | Same as that key. |

A click on `allowed` or `blocked` in a row that is not selected only selects that client, because the rows re-sort several times per second and may have moved just before the click.

Quitting the dashboard stops the server. The sort column, the sort direction, and which of `HOST` and `UA` are shown are kept in the state file.

The reverse DNS names are not verified: the owner of an IP address can publish any name for it. The server looks names up only while it shows the dashboard (also while the `HOST` column is hidden), and each lookup sends a client IP address to the system's DNS resolver (`/etc/resolv.conf`).

## Exiting

These are clean exits: `q` followed by `y`, `Ctrl-C` in the dashboard, and the signals `SIGINT`, `SIGTERM`, and `SIGHUP` (sent when the terminal window is closed). At a clean exit, the server stops handing out lines, saves the state file, restores the terminal, and exits with status 0, or with status 1 if the save failed. Before the server has started (for example, during a long scan of the item file), these signals end it at once, which loses nothing.

If saving the state file or reading the item file fails while the server runs, the server stops handing out lines, saves the state file (unless saving is what failed), prints the error to stderr, and exits with status 1.
