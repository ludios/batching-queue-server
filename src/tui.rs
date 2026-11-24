//! The dashboard: a terminal UI that shows the queue, the clients, and the most recent requests,
//! and lets the operator block clients, pause the server, and sort the clients table.

use crate::state::{self, Client, Column, Event, Shared, View};
use jiff::Timestamp;
use jiff::tz::TimeZone;
use ratatui::buffer::Buffer;
use ratatui::crossterm::event::{
    self, Event as Input, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, SetTitle, disable_raw_mode, enable_raw_mode,
};
use ratatui::crossterm::{cursor, event::DisableMouseCapture, event::EnableMouseCapture, execute};
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::{DefaultTerminal, Terminal, backend::CrosstermBackend};
use std::cmp::Ordering;
use std::io::{self, IsTerminal, stdout};
use std::net::IpAddr;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

/// The longest time between two frames.
const FRAME_INTERVAL: Duration = Duration::from_millis(200);

/// The number of blank screen columns between two columns of the clients table.
const GAP: u16 = 2;

/// The widest a `HOST` cell gets, in screen columns.
const MAX_HOST_WIDTH: usize = 40;

/// The rows that one turn of the mouse wheel moves the selection by.
const WHEEL_ROWS: isize = 3;

/// The columns of the clients table, left to right, whether shown or not.
const COLUMNS: [Column; 8] = [
    Column::Blocked,
    Column::Ip,
    Column::Items,
    Column::First,
    Column::Last,
    Column::Reqs,
    Column::Host,
    Column::Ua,
];

/// What the server's command line says, as the dashboard shows it.
pub struct Config {
    /// The name of the item file, without its directory, cleaned by `state::clean_for_terminal`.
    pub name: String,
    /// The port the server listens on.
    pub port: u16,
    /// Whether the server starts again at the first line after handing out the last one.
    pub cycle: bool,
}

/// What the reverse DNS lookup of a client found.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Host {
    NotLookedUp,
    /// Looked up, without a name: no `PTR` record, an error, or given up.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "reverse DNS lookups are still to come")
    )]
    NoName,
    /// Looked up, with this name, cleaned by `state::clean_for_terminal`.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "reverse DNS lookups are still to come")
    )]
    Name(String),
}

/// One client, as the dashboard shows it.
#[derive(Clone, Debug)]
struct Row {
    ip: IpAddr,
    client: Client,
    host: Host,
}

/// What the dashboard copies out of the state for one frame, so that it draws without holding
/// the state lock.
#[derive(Clone, Debug)]
struct Snapshot {
    position: u64,
    total: u64,
    wraps: u64,
    paused: bool,
    view: View,
    /// Every client, sorted as `view` says.
    rows: Vec<Row>,
    /// The most recent events, oldest first.
    events: Vec<Event>,
    /// The lines handed out in the last 60 seconds.
    lines_per_minute: u64,
    /// The requests in the last 60 seconds.
    requests_per_minute: u64,
}

/// Copies what a frame shows from the state in `shared` at `now`, with at most `events` events,
/// and sorts the clients after releasing the state lock.
fn snapshot(shared: &Shared, events: usize, now: Timestamp) -> Snapshot {
    let mut snapshot = {
        let state = shared.state.lock().unwrap();
        let last_minute = state.last_minute(now);
        let all_events = state.events();
        Snapshot {
            position: state.queue().position(),
            total: state.queue().total(),
            wraps: state.wraps(),
            paused: state.is_paused(),
            view: state.view(),
            rows: state
                .clients()
                .iter()
                .map(|(&ip, client)| Row {
                    ip,
                    client: client.clone(),
                    host: Host::NotLookedUp,
                })
                .collect(),
            events: all_events
                .range(all_events.len().saturating_sub(events)..)
                .cloned()
                .collect(),
            lines_per_minute: last_minute.lines,
            requests_per_minute: last_minute.requests,
        }
    };
    let view = snapshot.view;
    snapshot.rows.sort_by(|a, b| compare_rows(&view, a, b));
    snapshot
}

fn header(column: Column) -> &'static str {
    match column {
        Column::Blocked => "",
        Column::Ip => "IP",
        Column::Items => "ITEMS",
        Column::First => "FIRST",
        Column::Last => "LAST",
        Column::Reqs => "REQS",
        Column::Host => "HOST",
        Column::Ua => "UA",
    }
}

fn is_sortable(column: Column) -> bool {
    column != Column::Ua
}

fn is_right_aligned(column: Column) -> bool {
    matches!(
        column,
        Column::Items | Column::First | Column::Last | Column::Reqs
    )
}

/// Whether `column` sorts with the largest value first (the most recent time, or blocked
/// clients first) when it becomes the sort column.
fn is_descending_at_first(column: Column) -> bool {
    matches!(
        column,
        Column::Blocked | Column::Items | Column::First | Column::Last | Column::Reqs
    )
}

/// Whether the clients table sorts with the largest value first.
fn is_descending(view: &View) -> bool {
    is_descending_at_first(view.sort_column) != view.sort_inverted
}

fn is_shown(view: &View, column: Column) -> bool {
    match column {
        Column::Host => view.show_host,
        Column::Ua => view.show_ua,
        _ => true,
    }
}

/// Makes `column` the sort column, with its direction when first chosen, or inverts the
/// direction if it already is the sort column.
fn sort_by(view: &mut View, column: Column) {
    assert!(is_sortable(column), "{column:?} is not sortable");
    if view.sort_column == column {
        view.sort_inverted = !view.sort_inverted;
    } else {
        view.sort_column = column;
        view.sort_inverted = false;
    }
}

/// Makes the next sortable column that is shown, to the right of the sort column and wrapping
/// around, the sort column, with its direction when first chosen.
fn sort_by_next(view: &mut View) {
    let current = COLUMNS
        .iter()
        .position(|&column| column == view.sort_column)
        .expect("the sort column is a column");
    let next = (1..COLUMNS.len())
        .map(|offset| COLUMNS[(current + offset) % COLUMNS.len()])
        .find(|&column| is_sortable(column) && is_shown(view, column))
        .expect("some sortable column is always shown");
    view.sort_column = next;
    view.sort_inverted = false;
}

/// Compares the reverse DNS names `a` and `b` label by label, from the last label to the first,
/// ignoring case, so that the names of one domain are adjacent.
fn compare_hosts(a: &str, b: &str) -> Ordering {
    let labels = |name: &str| {
        name.rsplit('.')
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>()
    };
    labels(a).cmp(&labels(b))
}

/// Compares two rows of the clients table in the order that `view` says. Rows that compare equal
/// by the sort column are ordered by IP address, so that rows never jitter.
fn compare_rows(view: &View, a: &Row, b: &Row) -> Ordering {
    let directed = |ordering: Ordering| {
        if is_descending(view) {
            ordering.reverse()
        } else {
            ordering
        }
    };
    let ordering = match view.sort_column {
        Column::Blocked => directed(a.client.blocked.cmp(&b.client.blocked)),
        Column::Ip => directed(a.ip.cmp(&b.ip)),
        Column::Items => directed(a.client.items.cmp(&b.client.items)),
        Column::First => directed(a.client.first_seen.cmp(&b.client.first_seen)),
        Column::Last => directed(a.client.last_seen.cmp(&b.client.last_seen)),
        Column::Reqs => directed(a.client.requests.cmp(&b.client.requests)),
        // Clients without a name come last in both directions
        Column::Host => match (&a.host, &b.host) {
            (Host::Name(a), Host::Name(b)) => directed(compare_hosts(a, b)),
            (Host::Name(_), _) => Ordering::Less,
            (_, Host::Name(_)) => Ordering::Greater,
            _ => Ordering::Equal,
        },
        Column::Ua => panic!("the UA column is not sortable"),
    };
    ordering.then(a.ip.cmp(&b.ip))
}

/// Formats `n` with `,` as the thousands separator.
fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut formatted = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(digit);
    }
    formatted
}

/// Formats a duration of `seconds` in its largest unit only: `42s`, `17m`, `5h` (below 48
/// hours), or `12d`.
fn duration(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..172_800 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86_400),
    }
}

/// Formats the time from `then` to `now` as `duration` does, or as `0s` if `then` is after `now`.
fn age(now: Timestamp, then: Timestamp) -> String {
    duration(u64::try_from(now.as_second() - then.as_second()).unwrap_or(0))
}

/// Formats `position` as a percentage of `total` lines, rounded down to one decimal, or as `-`
/// when `total` is 0.
fn percent(position: u64, total: u64) -> String {
    if total == 0 {
        return "-".to_owned();
    }
    let tenths = u128::from(position) * 1000 / u128::from(total);
    format!("{}.{}%", tenths / 10, tenths % 10)
}

/// Returns `name` if it fits in a `HOST` cell, or else `…` followed by as much of its end as
/// fits, because the domain is at the end.
fn shorten_host(name: &str) -> String {
    let length = name.chars().count();
    if length <= MAX_HOST_WIDTH {
        return name.to_owned();
    }
    let end: String = name.chars().skip(length - (MAX_HOST_WIDTH - 1)).collect();
    format!("…{end}")
}

/// Returns the text of the cell of `column` in the row of `row`, at `now`.
fn cell(column: Column, row: &Row, now: Timestamp) -> String {
    match column {
        Column::Blocked => block_label(row.client.blocked).to_owned(),
        Column::Ip => row.ip.to_string(),
        Column::Items => thousands(row.client.items),
        Column::First => age(now, row.client.first_seen),
        Column::Last => age(now, row.client.last_seen),
        Column::Reqs => thousands(row.client.requests),
        Column::Host => match &row.host {
            Host::NotLookedUp => String::new(),
            Host::NoName => "-".to_owned(),
            Host::Name(name) => shorten_host(name),
        },
        Column::Ua => row.client.user_agent.as_deref().unwrap_or("-").to_owned(),
    }
}

/// Returns the label in the first column of a client that is blocked as `blocked` says: what a
/// click on it does.
fn block_label(blocked: bool) -> &'static str {
    if blocked { "[unblock]" } else { "[block]" }
}

/// A column of the clients table, as placed on the screen.
#[derive(Clone, Copy, Debug)]
struct Placed {
    column: Column,
    /// The screen column of its first cell.
    x: u16,
    /// Its width in screen columns.
    width: u16,
}

impl Placed {
    fn contains(&self, x: u16) -> bool {
        x >= self.x && x - self.x < self.width
    }

    /// Draws `text` in this column on screen row `y`, aligned as the column is and cut off at
    /// the end of the column.
    fn draw(&self, buf: &mut Buffer, y: u16, text: &str, style: Style) {
        let length = u16::try_from(text.chars().count()).unwrap_or(u16::MAX);
        let mut x = self.x;
        if is_right_aligned(self.column) {
            x = x.saturating_add(self.width.saturating_sub(length));
        }
        buf.set_stringn(x, y, text, usize::from(self.width), style);
    }
}

/// Places the columns that `view` shows, left to right from screen column `x`, each as wide as
/// its widest cell among `rows` at `now`. The `UA` column takes the rest of `width` columns.
fn place_columns(view: &View, rows: &[Row], now: Timestamp, x: u16, width: u16) -> Vec<Placed> {
    let mut placed = Vec::new();
    let mut next_x = x;
    for column in COLUMNS.into_iter().filter(|&column| is_shown(view, column)) {
        let column_width = if column == Column::Ua {
            x.saturating_add(width).saturating_sub(next_x)
        } else {
            // Room for the sort arrow after the header, so that the arrow does not shift the
            // columns to its right
            let header_width = header(column).len() + usize::from(is_sortable(column));
            let cells_width = if column == Column::Blocked {
                // As wide as it gets, so that the first block does not shift the table
                block_label(true).len()
            } else {
                rows.iter()
                    .map(|row| cell(column, row, now).chars().count())
                    .max()
                    .unwrap_or(0)
            };
            u16::try_from(header_width.max(cells_width)).unwrap_or(u16::MAX)
        };
        placed.push(Placed {
            column,
            x: next_x,
            width: column_width,
        });
        next_x = next_x.saturating_add(column_width).saturating_add(GAP);
    }
    placed
}

/// A client row as drawn on the screen.
#[derive(Clone, Copy, Debug)]
struct DrawnRow {
    y: u16,
    ip: IpAddr,
    blocked: bool,
}

/// What the most recently drawn frame showed where, so that a click acts on what the operator
/// saw, also when the rows have been re-sorted since.
#[derive(Debug, Default)]
struct Drawn {
    /// The screen row of the header of the clients table, if it was drawn.
    header_y: Option<u16>,
    /// The columns of the clients table that were shown.
    columns: Vec<Placed>,
    /// The client rows that were drawn.
    rows: Vec<DrawnRow>,
    /// The screen rows of the clients table, including its header.
    table: Range<u16>,
    /// The number of client rows that fit on the screen.
    page: usize,
    /// Every client IP address, in the order of the table.
    order: Vec<IpAddr>,
    /// The selected client, and whether it was blocked.
    selected: Option<(IpAddr, bool)>,
    /// The screen row of the key bar, if it was drawn.
    key_bar_y: Option<u16>,
    /// The labels of the key bar: their screen columns and their keys.
    labels: Vec<(Range<u16>, KeyCode)>,
}

/// What an input from the operator asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    /// Moves the selection by this many rows down (negative: up), stopping at the first and the
    /// last row.
    MoveBy(isize),
    Select(IpAddr),
    SetBlocked(IpAddr, bool),
    SortBy(Column),
    SortByNext,
    InvertSort,
    ToggleUa,
    ToggleHost,
    TogglePause,
    AskQuit,
    CancelQuit,
    Quit,
}

/// The dashboard: what the operator has selected and seen, and how inputs change that and the
/// state of the server.
struct Dashboard {
    config: Config,
    /// The time zone of the event times.
    time_zone: TimeZone,
    /// The selected client. With no clients, nothing is selected.
    selected: Option<IpAddr>,
    /// The index of the first client row that is drawn.
    scroll: usize,
    /// Whether the key bar asks whether to quit.
    asking_quit: bool,
    drawn: Drawn,
}

impl Dashboard {
    fn new(config: Config, time_zone: TimeZone) -> Dashboard {
        Dashboard {
            config,
            time_zone,
            selected: None,
            scroll: 0,
            asking_quit: false,
            drawn: Drawn::default(),
        }
    }

    /// Whether the server has handed out every line and hands out no more.
    fn is_done(&self, snapshot: &Snapshot) -> bool {
        !self.config.cycle && snapshot.position == snapshot.total
    }

    /// Returns the title of the terminal window for `snapshot`.
    fn title(&self, snapshot: &Snapshot) -> String {
        let name = &self.config.name;
        if self.is_done(snapshot) {
            return format!("DONE {name}");
        }
        let percent = percent(snapshot.position, snapshot.total);
        if snapshot.paused {
            format!("PAUSED {percent} {name}")
        } else {
            format!("{percent} {name}")
        }
    }

    /// Returns the status line for `snapshot`, without its `PAUSED` part.
    fn status(&self, snapshot: &Snapshot) -> String {
        let eta = if self.is_done(snapshot) {
            "DONE".to_owned()
        } else if snapshot.lines_per_minute == 0 {
            "ETA -".to_owned()
        } else {
            let remaining = snapshot.total.saturating_sub(snapshot.position);
            let seconds = u128::from(remaining) * 60 / u128::from(snapshot.lines_per_minute);
            format!(
                "ETA {}",
                duration(u64::try_from(seconds).unwrap_or(u64::MAX))
            )
        };
        let wraps = if self.config.cycle {
            format!("  wrap {}", thousands(snapshot.wraps))
        } else {
            String::new()
        };
        let blocked = snapshot
            .rows
            .iter()
            .filter(|row| row.client.blocked)
            .count();
        let ips = if snapshot.rows.len() == 1 {
            "IP"
        } else {
            "IPs"
        };
        format!(
            "{} :{}  {}/{} {}  {eta}  {} items/min  {} req/min{wraps}  {} {ips}, {} blocked",
            self.config.name,
            self.config.port,
            thousands(snapshot.position),
            thousands(snapshot.total),
            percent(snapshot.position, snapshot.total),
            thousands(snapshot.lines_per_minute),
            thousands(snapshot.requests_per_minute),
            thousands(snapshot.rows.len() as u64),
            thousands(blocked as u64),
        )
    }

    /// Draws `snapshot` at `now` into `buf`, which covers the whole terminal, and records what
    /// it drew where.
    fn draw(&mut self, buf: &mut Buffer, snapshot: &Snapshot, now: Timestamp) {
        self.drawn = Drawn::default();
        let area = *buf.area();
        if area.is_empty() {
            return;
        }

        let mut x = area.x;
        if snapshot.paused {
            let paused = "PAUSED";
            let style = Style::new().add_modifier(Modifier::REVERSED | Modifier::BOLD);
            x = buf
                .set_stringn(x, area.y, paused, usize::from(area.width), style)
                .0;
            x = x.saturating_add(1);
        }
        let status = self.status(snapshot);
        buf.set_stringn(x, area.y, status, usize::from(area.width), Style::new());

        // Between the status line and the key bar
        let middle = area.height.saturating_sub(2);
        let events_height = middle / 3;
        let table_height = middle - events_height;
        let table = Rect::new(area.x, area.y + 1, area.width, table_height);
        self.draw_table(buf, table, snapshot, now);
        let events = Rect::new(area.x, table.bottom(), area.width, events_height);
        self.draw_events(buf, events, &snapshot.events);
        if area.height >= 2 {
            self.draw_key_bar(buf, area.bottom() - 1, area, snapshot.paused);
        }
    }

    /// Draws the clients table of `snapshot` at `now` in `area` of `buf`.
    fn draw_table(&mut self, buf: &mut Buffer, area: Rect, snapshot: &Snapshot, now: Timestamp) {
        let rows = &snapshot.rows;
        if !rows.iter().any(|row| Some(row.ip) == self.selected) {
            self.selected = rows.first().map(|row| row.ip);
        }
        let selected = rows.iter().position(|row| Some(row.ip) == self.selected);
        self.drawn.order = rows.iter().map(|row| row.ip).collect();
        self.drawn.selected = selected.map(|i| (rows[i].ip, rows[i].client.blocked));
        self.drawn.table = area.y..area.bottom();
        if area.is_empty() {
            return;
        }

        let page = usize::from(area.height - 1);
        self.drawn.page = page;
        let selected = selected.unwrap_or(0);
        if selected < self.scroll {
            self.scroll = selected;
        }
        if page > 0 && selected >= self.scroll + page {
            self.scroll = selected + 1 - page;
        }
        self.scroll = self.scroll.min(rows.len().saturating_sub(page));

        let view = &snapshot.view;
        let columns = place_columns(view, rows, now, area.x, area.width);
        let header_style = Style::new().add_modifier(Modifier::BOLD);
        for placed in &columns {
            let arrow = match placed.column {
                column if column != view.sort_column => "",
                _ if is_descending(view) => "▼",
                _ => "▲",
            };
            let text = format!("{}{arrow}", header(placed.column));
            placed.draw(buf, area.y, &text, header_style);
        }
        self.drawn.header_y = Some(area.y);

        for (y, row) in (area.y + 1..area.bottom()).zip(rows.iter().skip(self.scroll)) {
            let mut style = Style::new();
            if row.client.blocked {
                style = style.fg(Color::Red);
            }
            if Some(row.ip) == self.selected {
                style = style.add_modifier(Modifier::REVERSED);
            }
            buf.set_style(Rect::new(area.x, y, area.width, 1), style);
            for placed in &columns {
                placed.draw(buf, y, &cell(placed.column, row, now), style);
            }
            self.drawn.rows.push(DrawnRow {
                y,
                ip: row.ip,
                blocked: row.client.blocked,
            });
        }
        self.drawn.columns = columns;
    }

    /// Draws the newest of `events` in `area` of `buf`, the newest at the bottom.
    fn draw_events(&self, buf: &mut Buffer, area: Rect, events: &[Event]) {
        let events = &events[events.len().saturating_sub(usize::from(area.height))..];
        let ips: Vec<String> = events.iter().map(|event| event.ip.to_string()).collect();
        let ip_width = ips.iter().map(String::len).max().unwrap_or(0);
        let top = area.bottom() - u16::try_from(events.len()).expect("as many as fit");
        for ((y, event), ip) in (top..area.bottom()).zip(events).zip(ips) {
            let time = self.time_zone.to_datetime(event.time).strftime("%H:%M:%S");
            let text = format!("{time} {ip:ip_width$}  {}", event.outcome);
            buf.set_stringn(area.x, y, text, usize::from(area.width), Style::new());
        }
    }

    /// Draws the key bar on screen row `y` of `area` of `buf`.
    fn draw_key_bar(&mut self, buf: &mut Buffer, y: u16, area: Rect, paused: bool) {
        self.drawn.key_bar_y = Some(y);
        let width = usize::from(area.width);
        if self.asking_quit {
            let style = Style::new().add_modifier(Modifier::REVERSED);
            buf.set_stringn(area.x, y, "Quit the server? y/n", width, style);
            return;
        }
        let labels = [
            (KeyCode::F(2), "F2", "UA"),
            (KeyCode::F(3), "F3", "Host"),
            (KeyCode::F(5), "F5", if paused { "Resume" } else { "Pause" }),
            (KeyCode::F(6), "F6", "Sort"),
            (KeyCode::F(7), "F7", "Invert"),
            (KeyCode::F(9), "F9", "Block"),
            (KeyCode::F(10), "F10", "Quit"),
        ];
        let key_style = Style::new().add_modifier(Modifier::BOLD);
        let mut x = area.x;
        for (key, key_name, label) in labels {
            let start = x;
            x = buf.set_stringn(x, y, key_name, width, key_style).0;
            x = buf
                .set_stringn(x.saturating_add(1), y, label, width, Style::new())
                .0;
            self.drawn.labels.push((start..x, key));
            x = x.saturating_add(GAP);
        }
    }

    /// Returns what the key press, repeat, or release `key` asks for.
    fn key_action(&self, key: &KeyEvent) -> Option<Action> {
        let pressed = key.kind == KeyEventKind::Press;
        if pressed
            && key.code == KeyCode::Char('c')
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return Some(Action::Quit);
        }
        if self.asking_quit {
            if !pressed {
                return None;
            }
            if key.code == KeyCode::Char('y') {
                return Some(Action::Quit);
            }
            return Some(Action::CancelQuit);
        }
        // Only the keys that move the selection react to key repeats
        let page = isize::try_from(self.drawn.page.max(1)).unwrap_or(isize::MAX);
        let rows = match key.code {
            KeyCode::Up => -1,
            KeyCode::Down => 1,
            KeyCode::PageUp => -page,
            KeyCode::PageDown => page,
            KeyCode::Home => isize::MIN,
            KeyCode::End => isize::MAX,
            _ => 0,
        };
        if rows != 0 {
            if key.kind == KeyEventKind::Release {
                return None;
            }
            return Some(Action::MoveBy(rows));
        }
        if !pressed {
            return None;
        }
        match key.code {
            KeyCode::F(9) | KeyCode::Char('b') => {
                let (ip, blocked) = self.drawn.selected?;
                Some(Action::SetBlocked(ip, !blocked))
            }
            KeyCode::F(6) | KeyCode::Char('s') => Some(Action::SortByNext),
            KeyCode::F(7) | KeyCode::Char('i') => Some(Action::InvertSort),
            KeyCode::F(2) | KeyCode::Char('u') => Some(Action::ToggleUa),
            KeyCode::F(3) | KeyCode::Char('h') => Some(Action::ToggleHost),
            KeyCode::F(5) | KeyCode::Char('p') => Some(Action::TogglePause),
            KeyCode::F(10) | KeyCode::Char('q') => Some(Action::AskQuit),
            _ => None,
        }
    }

    /// Returns what a left click on the screen cell at `x`, `y` asks for, resolved against what
    /// the most recently drawn frame showed there.
    fn click_action(&self, x: u16, y: u16) -> Option<Action> {
        let drawn = &self.drawn;
        if drawn.key_bar_y == Some(y) {
            let (_, key) = drawn.labels.iter().find(|(range, _)| range.contains(&x))?;
            return self.key_action(&KeyEvent::new(*key, KeyModifiers::NONE));
        }
        let placed = drawn.columns.iter().find(|placed| placed.contains(x));
        if drawn.header_y == Some(y) {
            let column = placed?.column;
            return is_sortable(column).then_some(Action::SortBy(column));
        }
        let row = drawn.rows.iter().find(|row| row.y == y)?;
        // A click never blocks a client that was not already selected, because the row under
        // the pointer may have changed just before the click
        let on_label = placed.is_some_and(|placed| {
            placed.column == Column::Blocked
                && usize::from(x - placed.x) < block_label(row.blocked).len()
        });
        if on_label && drawn.selected.map(|(ip, _)| ip) == Some(row.ip) {
            return Some(Action::SetBlocked(row.ip, !row.blocked));
        }
        Some(Action::Select(row.ip))
    }

    /// Returns what `input` asks for.
    fn action(&self, input: &Input) -> Option<Action> {
        match input {
            Input::Key(key) => self.key_action(key),
            Input::Mouse(MouseEvent {
                kind, column, row, ..
            }) => {
                let on_table = self.drawn.table.contains(row);
                let action = match kind {
                    MouseEventKind::Down(MouseButton::Left) => self.click_action(*column, *row),
                    MouseEventKind::ScrollUp if on_table => Some(Action::MoveBy(-WHEEL_ROWS)),
                    MouseEventKind::ScrollDown if on_table => Some(Action::MoveBy(WHEEL_ROWS)),
                    _ => None,
                };
                if self.asking_quit && action.is_some() {
                    return Some(Action::CancelQuit);
                }
                action
            }
            _ => None,
        }
    }

    /// Does what `action` asks for, to the dashboard and to the state in `shared`.
    ///
    /// Returns whether the operator chose to quit.
    fn apply(&mut self, action: Action, shared: &Shared) -> bool {
        let change_view = |change: &dyn Fn(&mut View)| {
            let mut state = shared.state.lock().unwrap();
            let mut view = state.view();
            change(&mut view);
            state.set_view(view);
        };
        match action {
            Action::MoveBy(rows) => {
                let order = &self.drawn.order;
                if order.is_empty() {
                    return false;
                }
                let current = order
                    .iter()
                    .position(|&ip| Some(ip) == self.selected)
                    .unwrap_or(0);
                let next = current.saturating_add_signed(rows).min(order.len() - 1);
                self.selected = Some(order[next]);
            }
            Action::Select(ip) => self.selected = Some(ip),
            Action::SetBlocked(ip, blocked) => {
                shared.state.lock().unwrap().set_blocked(ip, blocked);
            }
            Action::SortBy(column) => change_view(&|view| sort_by(view, column)),
            Action::SortByNext => change_view(&sort_by_next),
            Action::InvertSort => change_view(&|view| view.sort_inverted = !view.sort_inverted),
            Action::ToggleUa => change_view(&|view| view.show_ua = !view.show_ua),
            Action::ToggleHost => change_view(&|view| view.show_host = !view.show_host),
            Action::TogglePause => {
                let mut state = shared.state.lock().unwrap();
                let paused = state.is_paused();
                state.set_paused(!paused);
            }
            Action::AskQuit => self.asking_quit = true,
            Action::CancelQuit => self.asking_quit = false,
            Action::Quit => return true,
        }
        false
    }

    /// Does what `input` asks for.
    ///
    /// Returns whether the operator chose to quit.
    fn input(&mut self, input: &Input, shared: &Shared) -> bool {
        match self.action(input) {
            Some(action) => self.apply(action, shared),
            None => false,
        }
    }

    /// Draws frames on `terminal` and handles the inputs from `inputs` until `stop` is set, and
    /// sends a message through `quit` each time the operator chooses to quit.
    ///
    /// Returns an error if the terminal fails or is gone.
    fn run(
        &mut self,
        terminal: &mut DefaultTerminal,
        inputs: &Receiver<io::Result<Input>>,
        shared: &Shared,
        stop: &AtomicBool,
        quit: &UnboundedSender<()>,
    ) -> io::Result<()> {
        let mut title = String::new();
        while !stop.load(AtomicOrdering::Relaxed) {
            // A terminal that has hung up fails this check. Without it, the dashboard might not
            // notice, as long as the frames are unchanged and nothing is written.
            if !stdout().is_terminal() {
                return Err(io::Error::other("the terminal is gone"));
            }
            let now = state::now();
            // No more events than rows fit on the screen
            let snapshot = snapshot(shared, usize::from(terminal.size()?.height), now);
            terminal.draw(|frame| self.draw(frame.buffer_mut(), &snapshot, now))?;
            let new_title = self.title(&snapshot);
            if new_title != title {
                execute!(stdout(), SetTitle(&new_title))?;
                title = new_title;
            }
            // Waiting with a timeout, so that neither a frame nor `stop` waits for an input
            let input = match inputs.recv_timeout(FRAME_INTERVAL) {
                Ok(input) => input?,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("the input reader ends only after it sends its error")
                }
            };
            if self.input(&input, shared) {
                // If main has stopped receiving, it is exiting already
                let _ = quit.send(());
            }
        }
        Ok(())
    }
}

/// Reads inputs from the terminal and sends them through `inputs`, until reading fails (after
/// sending the error) or nothing receives them.
///
/// On a terminal that has hung up, `event::read` of crossterm 0.29 never returns and keeps a CPU
/// busy (crossterm issue 1067). So this runs on a thread of its own, which ends with the process,
/// and the dashboard does not depend on it to end.
fn read_inputs(inputs: Sender<io::Result<Input>>) {
    loop {
        let input = event::read();
        let failed = input.is_err();
        if inputs.send(input).is_err() || failed {
            return;
        }
    }
}

/// Sets the `stopped` title and restores the terminal, as far as that works: after SIGHUP, the
/// terminal is gone.
fn restore_terminal(name: &str) {
    let _ = execute!(stdout(), SetTitle(format!("stopped {name}")));
    let _ = execute!(stdout(), DisableMouseCapture);
    // Before leaving the alternate screen, as `ratatui::try_restore` does
    let _ = disable_raw_mode();
    let _ = execute!(stdout(), LeaveAlternateScreen, cursor::Show);
}

/// Switches the terminal to the dashboard: raw mode, the alternate screen, and mouse capture.
fn enter_terminal() -> io::Result<DefaultTerminal> {
    enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen, EnableMouseCapture)?;
    Terminal::new(CrosstermBackend::new(stdout()))
}

/// The dashboard, running on a thread of its own.
pub struct Running {
    stop: Arc<AtomicBool>,
    thread: std::thread::JoinHandle<()>,
}

impl Running {
    /// Stops the dashboard, sets the `stopped` title, and restores the terminal.
    pub fn finish(self) {
        self.stop.store(true, AtomicOrdering::Relaxed);
        self.thread
            .join()
            .expect("the dashboard thread does not panic");
    }
}

/// Switches the terminal to the dashboard for the state in `shared` and runs it on a thread of
/// its own, until `Running::finish`. A panic anywhere first sets the `stopped` title and restores
/// the terminal.
///
/// `quit` receives a message each time the operator chooses to quit. `fatal` receives the error
/// if the terminal fails, after the terminal has been restored as far as possible.
pub fn start(
    shared: Arc<Shared>,
    config: Config,
    quit: UnboundedSender<()>,
    fatal: UnboundedSender<anyhow::Error>,
) -> io::Result<Running> {
    let name = config.name.clone();
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal(&name);
        previous_hook(info);
    }));

    let mut terminal = match enter_terminal() {
        Ok(terminal) => terminal,
        Err(error) => {
            restore_terminal(&config.name);
            return Err(error);
        }
    };
    let (input_sender, inputs) = mpsc::channel();
    std::thread::Builder::new()
        .name("terminal input".to_owned())
        .spawn(move || read_inputs(input_sender))?;
    let stop = Arc::new(AtomicBool::new(false));
    let thread = std::thread::Builder::new()
        .name("dashboard".to_owned())
        .spawn({
            let stop = stop.clone();
            move || {
                let mut dashboard = Dashboard::new(config, TimeZone::system());
                let result = dashboard.run(&mut terminal, &inputs, &shared, &stop, &quit);
                restore_terminal(&dashboard.config.name);
                if let Err(error) = result {
                    let _ = fatal.send(anyhow::Error::new(error).context("the terminal failed"));
                }
            }
        })?;
    Ok(Running { stop, thread })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Queue;
    use crate::state::{Outcome, State};
    use proptest::prelude::*;
    use ratatui::backend::TestBackend;
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::TempDir;

    const A: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, 7));
    const B: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(198, 51, 100, 9));
    const C: IpAddr = IpAddr::V6(std::net::Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0x17));

    fn time(text: &str) -> Timestamp {
        text.parse().unwrap()
    }

    fn now() -> Timestamp {
        time("2026-11-24T12:00:00Z")
    }

    fn client(items: u64, requests: u64) -> Client {
        Client {
            blocked: false,
            items,
            requests,
            first_seen: time("2026-11-21T12:00:00Z"),
            last_seen: time("2026-11-24T11:59:58Z"),
            user_agent: Some("curl/8.9.1".to_owned()),
        }
    }

    /// A dashboard and the state of a server whose item file holds `lines` lines, in a new
    /// temporary directory.
    struct Test {
        _dir: TempDir,
        shared: Arc<Shared>,
        dashboard: Dashboard,
        terminal: Terminal<TestBackend>,
    }

    impl Test {
        /// `view` is the saved view settings to start from, and `clients` the known clients.
        fn new(lines: usize, cycle: bool, view: View, clients: &[(IpAddr, Client)]) -> Test {
            let dir = tempfile::tempdir().unwrap();
            let item_file = dir.path().join("items.txt");
            fs::write(&item_file, "x\n".repeat(lines)).unwrap();
            let queue = Queue::open(&item_file, 0, cycle).unwrap();
            let clients = clients.iter().cloned().collect::<BTreeMap<_, _>>();
            let state = State::new(queue, 0, clients, view);
            let shared = Arc::new(Shared::new(state, dir.path().join("items.txt.bqs.json")));
            let config = Config {
                name: "items.txt".to_owned(),
                port: 31417,
                cycle,
            };
            Test {
                _dir: dir,
                shared,
                dashboard: Dashboard::new(config, TimeZone::UTC),
                terminal: Terminal::new(TestBackend::new(100, 12)).unwrap(),
            }
        }

        /// Draws a frame and returns its rows.
        fn draw(&mut self) -> Vec<String> {
            let height = self.terminal.size().unwrap().height;
            let snapshot = snapshot(&self.shared, usize::from(height), now());
            let dashboard = &mut self.dashboard;
            self.terminal
                .draw(|frame| dashboard.draw(frame.buffer_mut(), &snapshot, now()))
                .unwrap();
            screen(self.terminal.backend().buffer())
        }

        /// Handles `input`, then draws a frame, as the dashboard does.
        ///
        /// Returns whether the operator chose to quit.
        fn input(&mut self, input: Input) -> bool {
            let quit = self.dashboard.input(&input, &self.shared);
            self.draw();
            quit
        }

        fn press(&mut self, code: KeyCode) -> bool {
            self.input(Input::Key(KeyEvent::new(code, KeyModifiers::NONE)))
        }

        fn click(&mut self, x: u16, y: u16) -> bool {
            self.input(mouse(MouseEventKind::Down(MouseButton::Left), x, y))
        }

        /// Clicks the first cell of the first `text` in screen row `y` of the most recently
        /// drawn frame.
        fn click_text(&mut self, y: u16, text: &str) {
            let screen = screen(self.terminal.backend().buffer());
            let (x, _) = find(&screen[usize::from(y)..=usize::from(y)], text);
            self.click(x, y);
        }

        /// Returns the IP addresses of the client rows of the most recently drawn frame.
        fn order(&self) -> Vec<IpAddr> {
            self.dashboard.drawn.rows.iter().map(|row| row.ip).collect()
        }

        fn view(&self) -> View {
            self.shared.state.lock().unwrap().view()
        }

        fn is_blocked(&self, ip: IpAddr) -> bool {
            self.shared.state.lock().unwrap().clients()[&ip].blocked
        }
    }

    fn mouse(kind: MouseEventKind, column: u16, row: u16) -> Input {
        Input::Mouse(MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        })
    }

    /// Returns the rows of `buffer` as text, without trailing spaces.
    fn screen(buffer: &Buffer) -> Vec<String> {
        let width = usize::from(buffer.area.width);
        buffer
            .content
            .chunks(width)
            .map(|row| {
                let text: String = row.iter().map(|cell| cell.symbol()).collect();
                text.trim_end().to_owned()
            })
            .collect()
    }

    /// Returns the screen column and row of the first `text` in `screen`.
    fn find(screen: &[String], text: &str) -> (u16, u16) {
        for (y, row) in screen.iter().enumerate() {
            if let Some(offset) = row.find(text) {
                let x = row[..offset].chars().count();
                return (u16::try_from(x).unwrap(), u16::try_from(y).unwrap());
            }
        }
        panic!("{text:?} is not on the screen:\n{}", screen.join("\n"));
    }

    fn three_clients() -> [(IpAddr, Client); 3] {
        [
            (A, client(12_400, 124)),
            (B, client(300, 1_215)),
            (C, client(0, 1)),
        ]
    }

    #[test]
    fn layout() {
        let mut test = Test::new(10, true, View::default(), &three_clients());
        test.shared.state.lock().unwrap().set_blocked(B, true);
        let ips_and_outcomes = [(A, Outcome::Lines(100)), (B, Outcome::Blocked)];
        for (ip, outcome) in ips_and_outcomes {
            test.shared.state.lock().unwrap().record_outcome(
                ip,
                time("2026-11-24T11:59:59Z"),
                outcome,
            );
        }
        assert_eq!(
            test.draw(),
            [
                "items.txt :31417  0/10 0.0%  ETA 6s  100 items/min  2 req/min  wrap 0  3 IPs, 1 blocked",
                "           IP            ITEMS▼   FIRST   LAST   REQS  HOST",
                "[block]    203.0.113.7   12,400      3d     2s    124",
                "[unblock]  198.51.100.9     300      3d     2s  1,215",
                "[block]    2001:db8::17       0      3d     2s      1",
                "",
                "",
                "",
                "",
                "11:59:59 203.0.113.7   100",
                "11:59:59 198.51.100.9  blocked",
                "F2 UA  F3 Host  F5 Pause  F6 Sort  F7 Invert  F9 Block  F10 Quit",
            ]
        );
    }

    #[test]
    fn sorting_by_click_and_by_key() {
        let mut test = Test::new(10, false, View::default(), &three_clients());
        test.draw();
        assert_eq!(test.order(), [A, B, C]);

        test.click_text(1, "REQS");
        assert_eq!(test.view().sort_column, Column::Reqs);
        assert_eq!(test.order(), [B, A, C]);
        assert!(test.draw()[1].contains("REQS▼"));
        test.click_text(1, "REQS");
        assert_eq!(test.order(), [C, A, B]);
        assert!(test.draw()[1].contains("REQS▲"));

        // A column that becomes the sort column starts with its direction when first chosen
        test.click_text(1, "IP");
        assert_eq!(
            (test.view().sort_column, test.view().sort_inverted),
            (Column::Ip, false)
        );
        assert_eq!(test.order(), [B, A, C]);
        assert!(test.draw()[1].contains("IP▲"));

        test.press(KeyCode::F(6));
        assert_eq!(test.view().sort_column, Column::Items);
        test.press(KeyCode::Char('i'));
        assert_eq!(test.order(), [C, B, A]);
        assert!(test.draw()[1].contains("ITEMS▲"));
        test.press(KeyCode::F(7));
        assert_eq!(test.order(), [A, B, C]);

        test.press(KeyCode::Char('s'));
        test.press(KeyCode::Char('i'));
        assert_eq!(
            (test.view().sort_column, test.view().sort_inverted),
            (Column::First, true)
        );
        test.press(KeyCode::F(6));
        assert_eq!(
            (test.view().sort_column, test.view().sort_inverted),
            (Column::Last, false)
        );
        test.press(KeyCode::F(6));
        test.press(KeyCode::F(6));
        assert_eq!(test.view().sort_column, Column::Host);
        // UA is not sortable, so F6 wraps around to the first column
        test.press(KeyCode::F(2));
        test.press(KeyCode::F(6));
        assert_eq!(test.view().sort_column, Column::Blocked);
        assert!(test.draw()[1].starts_with("▼"));

        // A click on the UA header does nothing
        test.click_text(1, "UA");
        assert_eq!(
            (test.view().sort_column, test.view().sort_inverted),
            (Column::Blocked, false)
        );
    }

    #[test]
    fn a_hidden_sort_column_keeps_the_order_but_loses_its_arrow() {
        let view = View {
            sort_column: Column::Host,
            sort_inverted: true,
            ..View::default()
        };
        let mut test = Test::new(10, false, view, &three_clients());
        assert!(test.draw()[1].contains("HOST▼"));
        test.press(KeyCode::F(3));
        let header = &test.draw()[1];
        assert!(!header.contains('▼') && !header.contains('▲'), "{header}");
        assert_eq!(
            test.view(),
            View {
                show_host: false,
                ..view
            }
        );
        // F6 moves on to the next shown sortable column to the right of HOST, wrapping around
        test.press(KeyCode::F(6));
        assert_eq!(
            (test.view().sort_column, test.view().sort_inverted),
            (Column::Blocked, false)
        );

        // Also when the saved view has the sort column hidden. (No client has a name, so they
        // are ordered by IP address.)
        let view = View {
            show_host: false,
            ..view
        };
        let mut test = Test::new(10, false, view, &three_clients());
        let header = &test.draw()[1];
        assert!(!header.contains('▼') && !header.contains('▲'), "{header}");
        assert!(!header.contains("HOST"), "{header}");
        assert_eq!(test.order(), [B, A, C]);
        assert_eq!(test.view(), view);
    }

    #[test]
    fn a_click_blocks_only_the_selected_client() {
        let mut test = Test::new(20_000, false, View::default(), &three_clients());
        test.draw();
        let (b_x, b_y) = find(&test.draw(), "198.51.100.9");
        assert_eq!(test.dashboard.selected, Some(A));

        // The first click on a row that is not selected only selects that client
        test.click(0, b_y);
        assert_eq!(test.dashboard.selected, Some(B));
        assert!(!test.is_blocked(B));
        // A click elsewhere in the row of the selected client does not block it either
        test.click(b_x, b_y);
        test.click(8, b_y);
        assert!(!test.is_blocked(B));

        // New data that re-sorts the rows arrives after the frame was drawn
        test.shared.state.lock().unwrap().take(B, 12_200).unwrap();
        test.click(0, b_y);
        assert!(test.is_blocked(B));
        assert!(!test.is_blocked(A) && !test.is_blocked(C));
        assert_eq!(test.order(), [B, A, C]);

        // `[unblock]` is wider than `[block]`
        let (_, b_y) = find(&test.draw(), "198.51.100.9");
        test.click(8, b_y);
        assert!(!test.is_blocked(B));
        test.press(KeyCode::F(9));
        assert!(test.is_blocked(B));
        test.press(KeyCode::Char('b'));
        assert!(!test.is_blocked(B));
    }

    #[test]
    fn the_selection_stays_on_the_same_ip() {
        let mut test = Test::new(20_000, false, View::default(), &three_clients());
        test.draw();
        test.press(KeyCode::Down);
        assert_eq!(test.dashboard.selected, Some(B));
        test.shared.state.lock().unwrap().take(C, 13_000).unwrap();
        test.draw();
        assert_eq!(test.order(), [C, A, B]);
        assert_eq!(test.dashboard.selected, Some(B));
        let buffer = test.terminal.backend().buffer();
        assert!(buffer[(20, 4)].modifier.contains(Modifier::REVERSED));
        assert!(!buffer[(20, 2)].modifier.contains(Modifier::REVERSED));
    }

    #[test]
    fn moving_the_selection() {
        let clients: Vec<(IpAddr, Client)> = (0..20u8)
            .map(|i| (IpAddr::from([10, 0, 0, i]), client(u64::from(i), 1)))
            .collect();
        let view = View {
            sort_column: Column::Ip,
            ..View::default()
        };
        let mut test = Test::new(10, false, view, &clients);
        test.draw();
        let ip = |i: u8| Some(IpAddr::from([10, 0, 0, i]));
        assert_eq!(test.dashboard.selected, ip(0));
        // 12 rows: 1 status, 1 key bar, 3 events, 1 header, 6 client rows
        assert_eq!(test.dashboard.drawn.page, 6);
        test.press(KeyCode::Up);
        assert_eq!(test.dashboard.selected, ip(0));
        test.press(KeyCode::PageDown);
        assert_eq!(test.dashboard.selected, ip(6));
        // The table scrolled to keep the selection visible
        assert_eq!(test.order().last().copied(), ip(6));
        test.input(mouse(MouseEventKind::ScrollDown, 5, 3));
        assert_eq!(test.dashboard.selected, ip(9));
        test.input(mouse(MouseEventKind::ScrollUp, 5, 3));
        test.press(KeyCode::Down);
        assert_eq!(test.dashboard.selected, ip(7));
        test.press(KeyCode::End);
        assert_eq!(test.dashboard.selected, ip(19));
        test.press(KeyCode::Down);
        assert_eq!(test.dashboard.selected, ip(19));
        test.press(KeyCode::PageUp);
        assert_eq!(test.dashboard.selected, ip(13));
        test.press(KeyCode::Home);
        assert_eq!(test.dashboard.selected, ip(0));

        // The wheel moves the selection only over the clients table
        test.input(mouse(MouseEventKind::ScrollDown, 5, 10));
        assert_eq!(test.dashboard.selected, ip(0));
        // A held key repeats moves, but nothing else
        let repeat = |code| {
            Input::Key(KeyEvent::new_with_kind(
                code,
                KeyModifiers::NONE,
                KeyEventKind::Repeat,
            ))
        };
        test.input(repeat(KeyCode::Down));
        assert_eq!(test.dashboard.selected, ip(1));
        test.input(repeat(KeyCode::Char('b')));
        assert!(!test.is_blocked(IpAddr::from([10, 0, 0, 1])));
    }

    #[test]
    fn saved_view_settings_are_shown_and_saved() {
        let view = View {
            sort_column: Column::Reqs,
            sort_inverted: true,
            show_host: false,
            show_ua: true,
        };
        let mut test = Test::new(10, false, view, &three_clients());
        let screen = test.draw();
        assert!(screen[1].contains("REQS▲"));
        assert!(!screen[1].contains("HOST"));
        assert!(screen[1].contains("UA"));
        assert!(screen[2].contains("curl/8.9.1"));
        assert_eq!(test.order(), [C, A, B]);

        test.press(KeyCode::F(3));
        test.press(KeyCode::F(7));
        test.shared.save().unwrap();
        let loaded = state::load(test.shared.path()).unwrap().unwrap();
        assert_eq!(
            loaded.view,
            View {
                sort_column: Column::Reqs,
                sort_inverted: false,
                show_host: true,
                show_ua: true,
            }
        );
    }

    #[test]
    fn showing_and_hiding_columns() {
        let mut test = Test::new(10, false, View::default(), &three_clients());
        let has_ua = |screen: &[String]| screen[1].contains("UA") && screen[2].contains("curl");
        assert!(!has_ua(&test.draw()));
        assert!(test.draw()[1].contains("HOST"));
        test.press(KeyCode::F(2));
        assert!(has_ua(&test.draw()));
        test.press(KeyCode::Char('u'));
        assert!(!has_ua(&test.draw()));
        test.press(KeyCode::F(3));
        assert!(!test.draw()[1].contains("HOST"));
        test.press(KeyCode::Char('h'));
        assert!(test.draw()[1].contains("HOST"));
    }

    fn row(ip: IpAddr, host: Host) -> Row {
        Row {
            ip,
            client: client(1, 1),
            host,
        }
    }

    #[test]
    fn sorting_by_host() {
        let name = |name: &str| Host::Name(name.to_owned());
        let rows = [
            row(IpAddr::from([10, 0, 0, 1]), name("a.example.net")),
            row(IpAddr::from([10, 0, 0, 2]), Host::NoName),
            row(IpAddr::from([10, 0, 0, 3]), name("A.Example.com")),
            row(IpAddr::from([10, 0, 0, 4]), name("a.foo-bar.com")),
            row(IpAddr::from([10, 0, 0, 5]), Host::NotLookedUp),
            row(IpAddr::from([10, 0, 0, 6]), name("c.foo.com")),
            row(IpAddr::from([10, 0, 0, 7]), name("b.foo-bar.com")),
            row(IpAddr::from([10, 0, 0, 8]), name("b.example.com")),
        ];
        let sorted = |sort_inverted| {
            let view = View {
                sort_column: Column::Host,
                sort_inverted,
                ..View::default()
            };
            let mut rows = rows.to_vec();
            rows.sort_by(|a, b| compare_rows(&view, a, b));
            rows.iter()
                .map(|row| cell(Column::Host, row, now()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            sorted(false),
            [
                "A.Example.com",
                "b.example.com",
                "c.foo.com",
                "a.foo-bar.com",
                "b.foo-bar.com",
                "a.example.net",
                "-",
                ""
            ]
        );
        assert_eq!(
            sorted(true),
            [
                "a.example.net",
                "b.foo-bar.com",
                "a.foo-bar.com",
                "c.foo.com",
                "b.example.com",
                "A.Example.com",
                "-",
                ""
            ]
        );
    }

    #[test]
    fn a_long_host_name_shows_its_end() {
        let long = format!("{}.example.com", "x".repeat(60));
        let shortened = shorten_host(&long);
        assert_eq!(shortened.chars().count(), MAX_HOST_WIDTH);
        assert_eq!(shortened, format!("…{}.example.com", "x".repeat(27)));
        assert_eq!(shorten_host(&"y".repeat(40)), "y".repeat(40));

        let mut snapshot = snapshot_with(vec![row(A, Host::Name(long))]);
        snapshot.view.show_ua = true;
        let screen = draw_snapshot(&snapshot, 120, 6);
        assert!(
            screen[2].contains(&format!("{shortened}  curl/8.9.1")),
            "{screen:?}"
        );
    }

    #[test]
    fn pausing_and_resuming() {
        let mut test = Test::new(10, false, View::default(), &three_clients());
        let screen = test.draw();
        assert!(screen[11].contains("F5 Pause"));
        test.press(KeyCode::F(5));
        assert!(test.shared.state.lock().unwrap().is_paused());
        let screen = test.draw();
        assert!(screen[0].starts_with("PAUSED items.txt"));
        assert!(screen[11].contains("F5 Resume"));
        assert!(
            test.terminal.backend().buffer()[(0, 0)]
                .modifier
                .contains(Modifier::REVERSED)
        );
        test.press(KeyCode::Char('p'));
        assert!(!test.shared.state.lock().unwrap().is_paused());
        assert!(test.draw()[0].starts_with("items.txt"));

        // A click on a key bar label does what its key does
        test.click_text(11, "F5 Pause");
        assert!(test.shared.state.lock().unwrap().is_paused());
        test.click_text(11, "Resume");
        assert!(!test.shared.state.lock().unwrap().is_paused());
    }

    #[test]
    fn quitting() {
        let mut test = Test::new(10, false, View::default(), &three_clients());
        test.draw();
        let ctrl_c = Input::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert!(test.input(ctrl_c.clone()));

        assert!(!test.press(KeyCode::Char('q')));
        assert_eq!(test.draw()[11], "Quit the server? y/n");
        // Any other key cancels, and does nothing else
        assert!(!test.press(KeyCode::F(5)));
        assert!(!test.shared.state.lock().unwrap().is_paused());
        assert!(test.draw()[11].starts_with("F2 UA"));

        assert!(!test.press(KeyCode::F(10)));
        assert!(test.press(KeyCode::Char('y')));
        assert!(!test.press(KeyCode::Char('n')));
        test.click_text(11, "F10 Quit");
        assert!(test.input(ctrl_c));
    }

    #[test]
    fn status_line_and_title() {
        let clients = three_clients();
        let mut test = Test::new(3, false, View::default(), &clients);
        test.shared.state.lock().unwrap().set_blocked(B, true);
        let title = |test: &Test| {
            let snapshot = snapshot(&test.shared, 10, now());
            test.dashboard.title(&snapshot)
        };
        assert!(test.draw()[0].ends_with("  ETA -  0 items/min  0 req/min  3 IPs, 1 blocked"));
        assert_eq!(title(&test), "0.0% items.txt");

        {
            let mut state = test.shared.state.lock().unwrap();
            state.take(A, 1).unwrap();
            state.record_outcome(A, time("2026-11-24T11:59:30Z"), Outcome::Lines(1));
        }
        assert!(
            test.draw()[0]
                .starts_with("items.txt :31417  1/3 33.3%  ETA 2m  1 items/min  1 req/min  3 IPs")
        );
        assert_eq!(title(&test), "33.3% items.txt");
        test.shared.state.lock().unwrap().set_paused(true);
        assert_eq!(title(&test), "PAUSED 33.3% items.txt");

        test.shared.state.lock().unwrap().take(A, 2).unwrap();
        assert!(test.draw()[0].starts_with("PAUSED items.txt :31417  3/3 100.0%  DONE  "));
        assert_eq!(title(&test), "DONE items.txt");
    }

    #[test]
    fn an_empty_item_file() {
        for cycle in [false, true] {
            let mut test = Test::new(0, cycle, View::default(), &[]);
            let screen = test.draw();
            let title = test.dashboard.title(&snapshot(&test.shared, 10, now()));
            if cycle {
                assert!(screen[0].starts_with("items.txt :31417  0/0 -  ETA -  "));
                assert!(screen[0].ends_with("  wrap 0  0 IPs, 0 blocked"));
                assert_eq!(title, "- items.txt");
            } else {
                assert!(screen[0].starts_with("items.txt :31417  0/0 -  DONE  "));
                assert_eq!(title, "DONE items.txt");
            }
            assert_eq!(test.dashboard.selected, None);
            assert!(!test.press(KeyCode::F(9)));
            assert!(!test.press(KeyCode::End));
        }
    }

    #[test]
    fn formatting() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1000), "1,000");
        assert_eq!(thousands(1_234_567), "1,234,567");
        assert_eq!(thousands(u64::MAX), "18,446,744,073,709,551,615");
        assert_eq!(duration(59), "59s");
        assert_eq!(duration(60), "1m");
        assert_eq!(duration(3599), "59m");
        assert_eq!(duration(3600), "1h");
        assert_eq!(duration(48 * 3600 - 1), "47h");
        assert_eq!(duration(48 * 3600), "2d");
        assert_eq!(duration(u64::MAX), "213503982334601d");
        // A time in the future
        assert_eq!(age(now(), time("2026-11-25T12:00:00Z")), "0s");
        assert_eq!(age(Timestamp::MAX, Timestamp::MIN), "7304481d");
        assert_eq!(percent(0, 0), "-");
        assert_eq!(percent(1, 3), "33.3%");
        assert_eq!(percent(9_999, 10_000), "99.9%");
        assert_eq!(percent(u64::MAX, u64::MAX), "100.0%");
    }

    /// A snapshot of a server without events, with `rows` as its clients.
    fn snapshot_with(rows: Vec<Row>) -> Snapshot {
        Snapshot {
            position: 0,
            total: 10,
            wraps: 0,
            paused: false,
            view: View::default(),
            rows,
            events: Vec::new(),
            lines_per_minute: 0,
            requests_per_minute: 0,
        }
    }

    /// Draws `snapshot` on a terminal of `width` × `height` and returns its rows.
    fn draw_snapshot(snapshot: &Snapshot, width: u16, height: u16) -> Vec<String> {
        let config = Config {
            name: "items.txt".to_owned(),
            port: 31417,
            cycle: true,
        };
        let mut dashboard = Dashboard::new(config, TimeZone::UTC);
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| dashboard.draw(frame.buffer_mut(), snapshot, now()))
            .unwrap();
        screen(terminal.backend().buffer())
    }

    fn arbitrary_timestamp() -> impl Strategy<Value = Timestamp> {
        (Timestamp::MIN.as_second()..=Timestamp::MAX.as_second())
            .prop_map(|second| Timestamp::from_second(second).unwrap())
    }

    fn arbitrary_row() -> impl Strategy<Value = Row> {
        let host = prop_oneof![
            Just(Host::NotLookedUp),
            Just(Host::NoName),
            ".{0,300}".prop_map(Host::Name),
        ];
        (
            any::<IpAddr>(),
            any::<bool>(),
            any::<u64>(),
            any::<u64>(),
            arbitrary_timestamp(),
            arbitrary_timestamp(),
            proptest::option::of(".{0,200}"),
            host,
        )
            .prop_map(
                |(ip, blocked, items, requests, first_seen, last_seen, user_agent, host)| Row {
                    ip,
                    client: Client {
                        blocked,
                        items,
                        requests,
                        first_seen,
                        last_seen,
                        user_agent,
                    },
                    host,
                },
            )
    }

    fn arbitrary_view() -> impl Strategy<Value = View> {
        let column = proptest::sample::select(&COLUMNS[..COLUMNS.len() - 1]);
        (column, any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
            |(sort_column, sort_inverted, show_host, show_ua)| View {
                sort_column,
                sort_inverted,
                show_host,
                show_ua,
            },
        )
    }

    fn arbitrary_event() -> impl Strategy<Value = Event> {
        let outcome = prop_oneof![
            any::<u64>().prop_map(Outcome::Lines),
            Just(Outcome::Blocked),
            Just(Outcome::WrongPath),
            Just(Outcome::Paused),
            Just(Outcome::Rejected(405)),
        ];
        (arbitrary_timestamp(), any::<IpAddr>(), outcome).prop_map(|(time, ip, outcome)| Event {
            time,
            ip,
            outcome,
        })
    }

    proptest! {
        #[test]
        fn drawing_never_panics(
            width in 1..=300u16,
            height in 1..=100u16,
            rows in proptest::collection::vec(arbitrary_row(), 0..40),
            events in proptest::collection::vec(arbitrary_event(), 0..40),
            view in arbitrary_view(),
            position in any::<u64>(),
            total in any::<u64>(),
            numbers in any::<(u64, u64, u64, bool)>(),
        ) {
            let (wraps, lines_per_minute, requests_per_minute, paused) = numbers;
            let mut snapshot = Snapshot {
                position,
                total,
                wraps,
                paused,
                view,
                rows,
                events,
                lines_per_minute,
                requests_per_minute,
            };
            snapshot.rows.sort_by(|a, b| compare_rows(&view, a, b));
            draw_snapshot(&snapshot, width, height);
        }
    }
}
