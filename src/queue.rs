//! The item file, read one line at a time, and the position of the next line to hand out.

use anyhow::{Context, Result, bail};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// The lines taken by one `Queue::take`.
#[derive(Debug, PartialEq, Eq)]
pub struct Batch {
    /// Each line taken, followed by `\n`.
    pub body: Vec<u8>,
    /// The number of lines in `body`.
    pub lines: u64,
    /// Whether this take reached the end of the file and started a new pass at the first line.
    pub wrapped: bool,
}

/// Opens the item file at `path` and scans it once, from its start.
///
/// `position` is the 0-based index of the line that the returned reader should read next.
///
/// Returns a reader that is ready to read the line at `position`, and the number of lines in
/// the file.
fn open_at(path: &Path, position: u64) -> Result<(BufReader<File>, u64)> {
    let file = File::open(path)
        .with_context(|| format!("cannot open the item file {}", path.display()))?;
    let context = || format!("cannot read the item file {}", path.display());
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut count = 0;
    let mut offset = 0;
    let mut position_offset = if position == 0 { Some(0) } else { None };
    loop {
        let length = reader.skip_until(b'\n').with_context(context)?;
        if length == 0 {
            break;
        }
        count += 1;
        offset += length as u64;
        if count == position {
            position_offset = Some(offset);
        }
    }
    let Some(position_offset) = position_offset else {
        bail!(
            "the position {position} is beyond the end of the item file {}, which has {count} lines",
            path.display()
        );
    };
    reader
        .seek(SeekFrom::Start(position_offset))
        .with_context(context)?;
    Ok((reader, count))
}

/// The lines of the item file, handed out in file order, one pass after another.
pub struct Queue {
    path: PathBuf,
    reader: BufReader<File>,
    /// The number of lines already handed out in this pass, which is the 0-based index of the
    /// next line to hand out.
    position: u64,
    /// The number of lines in the file, as of the last scan or the last time the end was reached.
    total: u64,
    /// Whether to start a new pass after the end of the file is reached.
    cycle: bool,
}

impl Queue {
    /// Opens the item file at `path` so that the next line handed out is the line at index
    /// `position`.
    ///
    /// `cycle` says whether to start a new pass at the first line after the end of the file is
    /// reached; without it, every take after the end is empty.
    pub fn open(path: &Path, position: u64, cycle: bool) -> Result<Queue> {
        let (reader, total) = open_at(path, position)?;
        Ok(Queue {
            path: path.to_owned(),
            reader,
            position,
            total,
            cycle,
        })
    }

    /// The number of lines already handed out in this pass.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The number of lines in the item file.
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Takes the next lines.
    ///
    /// `n` is the most lines to take; it must be at least 1.
    ///
    /// The end of the file is reached only when a read finds no line, so a take that gets
    /// exactly the last line has not reached it. A take that reaches it returns the lines it
    /// read before (possibly none), and with cycling on (and a position above 0), it starts a
    /// new pass, opening the item file again by its path; the next take then reads the first
    /// line.
    pub fn take(&mut self, n: u64) -> Result<Batch> {
        assert!(n >= 1, "n must be at least 1, but is {n}");
        let mut body = Vec::new();
        let mut lines = 0;
        let mut reached_end = false;
        while lines < n {
            let length = self
                .reader
                .read_until(b'\n', &mut body)
                .with_context(|| format!("cannot read the item file {}", self.path.display()))?;
            if length == 0 {
                reached_end = true;
                break;
            }
            if body.last() != Some(&b'\n') {
                body.push(b'\n');
            }
            lines += 1;
            self.position += 1;
        }

        let mut wrapped = false;
        if reached_end {
            self.total = self.position;
            if self.cycle && self.position > 0 {
                (self.reader, self.total) = open_at(&self.path, 0)?;
                self.position = 0;
                wrapped = true;
            }
        }
        Ok(Batch {
            body,
            lines,
            wrapped,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::fs;
    use tempfile::TempDir;

    /// Writes `contents` to a new item file in a new temporary directory.
    ///
    /// Returns the directory, which is removed when dropped, and the path of the item file.
    fn item_file(contents: &[u8]) -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("items.txt");
        fs::write(&path, contents).unwrap();
        (dir, path)
    }

    fn batch(body: &[u8], lines: u64, wrapped: bool) -> Batch {
        Batch {
            body: body.to_vec(),
            lines,
            wrapped,
        }
    }

    #[test]
    fn batches_in_file_order() {
        let (_dir, path) = item_file(b"a\nb\nc\nd\ne\n");
        let mut queue = Queue::open(&path, 0, false).unwrap();
        assert_eq!(queue.total(), 5);
        assert_eq!(queue.take(2).unwrap(), batch(b"a\nb\n", 2, false));
        assert_eq!(queue.take(1).unwrap(), batch(b"c\n", 1, false));
        assert_eq!(queue.position(), 3);
        // A short batch at the end of the file
        assert_eq!(queue.take(5).unwrap(), batch(b"d\ne\n", 2, false));
        assert_eq!(queue.position(), 5);
    }

    #[test]
    fn taking_exactly_the_last_line_does_not_reach_the_end() {
        let (_dir, path) = item_file(b"a\nb\n");
        let mut queue = Queue::open(&path, 0, true).unwrap();
        assert_eq!(queue.take(2).unwrap(), batch(b"a\nb\n", 2, false));
        assert_eq!(queue.position(), 2);
        assert_eq!(queue.take(2).unwrap(), batch(b"", 0, true));
        assert_eq!(queue.position(), 0);
        assert_eq!(queue.take(1).unwrap(), batch(b"a\n", 1, false));
    }

    #[test]
    fn a_take_that_reaches_the_end_returns_what_it_read_and_wraps() {
        let (_dir, path) = item_file(b"a\nb\nc\n");
        let mut queue = Queue::open(&path, 0, true).unwrap();
        assert_eq!(queue.take(2).unwrap(), batch(b"a\nb\n", 2, false));
        assert_eq!(queue.take(2).unwrap(), batch(b"c\n", 1, true));
        assert_eq!(queue.take(3).unwrap(), batch(b"a\nb\nc\n", 3, false));
        // Asking for more lines than are left reaches the end
        assert_eq!(queue.take(5).unwrap(), batch(b"", 0, true));
        assert_eq!(queue.take(5).unwrap(), batch(b"a\nb\nc\n", 3, true));
    }

    #[test]
    fn no_wrap_with_cycling_off() {
        let (_dir, path) = item_file(b"a\nb\n");
        let mut queue = Queue::open(&path, 0, false).unwrap();
        assert_eq!(queue.take(5).unwrap(), batch(b"a\nb\n", 2, false));
        for _ in 0..3 {
            assert_eq!(queue.take(5).unwrap(), batch(b"", 0, false));
            assert_eq!(queue.position(), 2);
        }
    }

    #[test]
    fn last_line_without_newline() {
        let (_dir, path) = item_file(b"a\nb");
        let mut queue = Queue::open(&path, 0, false).unwrap();
        assert_eq!(queue.total(), 2);
        assert_eq!(queue.take(5).unwrap(), batch(b"a\nb\n", 2, false));

        // The position at the end of such a file is valid too
        let mut queue = Queue::open(&path, 2, false).unwrap();
        assert_eq!(queue.take(5).unwrap(), batch(b"", 0, false));
    }

    #[test]
    fn empty_lines_and_carriage_returns_are_items() {
        let (_dir, path) = item_file(b"\n\na\r\n\n");
        let mut queue = Queue::open(&path, 0, false).unwrap();
        assert_eq!(queue.total(), 4);
        assert_eq!(queue.take(2).unwrap(), batch(b"\n\n", 2, false));
        assert_eq!(queue.take(5).unwrap(), batch(b"a\r\n\n", 2, false));
    }

    #[test]
    fn empty_file() {
        let (_dir, path) = item_file(b"");
        let mut queue = Queue::open(&path, 0, true).unwrap();
        assert_eq!(queue.total(), 0);
        // No wrap at position 0, even with cycling on
        assert_eq!(queue.take(5).unwrap(), batch(b"", 0, false));
        assert_eq!(queue.take(5).unwrap(), batch(b"", 0, false));
    }

    #[test]
    fn bytes_that_are_not_utf8_are_handed_out_unchanged() {
        let (_dir, path) = item_file(b"\xff\xfe\n\xc3(\n");
        let mut queue = Queue::open(&path, 0, false).unwrap();
        assert_eq!(
            queue.take(5).unwrap(),
            batch(b"\xff\xfe\n\xc3(\n", 2, false)
        );
    }

    #[test]
    fn a_replaced_item_file_is_served_from_the_next_pass_on() {
        let (dir, path) = item_file(b"a\nb\n");
        let mut queue = Queue::open(&path, 0, true).unwrap();
        assert_eq!(queue.take(1).unwrap(), batch(b"a\n", 1, false));

        let new_path = dir.path().join("new.txt");
        fs::write(&new_path, b"x\ny\nz\n").unwrap();
        fs::rename(&new_path, &path).unwrap();

        assert_eq!(queue.take(5).unwrap(), batch(b"b\n", 1, true));
        assert_eq!(queue.total(), 3);
        assert_eq!(queue.take(3).unwrap(), batch(b"x\ny\nz\n", 3, false));
    }

    #[test]
    fn restart_at_the_reported_position() {
        let (_dir, path) = item_file(b"a\nb\nc\nd\n");
        let mut queue = Queue::open(&path, 0, false).unwrap();
        assert_eq!(queue.take(3).unwrap(), batch(b"a\nb\nc\n", 3, false));
        let mut queue = Queue::open(&path, queue.position(), false).unwrap();
        assert_eq!(queue.take(5).unwrap(), batch(b"d\n", 1, false));
    }

    #[test]
    fn a_position_beyond_the_end_is_an_error() {
        let (_dir, path) = item_file(b"a\nb\n");
        let error = Queue::open(&path, 3, false).err().unwrap();
        assert!(
            error.to_string().contains("position 3 is beyond the end"),
            "{error:#}"
        );
    }

    /// Splits `contents` into the lines of an item file, without their `\n`.
    fn lines_of(contents: &[u8]) -> Vec<&[u8]> {
        let mut lines: Vec<&[u8]> = contents.split(|&b| b == b'\n').collect();
        if lines.last() == Some(&&b""[..]) {
            lines.pop();
        }
        lines
    }

    proptest! {
        /// The batches taken before the first wrap, across restarts, are the file's lines in order.
        #[test]
        fn batches_across_restarts_are_the_lines_in_order(
            contents in proptest::collection::vec(prop_oneof![Just(b'\n'), Just(b'a'), any::<u8>()], 0..40),
            steps in proptest::collection::vec((1..6u64, any::<bool>()), 1..30),
        ) {
            let (_dir, path) = item_file(&contents);
            let expected: Vec<u8> = lines_of(&contents)
                .iter()
                .flat_map(|line| [*line, b"\n"].concat())
                .collect();

            let mut queue = Queue::open(&path, 0, true).unwrap();
            let mut taken = Vec::new();
            let mut wrapped = false;
            for (n, restart) in steps {
                let batch = queue.take(n).unwrap();
                taken.extend_from_slice(&batch.body);
                if batch.wrapped {
                    wrapped = true;
                    break;
                }
                if restart {
                    queue = Queue::open(&path, queue.position(), true).unwrap();
                }
            }
            if wrapped {
                prop_assert_eq!(&taken, &expected);
            } else {
                prop_assert!(expected.starts_with(&taken));
            }
        }
    }
}
