//! Storage primitives: an append-only CommitLog and a fixed-size ConsumeQueue.
//!
//! This is the R1 foundation. It deliberately uses plain `std::fs` positioned
//! IO so it is easy to test on Windows and Linux; the production path will
//! replace the inner read/write loops with `memmap2` + `positioned-io`.

use crc32c::crc32c;
use mq_common::{MqError, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const RECORD_MAGIC: u32 = 0x4D51_5231; // "MQR1"
const RECORD_HEADER_LEN: usize = 12; // magic + len + crc32c
const MAX_RECORD_LEN: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResult {
    pub physical_offset: u64,
    pub wrote: usize,
    pub payload_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub valid_bytes: u64,
    pub records: u64,
    pub truncated: bool,
}

/// Append-only log with CRC-protected, length-prefixed records.
/// Records are rotated across segment files (`<base>`, `<base>.1`, ...).
pub struct CommitLog {
    path: PathBuf,
    segments: Vec<CommitSegment>,
    len: u64,
    base: u64,
    segment_bytes: u64,
}

struct CommitSegment {
    base: u64,
    file: File,
    len: u64,
}

const DEFAULT_COMMITLOG_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

impl CommitLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut log = Self::open_unrecovered(path)?;
        log.recover()?;
        Ok(log)
    }

    pub fn open_with_segment(path: impl AsRef<Path>, segment_bytes: u64) -> Result<Self> {
        let mut log = Self::open_unrecovered_with_segment(path, segment_bytes)?;
        log.recover()?;
        Ok(log)
    }

    /// Open without scanning; useful for tests and for a caller that wants the
    /// `RecoveryReport` from `recover()` itself.
    pub fn open_unrecovered(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_unrecovered_with_segment(path, DEFAULT_COMMITLOG_SEGMENT_BYTES)
    }

    pub fn open_unrecovered_with_segment(
        path: impl AsRef<Path>,
        segment_bytes: u64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let segment_bytes = segment_bytes.max(1);
        let meta_path = PathBuf::from(format!("{}.meta", path.display()));
        let base = std::fs::read_to_string(&meta_path)
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let mut segments = Vec::new();
        let mut len = base;
        for index in 0..100_000usize {
            let segment_path = if index == 0 {
                path.clone()
            } else {
                PathBuf::from(format!("{}.{}", path.display(), index))
            };
            if index > 0 && !segment_path.exists() {
                break;
            }
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&segment_path)?;
            let file_len = file.metadata()?.len();
            segments.push(CommitSegment {
                base: len,
                file,
                len: file_len,
            });
            len += file_len;
        }
        Ok(Self {
            path,
            segments,
            len,
            base,
            segment_bytes,
        })
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bytes physically present across all segments.
    pub fn disk_bytes(&self) -> u64 {
        self.segments.iter().map(|segment| segment.len).sum()
    }

    fn segment_path(&self, index: usize) -> PathBuf {
        if index == 0 {
            self.path.clone()
        } else {
            PathBuf::from(format!("{}.{}", self.path.display(), index))
        }
    }

    fn write_at(file: &mut File, relative: u64, buf: &[u8]) -> Result<()> {
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let mut written = 0;
            while written < buf.len() {
                let n = file.seek_write(&buf[written..], relative + written as u64)?;
                if n == 0 {
                    return Err(MqError::Store("short commitlog write".into()));
                }
                written += n;
            }
        }
        #[cfg(not(windows))]
        {
            file.seek(SeekFrom::Start(relative))?;
            file.write_all(buf)?;
        }
        Ok(())
    }

    /// Append and return the offset where the record starts.
    pub fn append(&mut self, payload: &[u8]) -> Result<AppendResult> {
        if payload.is_empty() {
            return Err(MqError::Store("empty commitlog payload".into()));
        }
        if payload.len() > MAX_RECORD_LEN {
            return Err(MqError::Store(format!(
                "commitlog payload too large: {}",
                payload.len()
            )));
        }
        let record_len = RECORD_HEADER_LEN + payload.len();
        self.ensure_capacity(record_len)?;
        let segment = self
            .segments
            .last_mut()
            .ok_or_else(|| MqError::Store("missing commitlog segment".into()))?;
        let offset = segment.base + segment.len;
        let mut buf = Vec::with_capacity(record_len);
        buf.extend_from_slice(&RECORD_MAGIC.to_be_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&crc32c(payload).to_be_bytes());
        buf.extend_from_slice(payload);
        Self::write_at(&mut segment.file, segment.len, &buf)?;
        segment.len += record_len as u64;
        self.len = offset + record_len as u64;
        Ok(AppendResult {
            physical_offset: offset,
            wrote: record_len,
            payload_len: payload.len(),
        })
    }

    fn ensure_capacity(&mut self, record_len: usize) -> Result<()> {
        let need_new = match self.segments.last() {
            Some(segment) => {
                segment.len > 0 && segment.len + record_len as u64 > self.segment_bytes
            }
            None => true,
        };
        if need_new {
            let index = self.segments.len();
            let path = self.segment_path(index);
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            self.segments.push(CommitSegment {
                base: self.len,
                file,
                len: 0,
            });
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        for segment in &mut self.segments {
            segment.file.sync_data()?;
        }
        Ok(())
    }

    /// Truncate the log to zero and remove rotated segment files.
    pub fn truncate(&mut self) -> Result<()> {
        self.segments.clear();
        for index in 1..100_000usize {
            let path = self.segment_path(index);
            if !path.exists() {
                break;
            }
            let _ = std::fs::remove_file(path);
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        self.segments = vec![CommitSegment {
            base: 0,
            file,
            len: 0,
        }];
        self.len = 0;
        self.base = 0;
        self.persist_base();
        Ok(())
    }

    fn meta_path(&self) -> PathBuf {
        PathBuf::from(format!("{}.meta", self.path.display()))
    }

    fn persist_base(&self) {
        let _ = std::fs::write(self.meta_path(), self.base.to_string());
    }

    /// Delete whole segments ending at or before `cutoff`. The last segment is
    /// always kept; global offsets remain unchanged.
    pub fn trim_prefix(&mut self, cutoff: u64) -> Result<u64> {
        let mut drop_count = 0usize;
        while drop_count + 1 < self.segments.len()
            && self.segments[drop_count].base + self.segments[drop_count].len <= cutoff
        {
            drop_count += 1;
        }
        if drop_count == 0 {
            return Ok(0);
        }
        let freed: u64 = self.segments[..drop_count].iter().map(|s| s.len).sum();
        let old_count = self.segments.len();
        self.segments.clear();
        for index in 0..drop_count {
            let path = if index == 0 {
                self.path.clone()
            } else {
                PathBuf::from(format!("{}.{}", self.path.display(), index))
            };
            let _ = std::fs::remove_file(path);
        }
        for index in drop_count..old_count {
            let from = if index == 0 {
                self.path.clone()
            } else {
                PathBuf::from(format!("{}.{}", self.path.display(), index))
            };
            let to_index = index - drop_count;
            let to = if to_index == 0 {
                self.path.clone()
            } else {
                PathBuf::from(format!("{}.{}", self.path.display(), to_index))
            };
            std::fs::rename(from, to)?;
        }
        self.base += freed;
        self.persist_base();
        self.reopen_segments()?;
        Ok(freed)
    }

    fn reopen_segments(&mut self) -> Result<()> {
        let mut segments = Vec::new();
        let mut len = self.base;
        for index in 0..100_000usize {
            let path = if index == 0 {
                self.path.clone()
            } else {
                PathBuf::from(format!("{}.{}", self.path.display(), index))
            };
            if index > 0 && !path.exists() {
                break;
            }
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            let file_len = file.metadata()?.len();
            segments.push(CommitSegment {
                base: len,
                file,
                len: file_len,
            });
            len += file_len;
        }
        self.segments = segments;
        Ok(())
    }

    /// Append several records. If the batch fits in the current segment it is
    /// written with one syscall; otherwise records are appended one by one.
    pub fn append_batch(&mut self, payloads: &[Vec<u8>]) -> Result<Vec<AppendResult>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let total: usize = payloads
            .iter()
            .map(|payload| RECORD_HEADER_LEN + payload.len())
            .sum();
        let fits = match self.segments.last() {
            Some(segment) => segment.len == 0 || segment.len + total as u64 <= self.segment_bytes,
            None => false,
        };
        if !fits {
            return payloads
                .iter()
                .map(|payload| self.append(payload))
                .collect();
        }
        let mut buf = Vec::with_capacity(total);
        let mut results = Vec::with_capacity(payloads.len());
        let segment = self
            .segments
            .last_mut()
            .ok_or_else(|| MqError::Store("missing commitlog segment".into()))?;
        let mut offset = segment.base + segment.len;
        for payload in payloads {
            if payload.is_empty() {
                return Err(MqError::Store("empty commitlog payload".into()));
            }
            if payload.len() > MAX_RECORD_LEN {
                return Err(MqError::Store(format!(
                    "commitlog payload too large: {}",
                    payload.len()
                )));
            }
            let record_len = RECORD_HEADER_LEN + payload.len();
            buf.extend_from_slice(&RECORD_MAGIC.to_be_bytes());
            buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&crc32c(payload).to_be_bytes());
            buf.extend_from_slice(payload);
            results.push(AppendResult {
                physical_offset: offset,
                wrote: record_len,
                payload_len: payload.len(),
            });
            offset += record_len as u64;
        }
        Self::write_at(&mut segment.file, segment.len, &buf)?;
        segment.len += total as u64;
        self.len = offset;
        Ok(results)
    }

    /// Read one record starting at `offset`.
    pub fn read_at(&mut self, offset: u64) -> Result<Vec<u8>> {
        if offset + RECORD_HEADER_LEN as u64 > self.len {
            return Err(MqError::Store(format!("offset out of range: {offset}")));
        }
        let segment = self
            .segments
            .iter_mut()
            .find(|segment| offset >= segment.base && offset < segment.base + segment.len)
            .ok_or_else(|| MqError::Store(format!("offset not in any segment: {offset}")))?;
        let relative = offset - segment.base;
        segment.file.seek(SeekFrom::Start(relative))?;
        let mut header = [0u8; RECORD_HEADER_LEN];
        segment.file.read_exact(&mut header)?;
        let magic = u32::from_be_bytes(header[0..4].try_into().unwrap());
        if magic != RECORD_MAGIC {
            return Err(MqError::Store(format!("bad record magic at {offset}")));
        }
        let len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
        if len == 0 || len > MAX_RECORD_LEN {
            return Err(MqError::Store(format!(
                "bad record length at {offset}: {len}"
            )));
        }
        let expected_crc = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let mut payload = vec![0u8; len];
        segment.file.read_exact(&mut payload)?;
        let actual_crc = crc32c(&payload);
        if actual_crc != expected_crc {
            return Err(MqError::Store(format!(
                "commitlog crc mismatch at {offset}: expected {expected_crc:#x}, got {actual_crc:#x}"
            )));
        }
        Ok(payload)
    }

    /// Read every complete record, across all segments.
    pub fn scan(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        for segment in &mut self.segments {
            let mut pos = 0u64;
            while pos + RECORD_HEADER_LEN as u64 <= segment.len {
                segment.file.seek(SeekFrom::Start(pos))?;
                let mut header = [0u8; RECORD_HEADER_LEN];
                if segment.file.read_exact(&mut header).is_err() {
                    break;
                }
                let magic = u32::from_be_bytes(header[0..4].try_into().unwrap());
                let len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
                if magic != RECORD_MAGIC || len == 0 || len > MAX_RECORD_LEN {
                    break;
                }
                let mut payload = vec![0u8; len];
                if segment.file.read_exact(&mut payload).is_err() {
                    break;
                }
                out.push(payload);
                pos += RECORD_HEADER_LEN as u64 + len as u64;
            }
        }
        Ok(out)
    }

    /// Scan records and truncate an incomplete / corrupt tail, including
    /// deleting later segments.
    pub fn recover(&mut self) -> Result<RecoveryReport> {
        let mut valid_bytes = 0u64;
        let mut records = 0u64;
        let mut truncated = false;
        let mut keep_segments = self.segments.len();
        for index in 0..self.segments.len() {
            let segment = &mut self.segments[index];
            let file_len = segment.file.metadata()?.len();
            let mut pos = 0u64;
            let mut corrupt = false;
            while pos + RECORD_HEADER_LEN as u64 <= file_len {
                segment.file.seek(SeekFrom::Start(pos))?;
                let mut header = [0u8; RECORD_HEADER_LEN];
                if segment.file.read_exact(&mut header).is_err() {
                    corrupt = true;
                    break;
                }
                let magic = u32::from_be_bytes(header[0..4].try_into().unwrap());
                let len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
                let expected_crc = u32::from_be_bytes(header[8..12].try_into().unwrap());
                if magic != RECORD_MAGIC || len == 0 || len > MAX_RECORD_LEN {
                    corrupt = true;
                    break;
                }
                let end = pos + RECORD_HEADER_LEN as u64 + len as u64;
                if end > file_len {
                    corrupt = true;
                    break;
                }
                let mut payload = vec![0u8; len];
                if segment.file.read_exact(&mut payload).is_err()
                    || crc32c(&payload) != expected_crc
                {
                    corrupt = true;
                    break;
                }
                pos = end;
                records += 1;
            }
            if corrupt || pos != file_len {
                segment.file.set_len(pos)?;
                segment.file.sync_all()?;
                segment.len = pos;
                valid_bytes += pos;
                keep_segments = index + 1;
                truncated = true;
                break;
            }
            valid_bytes += pos;
        }
        if keep_segments < self.segments.len() {
            for index in keep_segments..self.segments.len() {
                let path = self.segment_path(index);
                let _ = std::fs::remove_file(path);
            }
            self.segments.truncate(keep_segments);
        }
        self.len = self.base + valid_bytes;
        let mut base = self.base;
        for segment in &mut self.segments {
            segment.base = base;
            base += segment.len;
        }
        Ok(RecoveryReport {
            valid_bytes,
            records,
            truncated,
        })
    }
}

const CQ_ENTRY_LEN: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CqEntry {
    pub commitlog_offset: i64,
    pub msg_size: i32,
    pub tag_hash: i64,
}

/// Fixed 20-byte index entries pointing into the CommitLog.
/// Entries are rotated across segment files (`<base>`, `<base>.1`, ...).
pub struct ConsumeQueue {
    path: PathBuf,
    segments: Vec<File>,
    entries: u64,
    segment_entries: u64,
}

const CQ_SEGMENT_ENTRIES: u64 = 100_000;

impl ConsumeQueue {
    fn segment_path(&self, index: usize) -> PathBuf {
        if index == 0 {
            self.path.clone()
        } else {
            PathBuf::from(format!("{}.{}", self.path.display(), index))
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_segment(path, CQ_SEGMENT_ENTRIES)
    }

    pub fn open_with_segment(path: impl AsRef<Path>, segment_entries: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let segment_entries = segment_entries.max(1);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut segments = Vec::new();
        let mut entries = 0u64;
        for index in 0..100_000usize {
            let segment = if index == 0 {
                path.clone()
            } else {
                PathBuf::from(format!("{}.{}", path.display(), index))
            };
            if index > 0 && !segment.exists() {
                break;
            }
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&segment)?;
            entries += file.metadata()?.len() / CQ_ENTRY_LEN as u64;
            segments.push(file);
        }
        Ok(Self {
            path,
            segments,
            entries,
            segment_entries,
        })
    }

    pub fn len(&self) -> u64 {
        self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    pub fn append(&mut self, entry: CqEntry) -> Result<u64> {
        let index = self.entries;
        let segment = (self.entries / self.segment_entries) as usize;
        while self.segments.len() <= segment {
            let path = self.segment_path(self.segments.len());
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            self.segments.push(file);
        }
        let file = &mut self.segments[segment];
        file.seek(SeekFrom::End(0))?;
        let mut buf = [0u8; CQ_ENTRY_LEN];
        buf[0..8].copy_from_slice(&entry.commitlog_offset.to_be_bytes());
        buf[8..12].copy_from_slice(&entry.msg_size.to_be_bytes());
        buf[12..20].copy_from_slice(&entry.tag_hash.to_be_bytes());
        file.write_all(&buf)?;
        self.entries += 1;
        Ok(index)
    }

    pub fn sync(&mut self) -> Result<()> {
        for file in &mut self.segments {
            file.sync_data()?;
        }
        Ok(())
    }

    /// Truncate all segments and remove rotated files.
    pub fn truncate(&mut self) -> Result<()> {
        let extra = self.segments.len();
        self.segments.clear();
        for index in 1..extra {
            let path = if index == 0 {
                self.path.clone()
            } else {
                PathBuf::from(format!("{}.{}", self.path.display(), index))
            };
            let _ = std::fs::remove_file(path);
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)?;
        file.set_len(0)?;
        self.segments.push(file);
        self.entries = 0;
        Ok(())
    }

    /// Drop the first `count` entries, deleting whole segments where possible.
    pub fn drop_prefix(&mut self, count: u64) -> Result<()> {
        if count == 0 {
            return Ok(());
        }
        if count >= self.entries {
            return self.truncate();
        }
        if !count.is_multiple_of(self.segment_entries) {
            let remaining = self.read(count, usize::MAX)?;
            self.truncate()?;
            for entry in remaining {
                self.append(entry)?;
            }
            return Ok(());
        }
        let drop_segments = (count / self.segment_entries) as usize;
        if drop_segments > 0 {
            let old_count = self.segments.len();
            self.segments.clear();
            for index in 0..drop_segments {
                let path = if index == 0 {
                    self.path.clone()
                } else {
                    PathBuf::from(format!("{}.{}", self.path.display(), index))
                };
                let _ = std::fs::remove_file(path);
            }
            for index in drop_segments..old_count {
                let from = if index == 0 {
                    self.path.clone()
                } else {
                    PathBuf::from(format!("{}.{}", self.path.display(), index))
                };
                let to_index = index - drop_segments;
                let to = if to_index == 0 {
                    self.path.clone()
                } else {
                    PathBuf::from(format!("{}.{}", self.path.display(), to_index))
                };
                std::fs::rename(from, to)?;
            }
            self.reopen_segments()?;
            self.entries = self
                .entries
                .saturating_sub(drop_segments as u64 * self.segment_entries);
        }
        Ok(())
    }

    fn reopen_segments(&mut self) -> Result<()> {
        let mut segments = Vec::new();
        for index in 0..100_000usize {
            let path = if index == 0 {
                self.path.clone()
            } else {
                PathBuf::from(format!("{}.{}", self.path.display(), index))
            };
            if index > 0 && !path.exists() {
                break;
            }
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            segments.push(file);
        }
        self.segments = segments;
        Ok(())
    }

    pub fn read(&mut self, start: u64, max: usize) -> Result<Vec<CqEntry>> {
        if start >= self.entries || max == 0 {
            return Ok(Vec::new());
        }
        let count = ((self.entries - start) as usize).min(max);
        let mut out = Vec::with_capacity(count);
        let mut index = start;
        let mut remaining = count;
        let mut buf = [0u8; CQ_ENTRY_LEN];
        while remaining > 0 {
            let segment = (index / self.segment_entries) as usize;
            let offset = (index % self.segment_entries) * CQ_ENTRY_LEN as u64;
            let in_segment = (self.segment_entries - index % self.segment_entries) as usize;
            let take = remaining.min(in_segment);
            let file = self
                .segments
                .get_mut(segment)
                .ok_or_else(|| MqError::Store(format!("missing CQ segment {segment}")))?;
            file.seek(SeekFrom::Start(offset))?;
            for _ in 0..take {
                file.read_exact(&mut buf)?;
                out.push(CqEntry {
                    commitlog_offset: i64::from_be_bytes(buf[0..8].try_into().unwrap()),
                    msg_size: i32::from_be_bytes(buf[8..12].try_into().unwrap()),
                    tag_hash: i64::from_be_bytes(buf[12..20].try_into().unwrap()),
                });
            }
            index += take as u64;
            remaining -= take;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("mq-rust-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn commitlog_trim_prefix_keeps_offsets() {
        let dir = temp_dir("commitlog-trim");
        let path = dir.join("commitlog");
        let mut log = CommitLog::open_with_segment(&path, 64).unwrap();
        let mut offsets = Vec::new();
        for i in 0..10 {
            offsets.push(log.append(&[i as u8; 20]).unwrap().physical_offset);
        }
        let before_len = log.len();
        let freed = log.trim_prefix(offsets[6]).unwrap();
        assert!(freed > 0);
        assert_eq!(log.len(), before_len);
        assert!(log.read_at(offsets[0]).is_err());
        assert_eq!(log.read_at(offsets[6]).unwrap(), vec![6u8; 20]);
        assert_eq!(log.scan().unwrap().len(), 4);
        let new_offset = log.append(&[99u8; 20]).unwrap().physical_offset;
        assert_eq!(new_offset, before_len);
        drop(log);
        let mut reopened = CommitLog::open_with_segment(&path, 64).unwrap();
        assert_eq!(reopened.scan().unwrap().len(), 5);
        assert_eq!(reopened.read_at(new_offset).unwrap(), vec![99u8; 20]);
    }

    #[test]
    fn commitlog_rotates_segments() {
        let dir = temp_dir("commitlog-segments");
        let path = dir.join("commitlog");
        let mut log = CommitLog::open_with_segment(&path, 64).unwrap();
        let mut offsets = Vec::new();
        for i in 0..10 {
            let payload = vec![i as u8; 20];
            offsets.push(log.append(&payload).unwrap().physical_offset);
        }
        assert!(dir.join("commitlog.1").exists());
        for (i, offset) in offsets.iter().enumerate() {
            assert_eq!(log.read_at(*offset).unwrap(), vec![i as u8; 20]);
        }
        assert_eq!(log.scan().unwrap().len(), 10);
        drop(log);
        let mut reopened = CommitLog::open_with_segment(&path, 64).unwrap();
        assert_eq!(reopened.scan().unwrap().len(), 10);
    }

    #[test]
    fn commitlog_append_and_read() {
        let dir = temp_dir("commitlog");
        let mut log = CommitLog::open(dir.join("commitlog")).unwrap();
        let a = log.append(b"first").unwrap();
        let b = log.append(b"second").unwrap();
        assert_eq!(log.read_at(a.physical_offset).unwrap(), b"first");
        assert_eq!(log.read_at(b.physical_offset).unwrap(), b"second");
        assert_eq!(log.len(), (a.wrote + b.wrote) as u64);
    }

    #[test]
    fn commitlog_append_batch() {
        let dir = temp_dir("commitlog-batch");
        let mut log = CommitLog::open(dir.join("commitlog")).unwrap();
        let results = log
            .append_batch(&[b"one".to_vec(), b"two".to_vec(), b"three".to_vec()])
            .unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(log.read_at(results[0].physical_offset).unwrap(), b"one");
        assert_eq!(log.read_at(results[1].physical_offset).unwrap(), b"two");
        assert_eq!(log.read_at(results[2].physical_offset).unwrap(), b"three");
    }

    #[test]
    fn commitlog_recovery_truncates_bad_tail() {
        let dir = temp_dir("recover");
        let path = dir.join("commitlog");
        {
            let mut log = CommitLog::open(&path).unwrap();
            log.append(b"ok-1").unwrap();
            log.append(b"ok-2").unwrap();
        }
        // Append a partial record without a valid header.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0x00, 0x01, 0x02]).unwrap();
        drop(f);

        let mut log = CommitLog::open_unrecovered(&path).unwrap();
        let report = log.recover().unwrap();
        assert!(report.truncated);
        assert_eq!(report.records, 2);
        assert!(log.read_at(0).is_ok());
    }

    #[test]
    fn consumequeue_drop_prefix_unaligned() {
        let dir = temp_dir("cq-drop-unaligned");
        let path = dir.join("0.cq");
        let mut cq = ConsumeQueue::open_with_segment(&path, 4).unwrap();
        for i in 0..10 {
            cq.append(CqEntry {
                commitlog_offset: i,
                msg_size: 1,
                tag_hash: 0,
            })
            .unwrap();
        }
        cq.drop_prefix(5).unwrap();
        let entries = cq.read(0, 10).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].commitlog_offset, 5);
        assert_eq!(entries[4].commitlog_offset, 9);
    }

    #[test]
    fn consumequeue_drop_prefix_across_segments() {
        let dir = temp_dir("cq-drop");
        let path = dir.join("0.cq");
        let mut cq = ConsumeQueue::open_with_segment(&path, 4).unwrap();
        for i in 0..10i64 {
            cq.append(CqEntry {
                commitlog_offset: i,
                msg_size: 1,
                tag_hash: 0,
            })
            .unwrap();
        }
        cq.drop_prefix(4).unwrap();
        assert_eq!(cq.len(), 6);
        let read = cq.read(0, 6).unwrap();
        assert_eq!(read.len(), 6);
        assert_eq!(read[0].commitlog_offset, 4);
        assert_eq!(read[5].commitlog_offset, 9);
    }

    #[test]
    fn consumequeue_rotates_segments() {
        let dir = temp_dir("cq-segments");
        let path = dir.join("0.cq");
        let mut cq = ConsumeQueue::open_with_segment(&path, 4).unwrap();
        for i in 0..10i64 {
            cq.append(CqEntry {
                commitlog_offset: i,
                msg_size: 1,
                tag_hash: 0,
            })
            .unwrap();
        }
        assert_eq!(cq.len(), 10);
        assert!(dir.join("0.cq.1").exists());
        assert!(dir.join("0.cq.2").exists());
        let read = cq.read(3, 5).unwrap();
        assert_eq!(read.len(), 5);
        assert_eq!(read[0].commitlog_offset, 3);
        assert_eq!(read[4].commitlog_offset, 7);
        cq.truncate().unwrap();
        assert_eq!(cq.len(), 0);
        assert!(!dir.join("0.cq.1").exists());
        drop(cq);
        let reopened = ConsumeQueue::open(&path).unwrap();
        assert_eq!(reopened.len(), 0);
    }

    #[test]
    fn consumequeue_append_and_read() {
        let dir = temp_dir("cq");
        let mut cq = ConsumeQueue::open(dir.join("cq")).unwrap();
        cq.append(CqEntry {
            commitlog_offset: 100,
            msg_size: 32,
            tag_hash: 7,
        })
        .unwrap();
        cq.append(CqEntry {
            commitlog_offset: 200,
            msg_size: 64,
            tag_hash: 8,
        })
        .unwrap();
        let entries = cq.read(0, 10).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].commitlog_offset, 100);
        assert_eq!(entries[1].msg_size, 64);
    }
}
