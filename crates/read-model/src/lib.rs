//! Rebuildable, checksummed local read-model projection.
//!
//! The journal remains authoritative. This projection is an explicitly
//! versioned query cache that can be deleted and rebuilt after restart.

#![forbid(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use insider_journal::{Journal, JournalError, Record};

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "read_model";
const MAGIC: &[u8; 4] = b"ITRM";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 8 + 4;
const TRAILER_LEN: usize = 4;
const MAX_RECORDS: usize = 1_000_000;
const MAX_PAYLOAD: usize = 16 * 1024 * 1024;
const MAX_PROJECTION_BYTES: u64 = 256 * 1024 * 1024;

/// Rebuildable projection record retaining the source journal cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionRecord {
    /// Source journal sequence.
    pub sequence: u64,
    /// Opaque normalized projection payload.
    pub payload: Vec<u8>,
}

/// Read-model failures.
#[derive(Debug)]
pub enum ReadModelError {
    /// Filesystem failure.
    Io(std::io::Error),
    /// Source journal failure.
    Journal(JournalError),
    /// Projection framing/checksum/version failure.
    Corrupt(&'static str),
    /// A bounded projection input was exceeded.
    Bounds(&'static str),
}

impl From<std::io::Error> for ReadModelError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<JournalError> for ReadModelError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

/// On-disk projection with atomic rebuild and cursor queries.
pub struct ProjectionStore {
    path: PathBuf,
}

/// Result of a projection backup or restore verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionManifest {
    /// Number of verified records.
    pub record_count: u64,
    /// Highest source journal sequence, or zero for an empty projection.
    pub newest_sequence: u64,
}

impl ProjectionStore {
    /// Opens a projection path without mutating it.
    #[must_use]
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    /// Returns the projection path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Rebuilds the projection from a journal scan and atomically publishes it.
    /// Existing readers continue seeing the previous file until the rename.
    ///
    /// # Errors
    /// Returns [`ReadModelError`] for journal, filesystem, bounds, or source
    /// ordering failures.
    pub fn rebuild_from_journal(&self, journal: &Journal) -> Result<u64, ReadModelError> {
        let records = journal.scan()?.records;
        self.rebuild(&records)
    }

    /// Rebuilds from validated source records and returns the newest cursor.
    ///
    /// # Errors
    /// Returns [`ReadModelError`] when records exceed bounds, are not ordered,
    /// or the atomic publication fails.
    pub fn rebuild(&self, records: &[Record]) -> Result<u64, ReadModelError> {
        if records.len() > MAX_RECORDS {
            return Err(ReadModelError::Bounds("projection record count"));
        }
        let temp = self.path.with_extension("rebuild.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)?;
        file.write_all(MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        file.write_all(&0_u64.to_le_bytes())?;
        file.write_all(
            &(u32::try_from(records.len())
                .map_err(|_| ReadModelError::Bounds("projection record count"))?)
            .to_le_bytes(),
        )?;
        let mut cursor = 0_u64;
        let mut output_bytes = HEADER_LEN as u64;
        for record in records {
            if record.payload.len() > MAX_PAYLOAD {
                return Err(ReadModelError::Bounds("projection payload"));
            }
            if record.sequence <= cursor && cursor != 0 {
                return Err(ReadModelError::Corrupt("non-monotonic source cursor"));
            }
            cursor = record.sequence;
            let mut frame = Vec::with_capacity(HEADER_LEN + record.payload.len() + TRAILER_LEN);
            frame.extend_from_slice(MAGIC);
            frame.extend_from_slice(&VERSION.to_le_bytes());
            frame.extend_from_slice(&record.sequence.to_le_bytes());
            frame.extend_from_slice(
                &(u32::try_from(record.payload.len())
                    .map_err(|_| ReadModelError::Bounds("projection payload"))?)
                .to_le_bytes(),
            );
            frame.extend_from_slice(&record.payload);
            frame.extend_from_slice(&crc32c(&frame).to_le_bytes());
            output_bytes = output_bytes
                .checked_add(
                    u64::try_from(frame.len())
                        .map_err(|_| ReadModelError::Bounds("projection bytes"))?,
                )
                .ok_or(ReadModelError::Bounds("projection bytes"))?;
            if output_bytes > MAX_PROJECTION_BYTES {
                return Err(ReadModelError::Bounds("projection bytes"));
            }
            file.write_all(&frame)?;
        }
        file.sync_all()?;
        std::fs::rename(&temp, &self.path)?;
        Ok(records.last().map_or(0, |record| record.sequence))
    }

    /// Appends one journal record to an existing projection without rewriting
    /// prior frames. The frame is synced before the header count is advanced;
    /// an interrupted update is therefore detected and repaired by rebuild.
    ///
    /// # Errors
    /// Returns [`ReadModelError`] for missing/corrupt projections, a sequence
    /// gap, oversized payloads, or filesystem failure.
    pub fn append_record(&self, record: &Record) -> Result<(), ReadModelError> {
        if record.payload.len() > MAX_PAYLOAD {
            return Err(ReadModelError::Bounds("projection payload"));
        }
        let existing = self.read_all()?;
        if existing
            .last()
            .is_some_and(|previous| record.sequence <= previous.sequence)
        {
            return Err(ReadModelError::Corrupt("non-monotonic projection append"));
        }
        if existing.len() >= MAX_RECORDS {
            return Err(ReadModelError::Bounds("projection record count"));
        }
        let mut frame = Vec::with_capacity(HEADER_LEN + record.payload.len() + TRAILER_LEN);
        frame.extend_from_slice(MAGIC);
        frame.extend_from_slice(&VERSION.to_le_bytes());
        frame.extend_from_slice(&record.sequence.to_le_bytes());
        frame.extend_from_slice(
            &u32::try_from(record.payload.len())
                .map_err(|_| ReadModelError::Bounds("projection payload"))?
                .to_le_bytes(),
        );
        frame.extend_from_slice(&record.payload);
        frame.extend_from_slice(&crc32c(&frame).to_le_bytes());
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        file.seek(SeekFrom::End(0))?;
        let projected_size = file
            .metadata()?
            .len()
            .checked_add(
                u64::try_from(frame.len())
                    .map_err(|_| ReadModelError::Bounds("projection bytes"))?,
            )
            .ok_or(ReadModelError::Bounds("projection bytes"))?;
        if projected_size > MAX_PROJECTION_BYTES {
            return Err(ReadModelError::Bounds("projection bytes"));
        }
        file.write_all(&frame)?;
        file.sync_data()?;
        file.seek(SeekFrom::Start(14))?;
        file.write_all(
            &u32::try_from(existing.len() + 1)
                .map_err(|_| ReadModelError::Bounds("projection record count"))?
                .to_le_bytes(),
        )?;
        file.sync_all()?;
        Ok(())
    }

    /// Reads and verifies the complete projection.
    ///
    /// # Errors
    /// Returns [`ReadModelError`] if the file is missing, malformed, corrupt,
    /// out of version, or exceeds configured bounds.
    pub fn read_all(&self) -> Result<Vec<ProjectionRecord>, ReadModelError> {
        let file = File::open(&self.path)?;
        if file.metadata()?.len() > MAX_PROJECTION_BYTES {
            return Err(ReadModelError::Bounds("projection bytes"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_PROJECTION_BYTES.saturating_add(1))
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_PROJECTION_BYTES {
            return Err(ReadModelError::Bounds("projection bytes"));
        }
        if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
            return Err(ReadModelError::Corrupt("projection header"));
        }
        let version = u16::from_le_bytes(
            bytes[4..6]
                .try_into()
                .map_err(|_| ReadModelError::Corrupt("projection version"))?,
        );
        if version != VERSION {
            return Err(ReadModelError::Corrupt("projection version"));
        }
        let count = usize::try_from(u32::from_le_bytes(
            bytes[14..18]
                .try_into()
                .map_err(|_| ReadModelError::Corrupt("projection count"))?,
        ))
        .map_err(|_| ReadModelError::Bounds("projection count"))?;
        if count > MAX_RECORDS {
            return Err(ReadModelError::Bounds("projection count"));
        }
        let mut cursor = HEADER_LEN;
        let mut output = Vec::with_capacity(count);
        let mut previous = None;
        while cursor < bytes.len() {
            let start = cursor;
            if bytes.len() - cursor < HEADER_LEN + TRAILER_LEN
                || &bytes[cursor..cursor + 4] != MAGIC
            {
                return Err(ReadModelError::Corrupt("projection frame"));
            }
            let sequence = u64::from_le_bytes(
                bytes[cursor + 6..cursor + 14]
                    .try_into()
                    .map_err(|_| ReadModelError::Corrupt("projection sequence"))?,
            );
            let length = usize::try_from(u32::from_le_bytes(
                bytes[cursor + 14..cursor + 18]
                    .try_into()
                    .map_err(|_| ReadModelError::Corrupt("projection length"))?,
            ))
            .map_err(|_| ReadModelError::Bounds("projection length"))?;
            if length > MAX_PAYLOAD
                || cursor
                    .checked_add(HEADER_LEN + length + TRAILER_LEN)
                    .is_none()
            {
                return Err(ReadModelError::Bounds("projection frame"));
            }
            let end = cursor + HEADER_LEN + length + TRAILER_LEN;
            let expected = u32::from_le_bytes(
                bytes[end - 4..end]
                    .try_into()
                    .map_err(|_| ReadModelError::Corrupt("projection checksum"))?,
            );
            if crc32c(&bytes[start..end - 4]) != expected
                || previous.is_some_and(|last| sequence <= last)
            {
                return Err(ReadModelError::Corrupt("projection checksum/order"));
            }
            output.push(ProjectionRecord {
                sequence,
                payload: bytes[cursor + HEADER_LEN..end - 4].to_vec(),
            });
            previous = Some(sequence);
            cursor = end;
        }
        if output.len() != count {
            return Err(ReadModelError::Corrupt("projection count mismatch"));
        }
        Ok(output)
    }

    /// Copies a verified projection to a new destination and atomically publishes it.
    /// Existing destinations are rejected to prevent silent evidence overwrite.
    ///
    /// # Errors
    /// Returns an error when the source is invalid, the destination exists, or
    /// the filesystem cannot publish the copy.
    pub fn backup_to(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<ProjectionManifest, ReadModelError> {
        let records = self.read_all()?;
        let destination = destination.as_ref();
        if destination.exists() {
            return Err(ReadModelError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "projection backup destination exists",
            )));
        }
        let temp = destination.with_extension("backup.tmp");
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        let source = File::open(&self.path)?;
        std::io::copy(&mut std::io::BufReader::new(source), &mut output)?;
        output.sync_all()?;
        std::fs::rename(&temp, destination)?;
        Ok(ProjectionManifest {
            record_count: records.len() as u64,
            newest_sequence: records.last().map_or(0, |record| record.sequence),
        })
    }

    /// Verifies a projection backup and atomically restores it to a new path.
    ///
    /// # Errors
    /// Returns an error when the source is invalid, the destination exists, or
    /// the filesystem cannot publish the restored copy.
    pub fn restore_from(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<ProjectionManifest, ReadModelError> {
        let source = source.as_ref();
        let destination = destination.as_ref();
        let verified = Self::new(source).read_all()?;
        if destination.exists() {
            return Err(ReadModelError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "projection restore destination exists",
            )));
        }
        let temp = destination.with_extension("restore.tmp");
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        let mut input = File::open(source)?;
        std::io::copy(&mut input, &mut output)?;
        output.sync_all()?;
        std::fs::rename(&temp, destination)?;
        Ok(ProjectionManifest {
            record_count: verified.len() as u64,
            newest_sequence: verified.last().map_or(0, |record| record.sequence),
        })
    }
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82f6_3b78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("insider-read-model-{label}-{nonce}"))
    }

    #[test]
    fn backup_and_restore_verify_cursor_and_count() {
        let source = temp_path("source");
        let backup = temp_path("backup");
        let restored = temp_path("restored");
        let store = ProjectionStore::new(&source);
        store
            .rebuild(&[
                Record {
                    sequence: 4,
                    payload: b"a".to_vec(),
                },
                Record {
                    sequence: 9,
                    payload: b"b".to_vec(),
                },
            ])
            .expect("rebuild");
        let manifest = store.backup_to(&backup).expect("backup");
        assert_eq!(manifest.record_count, 2);
        assert_eq!(manifest.newest_sequence, 9);
        let restored_manifest = ProjectionStore::restore_from(&backup, &restored).expect("restore");
        assert_eq!(restored_manifest, manifest);
        assert_eq!(
            ProjectionStore::new(&restored).read_all().expect("read"),
            store.read_all().expect("read")
        );
        let _ = std::fs::remove_file(source);
        let _ = std::fs::remove_file(backup);
        let _ = std::fs::remove_file(restored);
    }

    #[test]
    fn oversized_projection_is_rejected_before_buffering() {
        let path = temp_path("oversized");
        let file = File::create(&path).expect("create sparse projection");
        file.set_len(MAX_PROJECTION_BYTES + 1)
            .expect("set sparse projection length");
        assert!(matches!(
            ProjectionStore::new(&path).read_all(),
            Err(ReadModelError::Bounds("projection bytes"))
        ));
        let _ = std::fs::remove_file(path);
    }
}
