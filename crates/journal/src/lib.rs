//! Checksummed append-only journal with deterministic tail recovery.

#![forbid(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const MAGIC: &[u8; 4] = b"ITJR";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 4 + 2 + 8 + 4;
const TRAILER_LEN: usize = 4;
const MAX_JOURNAL_BYTES: u64 = 512 * 1024 * 1024;
const MAX_SEAL_BYTES: u64 = 256;

/// A journal payload with its monotonically increasing sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Journal sequence assigned at append time.
    pub sequence: u64,
    /// Opaque versioned event bytes.
    pub payload: Vec<u8>,
}

/// Hash manifest returned after an atomic journal backup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupManifest {
    /// Source journal path at backup time.
    pub source: PathBuf,
    /// Published backup path.
    pub destination: PathBuf,
    /// Number of bytes copied.
    pub byte_len: u64,
    /// SHA-256 digest of the copied journal bytes.
    pub sha256: [u8; 32],
}

/// Read-only result of scanning a journal file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scan {
    /// Valid records in file order.
    pub records: Vec<Record>,
    /// Byte offset immediately after the last valid record.
    pub valid_bytes: u64,
    /// Whether bytes after `valid_bytes` were truncated/corrupt.
    pub has_invalid_tail: bool,
}

/// Journal failures. Invalid tails are reported separately so callers can choose
/// whether opening in recovery mode is appropriate.
#[derive(Debug)]
pub enum JournalError {
    /// Underlying filesystem error.
    Io(io::Error),
    /// A record violated the journal framing or checksum.
    Corrupt {
        /// Byte offset where corruption was detected.
        offset: u64,
        /// Stable diagnostic category.
        reason: &'static str,
    },
    /// The requested append would overflow its bounded frame format.
    PayloadTooLarge(usize),
    /// The sealed SHA-256 digest does not match the journal bytes.
    SealMismatch,
    /// A backup or restore destination already exists.
    BackupExists(PathBuf),
    /// Another process currently owns the journal writer lock.
    WriterLocked(PathBuf),
    /// A journal segment exceeds the recovery buffer bound.
    BoundsExceeded(u64),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "journal I/O error: {error}"),
            Self::Corrupt { offset, reason } => {
                write!(formatter, "corrupt journal at {offset}: {reason}")
            }
            Self::PayloadTooLarge(size) => {
                write!(formatter, "journal payload too large: {size} bytes")
            }
            Self::SealMismatch => formatter.write_str("journal SHA-256 seal mismatch"),
            Self::BackupExists(path) => {
                write!(
                    formatter,
                    "backup destination already exists: {}",
                    path.display()
                )
            }
            Self::WriterLocked(path) => {
                write!(formatter, "journal writer lock exists: {}", path.display())
            }
            Self::BoundsExceeded(size) => {
                write!(
                    formatter,
                    "journal segment exceeds recovery bound: {size} bytes"
                )
            }
        }
    }
}

impl std::error::Error for JournalError {}

impl From<io::Error> for JournalError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Atomic single-writer ownership record for a journal path.
pub struct JournalWriterLock {
    path: PathBuf,
    file: File,
}

impl JournalWriterLock {
    /// Acquires an owner-only lock adjacent to the journal.
    ///
    /// The lock is created with `create_new`, so two engine processes cannot
    /// both believe they are authoritative. A crashed process leaves the lock
    /// for explicit operator recovery rather than risking concurrent mutation.
    ///
    /// # Errors
    /// Returns [`JournalError::WriterLocked`] when another owner exists.
    pub fn acquire(journal_path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = journal_path.as_ref().with_extension("lock");
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(JournalError::WriterLocked(path));
            }
            Err(error) => return Err(JournalError::Io(error)),
        };
        let owner = format!("pid={}\n", std::process::id());
        file.write_all(owner.as_bytes())?;
        file.sync_all()?;
        Ok(Self { path, file })
    }

    /// Returns the lock file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for JournalWriterLock {
    fn drop(&mut self) {
        let _ = self.file.sync_all();
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Single-writer append-only journal.
pub struct Journal {
    path: PathBuf,
    file: Mutex<File>,
    next_sequence: Mutex<u64>,
}

impl Journal {
    /// Opens a journal and truncates only an incomplete/corrupt final tail.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the path cannot be opened, read, or safely
    /// recovered, or when a supported-version record is structurally corrupt.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref().to_path_buf();
        let scan = scan_path(&path)?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        if scan.has_invalid_tail {
            file.set_len(scan.valid_bytes)?;
            file.seek(SeekFrom::End(0))?;
        }
        let next_sequence = scan
            .records
            .last()
            .map_or(0, |record| record.sequence.saturating_add(1));
        Ok(Self {
            path,
            file: Mutex::new(file),
            next_sequence: Mutex::new(next_sequence),
        })
    }

    /// Returns the journal path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends one framed record and synchronizes it to stable storage.
    ///
    /// # Errors
    /// Returns [`JournalError`] if the payload is too large, the journal lock is
    /// poisoned, or the filesystem rejects the write or synchronization.
    pub fn append(&self, payload: &[u8]) -> Result<u64, JournalError> {
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| JournalError::PayloadTooLarge(payload.len()))?;
        let mut sequence = self
            .next_sequence
            .lock()
            .map_err(|_| JournalError::Corrupt {
                offset: 0,
                reason: "sequence lock poisoned",
            })?;
        let mut file = self.file.lock().map_err(|_| JournalError::Corrupt {
            offset: 0,
            reason: "file lock poisoned",
        })?;
        let current = *sequence;
        let mut frame = Vec::with_capacity(HEADER_LEN + payload.len() + TRAILER_LEN);
        frame.extend_from_slice(MAGIC);
        frame.extend_from_slice(&VERSION.to_le_bytes());
        frame.extend_from_slice(&current.to_le_bytes());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&crc32c(&frame).to_le_bytes());
        let current_size = file.metadata()?.len();
        let projected_size = current_size
            .checked_add(
                u64::try_from(frame.len())
                    .map_err(|_| JournalError::PayloadTooLarge(payload.len()))?,
            )
            .ok_or(JournalError::BoundsExceeded(u64::MAX))?;
        if projected_size > MAX_JOURNAL_BYTES {
            return Err(JournalError::BoundsExceeded(projected_size));
        }
        file.write_all(&frame)?;
        file.sync_data()?;
        *sequence = current.saturating_add(1);
        Ok(current)
    }

    /// Scans the current file without modifying it.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the file cannot be read or contains an
    /// unsupported journal version.
    pub fn scan(&self) -> Result<Scan, JournalError> {
        scan_path(&self.path)
    }

    /// Seals the current journal bytes with an atomically published SHA-256 sidecar.
    ///
    /// # Errors
    /// Returns [`JournalError`] when the journal cannot be synced/read or the
    /// sidecar cannot be written atomically.
    pub fn seal(&self) -> Result<[u8; 32], JournalError> {
        self.file
            .lock()
            .map_err(|_| JournalError::Corrupt {
                offset: 0,
                reason: "file lock poisoned",
            })?
            .sync_data()?;
        let bytes = read_journal_bytes(&self.path)?;
        let digest = sha256(&bytes);
        let sidecar = sidecar_path(&self.path);
        let temporary = sidecar.with_extension("sha256.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(hex_digest(&digest).as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(temporary, sidecar)?;
        Ok(digest)
    }

    /// Verifies the journal bytes against the previously published seal.
    ///
    /// # Errors
    /// Returns [`JournalError::SealMismatch`] when the sidecar is absent,
    /// malformed, or does not match the current journal bytes.
    pub fn verify_seal(&self) -> Result<[u8; 32], JournalError> {
        let encoded = read_seal_text(&sidecar_path(&self.path))?;
        let expected = parse_hex_digest(encoded.trim()).ok_or(JournalError::SealMismatch)?;
        let actual = sha256(&read_journal_bytes(&self.path)?);
        if expected != actual {
            return Err(JournalError::SealMismatch);
        }
        Ok(actual)
    }

    /// Copies the journal and its verified digest to a new destination.
    ///
    /// The destination is first written to a sibling temporary file, synced,
    /// and atomically renamed. Existing destinations are rejected so a backup
    /// cannot silently overwrite an operator artifact.
    ///
    /// # Errors
    /// Returns [`JournalError::BackupExists`] for an existing destination or a
    /// filesystem error when the copy cannot be synced and published.
    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<BackupManifest, JournalError> {
        self.file
            .lock()
            .map_err(|_| JournalError::Corrupt {
                offset: 0,
                reason: "file lock poisoned",
            })?
            .sync_data()?;
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(JournalError::BackupExists(destination));
        }
        let bytes = read_journal_bytes(&self.path)?;
        let digest = sha256(&bytes);
        publish_copy(&bytes, &destination)?;
        if let Err(error) = publish_digest(&destination, &digest) {
            let _ = std::fs::remove_file(&destination);
            return Err(error);
        }
        Ok(BackupManifest {
            source: self.path.clone(),
            destination,
            byte_len: bytes.len() as u64,
            sha256: digest,
        })
    }

    /// Restores a verified backup into a new journal path.
    ///
    /// The source sidecar is mandatory and is checked before any destination
    /// file is created. The restored journal is then scanned to ensure its
    /// framing is valid.
    ///
    /// # Errors
    /// Returns [`JournalError::SealMismatch`] for a missing or invalid source
    /// digest, [`JournalError::BackupExists`] for an existing destination, or
    /// a journal/filesystem error for invalid restored bytes.
    pub fn restore_backup(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<BackupManifest, JournalError> {
        let source = source.as_ref().to_path_buf();
        let destination = destination.as_ref().to_path_buf();
        if destination.exists() {
            return Err(JournalError::BackupExists(destination));
        }
        let expected = read_seal_text(&sidecar_path(&source))
            .ok()
            .and_then(|value| parse_hex_digest(value.trim()))
            .ok_or(JournalError::SealMismatch)?;
        let bytes = read_journal_bytes(&source)?;
        let actual = sha256(&bytes);
        if actual != expected {
            return Err(JournalError::SealMismatch);
        }
        publish_copy(&bytes, &destination)?;
        if scan_path(&destination)?.has_invalid_tail {
            let _ = std::fs::remove_file(&destination);
            return Err(JournalError::Corrupt {
                offset: 0,
                reason: "backup contains an invalid tail",
            });
        }
        if let Err(error) = publish_digest(&destination, &actual) {
            let _ = std::fs::remove_file(&destination);
            return Err(error);
        }
        Ok(BackupManifest {
            source,
            destination,
            byte_len: bytes.len() as u64,
            sha256: actual,
        })
    }
}

fn sidecar_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.sha256", path.display()))
}

fn read_journal_bytes(path: &Path) -> Result<Vec<u8>, JournalError> {
    let file = File::open(path)?;
    let size = file.metadata()?.len();
    if size > MAX_JOURNAL_BYTES {
        return Err(JournalError::BoundsExceeded(size));
    }
    let mut bytes = Vec::new();
    file.take(MAX_JOURNAL_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(JournalError::BoundsExceeded(bytes.len() as u64));
    }
    Ok(bytes)
}

fn read_seal_text(path: &Path) -> Result<String, JournalError> {
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(MAX_SEAL_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_SEAL_BYTES {
        return Err(JournalError::BoundsExceeded(bytes.len() as u64));
    }
    String::from_utf8(bytes).map_err(|_| JournalError::SealMismatch)
}

fn publish_copy(bytes: &[u8], destination: &Path) -> Result<(), JournalError> {
    let temporary = PathBuf::from(format!(
        "{}.tmp.{}",
        destination.display(),
        std::process::id()
    ));
    if temporary.exists() {
        return Err(JournalError::BackupExists(temporary));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, destination)?;
    Ok(())
}

fn publish_digest(path: &Path, digest: &[u8; 32]) -> Result<(), JournalError> {
    let sidecar = sidecar_path(path);
    let temporary = PathBuf::from(format!("{}.tmp.{}", sidecar.display(), std::process::id()));
    if temporary.exists() {
        return Err(JournalError::BackupExists(temporary));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(hex_digest(digest).as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    std::fs::rename(temporary, sidecar)?;
    Ok(())
}

#[allow(clippy::format_collect)]
/// Returns a lowercase hexadecimal SHA-256 digest.
#[must_use]
pub fn hex_digest(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_hex_digest(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut digest = [0_u8; 32];
    for (index, slot) in digest.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(digest)
}

#[allow(
    clippy::unreadable_literal,
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::chunks_exact_to_as_chunks
)]
/// Computes a SHA-256 digest without relying on system tools.
#[must_use]
#[allow(clippy::missing_const_for_fn)]
pub fn sha256(input: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut data = input.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bit_len.to_be_bytes());
    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    for chunk in data.chunks_exact(64) {
        let mut words = [0_u32; 64];
        for (index, word) in words.iter_mut().take(16).enumerate() {
            *word =
                u32::from_be_bytes(chunk[index * 4..index * 4 + 4].try_into().unwrap_or([0; 4]));
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h) = (
            state[0], state[1], state[2], state[3], state[4], state[5], state[6], state[7],
        );
        for index in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(choose)
                .wrapping_add(K[index])
                .wrapping_add(words[index]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(majority);
            (h, g, f, e, d, c, b, a) = (
                g,
                f,
                e,
                d.wrapping_add(temp1),
                c,
                b,
                a,
                temp1.wrapping_add(temp2),
            );
        }
        for (index, value) in state.iter_mut().enumerate() {
            *value = (*value).wrapping_add([a, b, c, d, e, f, g, h][index]);
        }
    }
    let mut output = [0_u8; 32];
    for (index, word) in state.iter().enumerate() {
        output[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    output
}

fn scan_path(path: &Path) -> Result<Scan, JournalError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Scan {
                records: Vec::new(),
                valid_bytes: 0,
                has_invalid_tail: false,
            });
        }
        Err(error) => return Err(error.into()),
    };
    let size = file.metadata()?.len();
    if size > MAX_JOURNAL_BYTES {
        return Err(JournalError::BoundsExceeded(size));
    }
    let mut bytes = Vec::new();
    file.take(MAX_JOURNAL_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(JournalError::BoundsExceeded(bytes.len() as u64));
    }
    let mut offset = 0usize;
    let mut records = Vec::new();
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < HEADER_LEN {
            return Ok(Scan {
                records,
                valid_bytes: offset as u64,
                has_invalid_tail: true,
            });
        }
        if &bytes[offset..offset + 4] != MAGIC {
            return Ok(Scan {
                records,
                valid_bytes: offset as u64,
                has_invalid_tail: true,
            });
        }
        let version = u16::from_le_bytes([bytes[offset + 4], bytes[offset + 5]]);
        if version != VERSION {
            return Err(JournalError::Corrupt {
                offset: offset as u64,
                reason: "unsupported journal version",
            });
        }
        let sequence =
            u64::from_le_bytes(bytes[offset + 6..offset + 14].try_into().map_err(|_| {
                JournalError::Corrupt {
                    offset: offset as u64,
                    reason: "sequence framing",
                }
            })?);
        let length =
            u32::from_le_bytes(bytes[offset + 14..offset + 18].try_into().map_err(|_| {
                JournalError::Corrupt {
                    offset: offset as u64,
                    reason: "length framing",
                }
            })?) as usize;
        let frame_len = HEADER_LEN
            .saturating_add(length)
            .saturating_add(TRAILER_LEN);
        if frame_len > remaining {
            return Ok(Scan {
                records,
                valid_bytes: offset as u64,
                has_invalid_tail: true,
            });
        }
        let expected = u32::from_le_bytes(
            bytes[offset + HEADER_LEN + length..offset + frame_len]
                .try_into()
                .map_err(|_| JournalError::Corrupt {
                    offset: offset as u64,
                    reason: "checksum framing",
                })?,
        );
        if crc32c(&bytes[offset..offset + HEADER_LEN + length]) != expected {
            return Ok(Scan {
                records,
                valid_bytes: offset as u64,
                has_invalid_tail: true,
            });
        }
        records.push(Record {
            sequence,
            payload: bytes[offset + HEADER_LEN..offset + HEADER_LEN + length].to_vec(),
        });
        offset += frame_len;
    }
    Ok(Scan {
        records,
        valid_bytes: offset as u64,
        has_invalid_tail: false,
    })
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
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
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        Journal, JournalError, JournalWriterLock, MAX_JOURNAL_BYTES, hex_digest, scan_path, sha256,
    };

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map_or(0, |value| value.as_nanos());
        std::env::temp_dir().join(format!("insidertrader-journal-{label}-{nanos}.log"))
    }

    #[test]
    fn oversized_sparse_segment_is_rejected_before_recovery_buffering() {
        let path = temp_path("oversized");
        let file = fs::File::create(&path).ok();
        assert!(file.is_some());
        if let Some(file) = file {
            assert!(file.set_len(MAX_JOURNAL_BYTES + 1).is_ok());
        }
        assert!(matches!(
            scan_path(&path),
            Err(JournalError::BoundsExceeded(size)) if size == MAX_JOURNAL_BYTES + 1
        ));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn append_and_reopen_preserve_sequences_and_payloads() {
        let path = temp_path("reopen");
        let journal = Journal::open(&path).ok();
        assert!(journal.is_some());
        if let Some(journal) = journal {
            assert_eq!(journal.append(b"one").ok(), Some(0));
            assert_eq!(journal.append(b"two").ok(), Some(1));
        }
        let reopened = Journal::open(&path).ok();
        assert!(reopened.is_some());
        if let Some(reopened) = reopened {
            let scan = reopened.scan().ok();
            assert_eq!(scan.as_ref().map(|value| value.records.len()), Some(2));
            assert_eq!(reopened.append(b"three").ok(), Some(2));
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn writer_lock_is_atomic_and_releases_on_drop() {
        let path = temp_path("writer-lock");
        let first = JournalWriterLock::acquire(&path).ok();
        assert!(first.is_some());
        assert!(matches!(
            JournalWriterLock::acquire(&path),
            Err(super::JournalError::WriterLocked(_))
        ));
        drop(first);
        assert!(JournalWriterLock::acquire(&path).is_ok());
        let _ = fs::remove_file(path.with_extension("lock"));
    }

    #[test]
    fn incomplete_tail_is_truncated_on_reopen() {
        let path = temp_path("tail");
        let journal = Journal::open(&path).ok();
        assert!(journal.is_some());
        if let Some(journal) = journal {
            assert_eq!(journal.append(b"valid").ok(), Some(0));
        }
        if let Ok(mut file) = fs::OpenOptions::new().append(true).open(&path) {
            use std::io::Write;
            let _ = file.write_all(b"IT");
        }
        let reopened = Journal::open(&path).ok();
        assert!(reopened.is_some());
        if let Some(reopened) = reopened {
            assert_eq!(
                reopened.scan().ok().map(|value| value.has_invalid_tail),
                Some(false)
            );
            assert_eq!(reopened.append(b"next").ok(), Some(1));
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn checksum_corruption_is_recovered_as_invalid_tail() {
        let path = temp_path("checksum");
        let journal = Journal::open(&path).ok();
        assert!(journal.is_some());
        if let Some(journal) = journal {
            assert_eq!(journal.append(b"payload").ok(), Some(0));
        }
        if let Ok(mut bytes) = fs::read(&path) {
            if let Some(last) = bytes.last_mut() {
                *last ^= 0xff;
            }
            let _ = fs::write(&path, bytes);
        }
        let scan = Journal::open(&path)
            .ok()
            .and_then(|journal| journal.scan().ok());
        assert_eq!(scan.map(|value| value.has_invalid_tail), Some(false));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn sha256_seal_verifies_and_detects_mutation() {
        let path = temp_path("seal");
        let sidecar = PathBuf::from(format!("{}.sha256", path.display()));
        let journal = Journal::open(&path).ok();
        assert!(journal.is_some());
        if let Some(journal) = journal {
            assert!(journal.append(b"sealed").is_ok());
            assert!(journal.seal().is_ok());
            assert!(journal.verify_seal().is_ok());
        }
        assert!(fs::write(&path, b"tampered").is_ok());
        let reopened = Journal::open(&path).ok();
        assert!(reopened.is_some_and(|journal| journal.verify_seal().is_err()));
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(sidecar);
    }

    #[test]
    fn sha256_matches_standard_abc_vector() {
        assert_eq!(
            hex_digest(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn backup_and_restore_are_atomic_and_hash_verified() {
        let path = temp_path("backup-source");
        let backup = temp_path("backup-copy");
        let restored = temp_path("backup-restored");
        let journal = Journal::open(&path).ok();
        assert!(journal.is_some());
        if let Some(journal) = journal {
            assert!(journal.append(b"durable-order").is_ok());
            let manifest = journal.backup_to(&backup).ok();
            assert_eq!(
                manifest.as_ref().map(|value| value.byte_len),
                fs::metadata(&backup).ok().map(|value| value.len())
            );
            assert!(manifest.is_some_and(
                |value| value.sha256 == sha256(&fs::read(&backup).unwrap_or_default())
            ));
        }
        let restored_manifest = Journal::restore_backup(&backup, &restored).ok();
        assert!(restored_manifest.is_some());
        assert_eq!(
            Journal::open(&restored)
                .ok()
                .and_then(|journal| journal.scan().ok())
                .map(|scan| scan.records.len()),
            Some(1)
        );
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(backup.clone());
        let _ = fs::remove_file(restored.clone());
        let _ = fs::remove_file(format!("{}.sha256", backup.display()));
        let _ = fs::remove_file(format!("{}.sha256", restored.display()));
    }
}
