// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

//! Blob Bundle Format Structure:
//!
//! ```text
//! +----------------------+
//! |      Header          |
//! +----------------------+
//! | Magic    [4 bytes]   | "WLBD" (0x57, 0x4C, 0x42, 0x44)
//! | Version  [4 bytes]   | Format version (currently 1)
//! +----------------------+
//! |    Data Section      |
//! +----------------------+
//! | Data Entry 1         | Raw bytes of first blob
//! | Data Entry 2         | Raw bytes of second blob
//! | ...                  | ...
//! | Data Entry N         | Raw bytes of Nth blob
//! +----------------------+
//! |   Index Section      |
//! +----------------------+
//! | Index Entry 1        |
//! |   ID Len [4 bytes]   |
//! |   ID     [varies]    |
//! |   Offset [8 bytes]   |
//! |   Length [8 bytes]   |
//! |   CRC32  [4 bytes]   |
//! +----------------------+
//! | Index Entry 2        |
//! | ...                  |
//! | Index Entry N        |
//! +----------------------+
//! |      Footer          |
//! +----------------------+
//! | Magic    [4 bytes]   | "DBLW" (0x44, 0x42, 0x4C, 0x57) - reversed
//! | Version  [4 bytes]   | Format version (must match header)
//! | Index Offset         |
//! |          [8 bytes]   | Byte offset where index section starts
//! | Index Entries        |
//! |          [4 bytes]   | Number of entries in index
//! | Footer CRC32         |
//! |          [4 bytes]   | CRC32 of footer data (excluding this field)
//! +----------------------+
//! ```

use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{self, Cursor, Read, Seek, Write},
    num::NonZeroU16,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use crc32fast::Hasher;
use thiserror::Error;

/// Current version of the format
const FORMAT_VERSION: u32 = 1;

/// Byte sizes for serialized types
const MAGIC_SIZE: usize = std::mem::size_of::<[u8; 4]>();
const VERSION_SIZE: usize = std::mem::size_of::<u32>();
const U32_SIZE: usize = std::mem::size_of::<u32>();
const U64_SIZE: usize = std::mem::size_of::<u64>();
const CRC32_SIZE: usize = std::mem::size_of::<u32>();

/// Magic number for the blob bundle header
const HEADER_MAGIC: [u8; MAGIC_SIZE] = [0x57, 0x4C, 0x42, 0x44]; // "WLBD"

/// Magic number for the blob bundle footer
const FOOTER_MAGIC: [u8; MAGIC_SIZE] = [0x44, 0x42, 0x4C, 0x57]; // "DBLW" (reversed)

/// Error types for blob bundle operations
#[derive(Debug, Error)]
pub enum BlobBundleError {
    #[error("Invalid magic number in header")]
    InvalidHeaderMagic,

    #[error("Invalid magic number in footer")]
    InvalidFooterMagic,

    #[error("Unsupported format version: {0}")]
    UnsupportedVersion(u32),

    #[error("Entry CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    EntryCrcMismatch { expected: u32, actual: u32 },

    #[error("Footer CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    FooterCrcMismatch { expected: u32, actual: u32 },

    #[error("Index entry not found: {0}")]
    EntryNotFound(String),

    #[error("Invalid format structure")]
    InvalidFormat,

    #[error("Blob size {size} bytes exceeds maximum {max_size} bytes for {n_shards} shards")]
    BlobSizeExceeded {
        size: u64,
        max_size: u64,
        n_shards: u16,
    },

    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// Result type for blob bundle operations
pub type Result<T> = std::result::Result<T, BlobBundleError>;

/// Header structure for the blob bundle format
#[derive(Debug, Clone)]
struct Header {
    magic: [u8; MAGIC_SIZE],
    version: u32,
}

impl Header {
    fn new() -> Self {
        Self {
            magic: HEADER_MAGIC,
            version: FORMAT_VERSION,
        }
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.magic)?;
        writer.write_all(&self.version.to_le_bytes())?;
        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut magic = [0u8; MAGIC_SIZE];
        reader.read_exact(&mut magic)?;

        if magic != HEADER_MAGIC {
            return Err(BlobBundleError::InvalidHeaderMagic);
        }

        let mut version_bytes = [0u8; VERSION_SIZE];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);

        if version != FORMAT_VERSION {
            return Err(BlobBundleError::UnsupportedVersion(version));
        }

        Ok(Self { magic, version })
    }

    fn size() -> usize {
        MAGIC_SIZE + VERSION_SIZE // magic + version
    }
}

/// Index entry for each data segment
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub id: String,
    pub offset: u64,
    pub length: u64,
    pub crc32: u32,
}

impl IndexEntry {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write ID length and ID
        let id_bytes = self.id.as_bytes();
        let id_len = u32::try_from(id_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "ID length too large"))?;
        writer.write_all(&id_len.to_le_bytes())?;
        writer.write_all(id_bytes)?;

        // Write offset, length, and CRC32
        writer.write_all(&self.offset.to_le_bytes())?;
        writer.write_all(&self.length.to_le_bytes())?;
        writer.write_all(&self.crc32.to_le_bytes())?;

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // Read ID length and ID
        let mut id_len_bytes = [0u8; U32_SIZE];
        reader.read_exact(&mut id_len_bytes)?;
        let id_len = u32::from_le_bytes(id_len_bytes) as usize;

        let mut id_bytes = vec![0u8; id_len];
        reader.read_exact(&mut id_bytes)?;
        let id = String::from_utf8(id_bytes).map_err(|_| BlobBundleError::InvalidFormat)?;

        // Read offset, length, and CRC32
        let mut offset_bytes = [0u8; U64_SIZE];
        reader.read_exact(&mut offset_bytes)?;
        let offset = u64::from_le_bytes(offset_bytes);

        let mut length_bytes = [0u8; U64_SIZE];
        reader.read_exact(&mut length_bytes)?;
        let length = u64::from_le_bytes(length_bytes);

        let mut crc32_bytes = [0u8; CRC32_SIZE];
        reader.read_exact(&mut crc32_bytes)?;
        let crc32 = u32::from_le_bytes(crc32_bytes);

        Ok(Self {
            id,
            offset,
            length,
            crc32,
        })
    }
}

/// Footer structure for the blob bundle format
#[derive(Debug, Clone)]
struct Footer {
    magic: [u8; MAGIC_SIZE],
    version: u32,
    index_offset: u64,
    index_entries: u32,
    footer_crc32: u32,
}

impl Footer {
    fn new(index_offset: u64, index_entries: u32) -> Self {
        // Calculate CRC32 of footer data (excluding the CRC32 field itself)
        let mut hasher = Hasher::new();
        hasher.update(&FOOTER_MAGIC);
        hasher.update(&FORMAT_VERSION.to_le_bytes());
        hasher.update(&index_offset.to_le_bytes());
        hasher.update(&index_entries.to_le_bytes());
        let footer_crc32 = hasher.finalize();

        Self {
            magic: FOOTER_MAGIC,
            version: FORMAT_VERSION,
            index_offset,
            index_entries,
            footer_crc32,
        }
    }

    /// Validates that the footer's CRC32 checksum matches the calculated value
    pub fn validate_crc32(&self) -> Result<()> {
        let mut hasher = Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.index_offset.to_le_bytes());
        hasher.update(&self.index_entries.to_le_bytes());
        let calculated_crc32 = hasher.finalize();

        if calculated_crc32 != self.footer_crc32 {
            return Err(BlobBundleError::FooterCrcMismatch {
                expected: self.footer_crc32,
                actual: calculated_crc32,
            });
        }

        Ok(())
    }

    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.magic)?;
        writer.write_all(&self.version.to_le_bytes())?;
        writer.write_all(&self.index_offset.to_le_bytes())?;
        writer.write_all(&self.index_entries.to_le_bytes())?;
        writer.write_all(&self.footer_crc32.to_le_bytes())?;

        Ok(())
    }

    fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut magic = [0u8; MAGIC_SIZE];
        reader.read_exact(&mut magic)?;

        if magic != FOOTER_MAGIC {
            return Err(BlobBundleError::InvalidFooterMagic);
        }

        let mut version_bytes = [0u8; VERSION_SIZE];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);

        if version != FORMAT_VERSION {
            return Err(BlobBundleError::UnsupportedVersion(version));
        }

        let mut index_offset_bytes = [0u8; U64_SIZE];
        reader.read_exact(&mut index_offset_bytes)?;
        let index_offset = u64::from_le_bytes(index_offset_bytes);

        let mut index_entries_bytes = [0u8; U32_SIZE];
        reader.read_exact(&mut index_entries_bytes)?;
        let index_entries = u32::from_le_bytes(index_entries_bytes);

        let mut footer_crc32_bytes = [0u8; CRC32_SIZE];
        reader.read_exact(&mut footer_crc32_bytes)?;
        let footer_crc32 = u32::from_le_bytes(footer_crc32_bytes);

        let footer = Self {
            magic,
            version,
            index_offset,
            index_entries,
            footer_crc32,
        };

        // Validate CRC32 using the helper method
        footer.validate_crc32()?;

        Ok(footer)
    }

    fn size() -> usize {
        // magic + version + index_offset + index_entries + crc32
        MAGIC_SIZE + VERSION_SIZE + U64_SIZE + U32_SIZE + CRC32_SIZE
    }
}

pub trait BlobBundleBuilderTrait {
    fn get_index_map(&self) -> Vec<(String, (u64, u64))>;
    fn get_total_size(&self) -> u64;
    // Intentionally to move the data out of the builder.
    fn get_data(self) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send;
}

/// Result of building a blob bundle to a file
#[derive(Debug)]
pub struct BlobBundleBuildResult {
    /// Path to the created blob bundle file
    pub file_path: PathBuf,
    /// Map of id to (offset, length) in insertion order
    pub index_map: Vec<(String, (u64, u64))>,
    /// Total size of the blob bundle file
    pub total_size: u64,
}

impl BlobBundleBuilderTrait for BlobBundleBuildResult {
    fn get_index_map(&self) -> Vec<(String, (u64, u64))> {
        self.index_map.clone()
    }

    fn get_total_size(&self) -> u64 {
        self.total_size
    }

    async fn get_data(self) -> Result<Vec<u8>> {
        tokio::fs::read(self.file_path.clone())
            .await
            .map_err(|e| BlobBundleError::Io(e))
    }
}

/// Result from building a blob bundle entirely in memory.
#[derive(Debug, Clone)]
pub struct BlobBundleInMemoryBuildResult {
    /// The complete blob bundle as bytes.
    pub bundle: Vec<u8>,
    /// Map of id to (offset, length) in insertion order.
    pub index_map: Vec<(String, (u64, u64))>,
}

impl BlobBundleBuilderTrait for BlobBundleInMemoryBuildResult {
    fn get_index_map(&self) -> Vec<(String, (u64, u64))> {
        self.index_map.clone()
    }

    fn get_total_size(&self) -> u64 {
        self.bundle.len() as u64
    }

    async fn get_data(self) -> Result<Vec<u8>> {
        Ok(self.bundle)
    }
}

/// Builder for creating blob bundle with the specified format
#[derive(Debug)]
pub struct BlobBundleBuilder {
    n_shards: NonZeroU16,
}

impl BlobBundleBuilder {
    /// Create a new builder with the specified number of shards
    pub fn new(n_shards: NonZeroU16) -> Self {
        Self { n_shards }
    }

    /// Calculate the estimated size of the bundle for the given file paths.
    /// This is useful for checking whether the bundle will exceed size limits.
    ///
    /// Returns the estimated total size in bytes including:
    /// - Header (magic + version)
    /// - All data entries (file sizes)
    /// - Index entries (with ID strings, offsets, lengths, and CRC32s)
    /// - Footer (magic + version + index_offset + index_entries + crc32)
    pub fn estimate_size<P: AsRef<Path>>(file_paths: &[P]) -> Result<u64> {
        // Calculate data size by summing file sizes
        let mut data_size = 0u64;
        for path in file_paths {
            let metadata = std::fs::metadata(path.as_ref()).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "Failed to read file metadata for {}: {}",
                        path.as_ref().display(),
                        e
                    ),
                )
            })?;
            data_size += metadata.len();
        }

        // Calculate header size
        let header_size = Header::size() as u64;

        // Calculate index overhead
        // Each index entry has: id_len (4) + id_bytes + offset (8) + length (8) + crc32 (4)
        let index_entry_overhead_per_item = U32_SIZE + U64_SIZE + U64_SIZE + CRC32_SIZE;
        let index_overhead: u64 = file_paths
            .iter()
            .map(|p| {
                let id = p
                    .as_ref()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .len();
                id + index_entry_overhead_per_item
            })
            .sum::<usize>() as u64;

        // Calculate footer size
        let footer_size = Footer::size() as u64;

        // Return total size
        Ok(header_size + data_size + index_overhead + footer_size)
    }

    /// Build the blob bundle from a list of file paths.
    /// Files are added as data entries in the order they appear in the list.
    /// The output file is created atomically using a temporary file that is renamed upon success.
    ///
    /// # Arguments
    /// * `file_paths` - List of paths to files to include in the bundle
    /// * `output_path` - Path where the blob bundle file will be created
    ///
    /// # Returns
    /// * `BlobBundleBuildResult` containing the output file path and index map
    pub fn build<P: AsRef<Path>, Q: AsRef<Path>>(
        self,
        file_paths: &[P],
        output_path: Q,
    ) -> Result<BlobBundleBuildResult> {
        let output_path = output_path.as_ref();

        // First, estimate the total size to check against max blob size
        let estimated_size = Self::estimate_size(file_paths)?;

        // Check against max blob size for RS2 encoding
        let max_size = walrus_core::encoding::max_blob_size_for_n_shards(
            self.n_shards,
            walrus_core::EncodingType::RS2,
        );

        if estimated_size > max_size {
            return Err(BlobBundleError::BlobSizeExceeded {
                size: estimated_size,
                max_size,
                n_shards: self.n_shards.get(),
            });
        }

        // Create temporary file for atomic write
        let temp_path = output_path.with_extension("tmp");
        let mut output_file = File::create(&temp_path)?;

        let mut index_entries = Vec::new();
        let mut index_map = Vec::new();

        // Write header
        let header = Header::new();
        header.write_to(&mut output_file)?;

        // Stream each file directly to the output, building index as we go
        for path in file_paths {
            let path = path.as_ref();

            // Get the current position in the output file (this is the offset for this entry)
            let offset = output_file.stream_position()?;

            // Open input file
            let mut input_file = File::open(path)?;
            let metadata = input_file.metadata()?;
            let file_size = metadata.len();

            // Calculate CRC32 while copying
            let mut hasher = Hasher::new();
            let mut buffer = [0u8; 256 * 1024]; // 256KB buffer for streaming
            let mut bytes_copied = 0u64;

            loop {
                let bytes_read = input_file.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                hasher.update(&buffer[..bytes_read]);
                output_file.write_all(&buffer[..bytes_read])?;
                bytes_copied += bytes_read as u64;
            }

            // Verify we copied the expected number of bytes
            if bytes_copied != file_size {
                return Err(BlobBundleError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "File size mismatch for {}: expected {}, copied {}",
                        path.display(),
                        file_size,
                        bytes_copied
                    ),
                )));
            }

            let crc32 = hasher.finalize();

            // Create ID from filename
            let id = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();

            // Add to index map
            index_map.push((id.clone(), (offset, file_size)));

            // Create index entry
            let entry = IndexEntry {
                id,
                offset,
                length: file_size,
                crc32,
            };
            index_entries.push(entry);
        }

        // Record where the index starts
        let index_offset = output_file.stream_position()?;

        // Write index entries
        for entry in &index_entries {
            entry.write_to(&mut output_file)?;
        }

        // Write footer
        let index_entries_count =
            u32::try_from(index_entries.len()).map_err(|_| BlobBundleError::InvalidFormat)?;
        let footer = Footer::new(index_offset, index_entries_count);
        footer.write_to(&mut output_file)?;

        // Get final file size
        let total_size = output_file.stream_position()?;

        // Ensure all data is written to disk
        // output_file.sync_all()?;
        drop(output_file);

        // Atomically rename temp file to final name
        std::fs::rename(&temp_path, output_path)?;

        Ok(BlobBundleBuildResult {
            file_path: output_path.to_path_buf(),
            index_map,
            total_size,
        })
    }

    /// Build the blob bundle entirely in memory from a list of file paths.
    /// Returns the complete blob bundle as a Vec<u8> along with the index map.
    ///
    /// # Arguments
    /// * `file_paths` - List of paths to files to include in the bundle
    ///
    /// # Returns
    /// * `BlobBundleInMemoryBuildResult` - The blob bundle bytes and index map
    pub fn build_in_memory<P: AsRef<Path>>(
        self,
        file_paths: &[P],
    ) -> Result<BlobBundleInMemoryBuildResult> {
        // First, estimate the total size to check against max blob size.
        let estimated_size = Self::estimate_size(file_paths)?;

        // Check against max blob size for RS2 encoding.
        let max_size = walrus_core::encoding::max_blob_size_for_n_shards(
            self.n_shards,
            walrus_core::EncodingType::RS2,
        );

        if estimated_size > max_size {
            return Err(BlobBundleError::BlobSizeExceeded {
                size: estimated_size,
                max_size,
                n_shards: self.n_shards.get(),
            });
        }

        // Pre-allocate a buffer with estimated size.
        let mut output_buffer = Vec::with_capacity(estimated_size as usize);
        let mut cursor = Cursor::new(&mut output_buffer);

        let mut index_entries = Vec::new();
        let mut index_map = Vec::new();

        // Write header.
        let header = Header::new();
        header.write_to(&mut cursor)?;

        // Stream each file to the buffer, building index as we go.
        for path in file_paths {
            let path = path.as_ref();

            // Get the current position in the output buffer (this is the offset for this entry).
            let offset = cursor.position();

            // Open input file.
            let mut input_file = File::open(path)?;
            let metadata = input_file.metadata()?;
            let file_size = metadata.len();

            // Calculate CRC32 while copying.
            let mut hasher = Hasher::new();
            let mut buffer = [0u8; 8192]; // 8KB buffer for streaming.
            let mut bytes_copied = 0u64;

            loop {
                let bytes_read = input_file.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                hasher.update(&buffer[..bytes_read]);
                cursor.write_all(&buffer[..bytes_read])?;
                bytes_copied += bytes_read as u64;
            }

            // Verify we copied the expected number of bytes.
            if bytes_copied != file_size {
                return Err(BlobBundleError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "File size mismatch for {}: expected {}, copied {}",
                        path.display(),
                        file_size,
                        bytes_copied
                    ),
                )));
            }

            let crc32 = hasher.finalize();

            // Create ID from filename.
            let id = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();

            // Add to index map.
            index_map.push((id.clone(), (offset, file_size)));

            // Create index entry.
            let entry = IndexEntry {
                id,
                offset,
                length: file_size,
                crc32,
            };
            index_entries.push(entry);
        }

        // Record where the index starts.
        let index_offset = cursor.position();

        // Write index entries.
        for entry in &index_entries {
            entry.write_to(&mut cursor)?;
        }

        // Write footer.
        let index_entries_count =
            u32::try_from(index_entries.len()).map_err(|_| BlobBundleError::InvalidFormat)?;
        let footer = Footer::new(index_offset, index_entries_count);
        footer.write_to(&mut cursor)?;

        // Drop the cursor to release the borrow on output_buffer.
        drop(cursor);

        // Shrink the buffer to the exact size (remove any extra capacity).
        output_buffer.shrink_to_fit();

        Ok(BlobBundleInMemoryBuildResult {
            bundle: output_buffer,
            index_map,
        })
    }
}

impl Default for BlobBundleBuilder {
    fn default() -> Self {
        // Default to 1000 shards
        // SAFETY: 1000 is a non-zero value, so this is guaranteed to succeed
        Self::new(NonZeroU16::new(1000).expect("1000 is non-zero"))
    }
}

/// Reader for blob bundle format with lazy index parsing
#[derive(Debug)]
pub struct BlobBundleReader {
    data: Bytes,
    // Lazy index parsing
    index: RefCell<Option<HashMap<String, IndexEntry>>>,
    // List of IDs in the order they appear in the blob bundle.
    ids: RefCell<Option<Vec<String>>>,
}

impl BlobBundleReader {
    /// Create a new reader from blob bundle bytes
    ///
    /// This method validates:
    /// - Header magic number matches expected value
    /// - Footer magic number matches expected value
    /// - Header and footer versions match
    /// - Footer CRC32 is valid
    ///
    /// Note: The index is NOT parsed until needed by operations that require it.
    pub fn new(data: Bytes) -> Result<Self> {
        // Minimum size check: header + footer at least
        if data.len() < Header::size() + Footer::size() {
            return Err(BlobBundleError::InvalidFormat);
        }

        let mut cursor = Cursor::new(&data);

        // Read and verify header
        let header = Header::read_from(&mut cursor)?;

        // Header validation is already done in Header::read_from
        // which checks magic and version

        // Read footer to get index location
        let footer_offset = data.len() - Footer::size();
        cursor.set_position(footer_offset as u64);
        let footer = Footer::read_from(&mut cursor)?;

        // Footer validation is already done in Footer::read_from
        // which checks magic, version, and CRC32

        // Verify that header and footer versions match
        if header.version != footer.version {
            return Err(BlobBundleError::InvalidFormat);
        }

        // Validate index offset is within bounds
        // Note: index_offset can equal footer_offset when there are no entries
        if footer.index_offset < Header::size() as u64 || footer.index_offset > footer_offset as u64
        {
            return Err(BlobBundleError::InvalidFormat);
        }

        Ok(Self {
            data,
            index: RefCell::new(None),
            ids: RefCell::new(None),
        })
    }

    /// Parse the index if it hasn't been parsed yet
    fn ensure_index_parsed(&self) -> Result<()> {
        if self.index.borrow().is_some() {
            return Ok(());
        }

        // Get footer to find index location
        let footer_offset = self.data.len() - Footer::size();
        let mut cursor = Cursor::new(&self.data);
        cursor.set_position(footer_offset as u64);
        let footer = Footer::read_from(&mut cursor)?;

        // Parse index
        cursor.set_position(footer.index_offset);
        let mut index = HashMap::new();
        let mut ids = Vec::new();

        for _ in 0..footer.index_entries {
            let entry = IndexEntry::read_from(&mut cursor)?;

            // Validate that entry offset and length are within data section bounds
            let entry_end = entry.offset.saturating_add(entry.length);
            if entry.offset < Header::size() as u64 || entry_end > footer.index_offset {
                return Err(BlobBundleError::InvalidFormat);
            }

            // Add to IDs list.
            ids.push(entry.id.clone());

            // Add to index map.
            index.insert(entry.id.clone(), entry);
        }

        *self.index.borrow_mut() = Some(index);
        *self.ids.borrow_mut() = Some(ids);
        Ok(())
    }

    /// Get data by ID
    pub fn get(&self, id: &str) -> Result<Bytes> {
        self.ensure_index_parsed()?;

        let index = self.index.borrow();
        let index = index
            .as_ref()
            .expect("index should be populated after ensure_index_parsed");

        let entry = index
            .get(id)
            .ok_or_else(|| BlobBundleError::EntryNotFound(id.to_string()))?;

        // Extract data slice
        let start = usize::try_from(entry.offset).map_err(|_| BlobBundleError::InvalidFormat)?;
        let length = usize::try_from(entry.length).map_err(|_| BlobBundleError::InvalidFormat)?;
        let end = start
            .checked_add(length)
            .ok_or(BlobBundleError::InvalidFormat)?;

        if end > self.data.len() {
            return Err(BlobBundleError::InvalidFormat);
        }

        let data = self.data.slice(start..end);

        // Verify CRC32
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let calculated_crc32 = hasher.finalize();

        if calculated_crc32 != entry.crc32 {
            return Err(BlobBundleError::EntryCrcMismatch {
                expected: entry.crc32,
                actual: calculated_crc32,
            });
        }

        Ok(data)
    }

    /// List all IDs in the blob bundle.
    pub fn list_ids(&self) -> Result<Vec<String>> {
        self.ensure_index_parsed()?;

        let ids = self.ids.borrow();
        let ids = ids
            .as_ref()
            .expect("ids should be populated after ensure_index_parsed");

        Ok(ids.clone())
    }

    /// Get index entry by ID
    pub fn get_entry(&self, id: &str) -> Result<Option<IndexEntry>> {
        self.ensure_index_parsed()?;

        let index = self.index.borrow();
        let index = index
            .as_ref()
            .expect("index should be populated after ensure_index_parsed");

        Ok(index.get(id).cloned())
    }

    /// Get all index entries in the order they appear in the blob bundle.
    pub fn entries(&self) -> Result<Vec<(String, IndexEntry)>> {
        self.ensure_index_parsed()?;

        let ids = self.ids.borrow();
        let ids = ids
            .as_ref()
            .expect("ids should be populated after ensure_index_parsed");

        let index = self.index.borrow();
        let index = index
            .as_ref()
            .expect("index should be populated after ensure_index_parsed");

        // Return entries in the order they appear in the blob bundle.
        Ok(ids
            .iter()
            .map(|id| {
                let entry = index
                    .get(id)
                    .expect("id from ids list should exist in index");
                (id.clone(), entry.clone())
            })
            .collect())
    }

    /// Read raw bytes from the blob bundle at the specified offset and length.
    ///
    /// This method provides direct access to the underlying data without any CRC32 verification.
    /// It's useful for reading specific portions of the blob bundle, such as when you have
    /// the offset and length from the index map.
    ///
    /// # Arguments
    /// * `offset` - The byte offset to start reading from
    /// * `length` - The number of bytes to read
    ///
    /// # Returns
    /// * `Ok(Bytes)` - The requested bytes if the range is valid
    /// * `Err(InvalidFormat)` - If the requested range is out of bounds
    pub fn read_range(&self, offset: u64, length: u64) -> Result<Bytes> {
        let start = usize::try_from(offset).map_err(|_| BlobBundleError::InvalidFormat)?;
        let length_usize = usize::try_from(length).map_err(|_| BlobBundleError::InvalidFormat)?;
        let end = start.saturating_add(length_usize);

        if end > self.data.len() {
            return Err(BlobBundleError::InvalidFormat);
        }

        Ok(self.data.slice(start..end))
    }

    /// Get the total size of the blob bundle
    pub fn total_size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_blob_bundle_roundtrip() {
        // Create temporary directory for test files
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test files
        let file1_path = temp_dir.path().join("entry1.txt");
        let file2_path = temp_dir.path().join("entry2.txt");
        let file3_path = temp_dir.path().join("entry3.bin");

        let data1 = b"Hello, World!";
        let data2 = b"This is test data";
        let data3 = vec![1, 2, 3, 4, 5];

        fs::write(&file1_path, data1).expect("Failed to write file1");
        fs::write(&file2_path, data2).expect("Failed to write file2");
        fs::write(&file3_path, &data3).expect("Failed to write file3");

        // Build blob bundle
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");

        let file_paths = vec![&file1_path, &file2_path, &file3_path];
        let result = builder
            .build(&file_paths, &output_path)
            .expect("Failed to build bundle");

        // Verify result
        assert_eq!(result.file_path, output_path);
        assert_eq!(result.index_map.len(), 3);
        assert_eq!(result.index_map[0].0, "entry1.txt");
        assert_eq!(result.index_map[1].0, "entry2.txt");
        assert_eq!(result.index_map[2].0, "entry3.bin");

        // Read blob bundle
        let bundle_data = fs::read(&output_path).expect("Failed to read bundle file");
        let reader =
            BlobBundleReader::new(Bytes::from(bundle_data)).expect("Failed to create reader");

        // Verify all entries
        assert_eq!(
            reader.get("entry1.txt").expect("Failed to get entry1"),
            Bytes::from(data1.to_vec())
        );
        assert_eq!(
            reader.get("entry2.txt").expect("Failed to get entry2"),
            Bytes::from(data2.to_vec())
        );
        assert_eq!(
            reader.get("entry3.bin").expect("Failed to get entry3"),
            Bytes::from(data3.clone())
        );

        // Verify IDs
        let mut ids = reader.list_ids().expect("Failed to list IDs");
        ids.sort();
        assert_eq!(ids, vec!["entry1.txt", "entry2.txt", "entry3.bin"]);
    }

    #[test]
    fn test_invalid_crc() {
        // Create temporary directory for test files
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test file
        let file_path = temp_dir.path().join("test.txt");
        fs::write(&file_path, b"Test data").expect("Failed to write file");

        // Build blob bundle
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");

        let _result = builder
            .build(&[&file_path], &output_path)
            .expect("Failed to build bundle");

        // Read and corrupt the bundle
        let mut bundled = fs::read(&output_path).expect("Failed to read bundle");

        // Corrupt the data (but not the index or footer)
        bundled[10] ^= 0xFF;

        // Try to read
        let reader = BlobBundleReader::new(Bytes::from(bundled)).expect("Failed to create reader");

        // Should fail with CRC mismatch
        match reader.get("test.txt") {
            Err(BlobBundleError::EntryCrcMismatch { .. }) => {}
            _ => panic!("Expected EntryCrcMismatch error"),
        }
    }

    #[test]
    fn test_empty_builder() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("empty.blob");

        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let result = builder
            .build(&[] as &[&Path], &output_path)
            .expect("Failed to build bundle");

        assert_eq!(result.index_map.len(), 0);

        let bundle_data = fs::read(&output_path).expect("Failed to read bundle");
        let reader =
            BlobBundleReader::new(Bytes::from(bundle_data)).expect("Failed to create reader");
        assert_eq!(reader.list_ids().expect("Failed to list IDs").len(), 0);
    }

    #[test]
    fn test_entry_not_found() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test file
        let file_path = temp_dir.path().join("exists.txt");
        fs::write(&file_path, b"data").expect("Failed to write file");

        let output_path = temp_dir.path().join("bundle.blob");
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let _result = builder
            .build(&[&file_path], &output_path)
            .expect("Failed to build bundle");

        let bundle_data = fs::read(&output_path).expect("Failed to read bundle");
        let reader =
            BlobBundleReader::new(Bytes::from(bundle_data)).expect("Failed to create reader");

        match reader.get("not_exists") {
            Err(BlobBundleError::EntryNotFound(id)) => {
                assert_eq!(id, "not_exists");
            }
            _ => panic!("Expected EntryNotFound error"),
        }
    }

    #[test]
    fn test_footer_validate_crc32_valid() {
        // Calculate the correct CRC32
        let mut hasher = Hasher::new();
        hasher.update(&FOOTER_MAGIC);
        hasher.update(&FORMAT_VERSION.to_le_bytes());
        hasher.update(&1024u64.to_le_bytes());
        hasher.update(&5u32.to_le_bytes());
        let correct_crc32 = hasher.finalize();

        // Create footer with correct CRC32
        let footer_with_crc = Footer {
            magic: FOOTER_MAGIC,
            version: FORMAT_VERSION,
            index_offset: 1024,
            index_entries: 5,
            footer_crc32: correct_crc32,
        };

        // Validation should succeed
        assert!(footer_with_crc.validate_crc32().is_ok());
    }

    #[test]
    fn test_footer_validate_crc32_invalid() {
        // Create a footer with incorrect CRC32
        let footer = Footer {
            magic: FOOTER_MAGIC,
            version: FORMAT_VERSION,
            index_offset: 1024,
            index_entries: 5,
            footer_crc32: 0xDEADBEEF, // Wrong CRC32
        };

        // Validation should fail
        match footer.validate_crc32() {
            Err(BlobBundleError::FooterCrcMismatch { expected, actual }) => {
                assert_eq!(expected, 0xDEADBEEF);
                // The actual CRC32 should be calculated correctly
                let mut hasher = Hasher::new();
                hasher.update(&FOOTER_MAGIC);
                hasher.update(&FORMAT_VERSION.to_le_bytes());
                hasher.update(&1024u64.to_le_bytes());
                hasher.update(&5u32.to_le_bytes());
                assert_eq!(actual, hasher.finalize());
            }
            _ => panic!("Expected FooterCrcMismatch error"),
        }
    }

    #[test]
    fn test_footer_crc32_roundtrip() {
        // Test that write_to and read_from produce matching CRC32
        let footer = Footer::new(2048, 10);

        // Write footer to bytes
        let mut buffer = Vec::new();
        footer
            .write_to(&mut buffer)
            .expect("Failed to write footer");

        // Read it back
        let mut cursor = Cursor::new(&buffer);
        let read_footer = Footer::read_from(&mut cursor).expect("Failed to read footer");

        // The read footer should have a valid CRC32
        assert!(read_footer.validate_crc32().is_ok());

        // Values should match
        assert_eq!(read_footer.index_offset, 2048);
        assert_eq!(read_footer.index_entries, 10);
    }

    #[test]
    fn test_build_result_index_map() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test files with known sizes
        let file1_path = temp_dir.path().join("first.txt");
        let file2_path = temp_dir.path().join("second.txt");
        let file3_path = temp_dir.path().join("third.txt");

        fs::write(&file1_path, b"Hello").expect("Failed to write file1"); // 5 bytes
        fs::write(&file2_path, b"World!").expect("Failed to write file2"); // 6 bytes
        fs::write(&file3_path, b"Testing").expect("Failed to write file3"); // 7 bytes

        // Build blob bundle
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");

        let file_paths = vec![&file1_path, &file2_path, &file3_path];
        let result = builder
            .build(&file_paths, &output_path)
            .expect("Failed to build bundle");

        // Verify index map has correct order and values
        assert_eq!(result.index_map.len(), 3);

        // Check IDs are in insertion order
        assert_eq!(result.index_map[0].0, "first.txt");
        assert_eq!(result.index_map[1].0, "second.txt");
        assert_eq!(result.index_map[2].0, "third.txt");

        // Check lengths
        assert_eq!(result.index_map[0].1 .1, 5); // "Hello" length
        assert_eq!(result.index_map[1].1 .1, 6); // "World!" length
        assert_eq!(result.index_map[2].1 .1, 7); // "Testing" length

        // Check offsets are sequential (header is magic + version)
        let header_size = (MAGIC_SIZE + VERSION_SIZE) as u64;
        assert_eq!(result.index_map[0].1 .0, header_size); // First data starts after header
        assert_eq!(result.index_map[1].1 .0, header_size + 5); // Second after first
        assert_eq!(result.index_map[2].1 .0, header_size + 11); // Third after second
    }

    #[test]
    fn test_blob_size_limit() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Test with small number of shards (10) which has a smaller max blob size
        let n_shards = NonZeroU16::new(10).expect("10 is non-zero");
        let builder = BlobBundleBuilder::new(n_shards);

        // Calculate max blob size for 10 shards
        let max_size = walrus_core::encoding::max_blob_size_for_n_shards(
            n_shards,
            walrus_core::EncodingType::RS2,
        );

        // Create a file that's close to the limit
        let large_file_path = temp_dir.path().join("large.bin");
        let large_data_size =
            usize::try_from(max_size).expect("max_size should fit in usize") - 1000;
        fs::write(&large_file_path, vec![0u8; large_data_size])
            .expect("Failed to write large file");

        let output_path = temp_dir.path().join("bundle.blob");

        // This should succeed
        let result = builder.build(&[&large_file_path], &output_path);
        assert!(result.is_ok());

        // Now try with a file that definitely exceeds the limit
        let too_large_file_path = temp_dir.path().join("too_large.bin");
        let too_large_data =
            vec![0u8; usize::try_from(max_size).expect("max_size should fit in usize") + 1];
        fs::write(&too_large_file_path, too_large_data).expect("Failed to write too large file");

        let builder2 = BlobBundleBuilder::new(n_shards);
        let output_path2 = temp_dir.path().join("bundle2.blob");

        // This should fail
        let result2 = builder2.build(&[&too_large_file_path], &output_path2);
        match result2 {
            Err(BlobBundleError::BlobSizeExceeded {
                size,
                max_size: max,
                n_shards: shards,
            }) => {
                assert_eq!(shards, 10);
                assert!(size > max);
            }
            _ => panic!("Expected BlobSizeExceeded error"),
        }
    }

    #[test]
    fn test_read_range() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test files
        let file1_path = temp_dir.path().join("first.txt");
        let file2_path = temp_dir.path().join("second.txt");
        let file3_path = temp_dir.path().join("third.txt");

        fs::write(&file1_path, b"Hello").expect("Failed to write file1");
        fs::write(&file2_path, b"World").expect("Failed to write file2");
        fs::write(&file3_path, b"Test").expect("Failed to write file3");

        // Build blob bundle
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");
        let file_paths = vec![&file1_path, &file2_path, &file3_path];
        let result = builder
            .build(&file_paths, &output_path)
            .expect("Failed to build bundle");

        // Read the bundle
        let bundle_data = fs::read(&output_path).expect("Failed to read bundle");
        let reader =
            BlobBundleReader::new(Bytes::from(bundle_data)).expect("Failed to create reader");

        // Test reading using the index map offsets
        for (id, (offset, length)) in &result.index_map {
            let data_from_range = reader
                .read_range(*offset, *length)
                .expect("Failed to read range");

            // Compare with data retrieved through the normal get method
            let data_from_get = reader.get(id).expect("Failed to get entry");
            assert_eq!(data_from_range, data_from_get);
        }

        // Test reading specific ranges
        // Read first 5 bytes from first entry (should be "Hello")
        let first_offset = result.index_map[0].1 .0;
        let hello_bytes = reader
            .read_range(first_offset, 5)
            .expect("Failed to read range");
        assert_eq!(hello_bytes, Bytes::from("Hello"));

        // Read partial data from second entry
        let second_offset = result.index_map[1].1 .0;
        let partial_world = reader
            .read_range(second_offset, 3)
            .expect("Failed to read range");
        assert_eq!(partial_world, Bytes::from("Wor"));

        // Test reading header (first bytes)
        let header_bytes = reader
            .read_range(0, (MAGIC_SIZE + VERSION_SIZE) as u64)
            .expect("Failed to read header bytes");
        assert_eq!(&header_bytes[0..MAGIC_SIZE], &HEADER_MAGIC);

        // Test error case: reading beyond the end
        let total_size = reader.total_size() as u64;
        let result = reader.read_range(total_size - 1, 2);
        assert!(matches!(result, Err(BlobBundleError::InvalidFormat)));

        // Test error case: offset beyond the end
        let result = reader.read_range(total_size + 1, 1);
        assert!(matches!(result, Err(BlobBundleError::InvalidFormat)));

        // Test edge case: reading zero bytes
        let empty = reader.read_range(0, 0).expect("Failed to read empty range");
        assert_eq!(empty.len(), 0);

        // Test reading exact boundary
        let all_data = reader
            .read_range(0, total_size)
            .expect("Failed to read all data");
        assert_eq!(
            all_data.len(),
            usize::try_from(total_size).expect("total_size should fit in usize")
        );
    }

    #[test]
    fn test_read_range_with_index_map() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test files with known content
        let file1_path = temp_dir.path().join("aaa.txt");
        let file2_path = temp_dir.path().join("bbb.txt");
        let file3_path = temp_dir.path().join("ccc.txt");

        let data1 = b"AAAAA"; // 5 bytes
        let data2 = b"BBBBBBBBBB"; // 10 bytes
        let data3 = b"CCCCCCC"; // 7 bytes

        fs::write(&file1_path, data1).expect("Failed to write file1");
        fs::write(&file2_path, data2).expect("Failed to write file2");
        fs::write(&file3_path, data3).expect("Failed to write file3");

        // Build blob bundle
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");
        let file_paths = vec![&file1_path, &file2_path, &file3_path];
        let result = builder
            .build(&file_paths, &output_path)
            .expect("Failed to build bundle");

        // Verify index map has correct offsets and lengths
        assert_eq!(result.index_map[0].0, "aaa.txt");
        assert_eq!(result.index_map[0].1 .1, 5); // length of "AAAAA"

        assert_eq!(result.index_map[1].0, "bbb.txt");
        assert_eq!(result.index_map[1].1 .1, 10); // length of "BBBBBBBBBB"

        assert_eq!(result.index_map[2].0, "ccc.txt");
        assert_eq!(result.index_map[2].1 .1, 7); // length of "CCCCCCC"

        // Read the bundle
        let bundle_data = fs::read(&output_path).expect("Failed to read bundle");
        let reader =
            BlobBundleReader::new(Bytes::from(bundle_data)).expect("Failed to create reader");

        // Use index_map to read each entry directly
        let entry1 = reader
            .read_range(result.index_map[0].1 .0, result.index_map[0].1 .1)
            .expect("Failed to read entry1 range");
        assert_eq!(entry1, Bytes::from(data1.to_vec()));

        let entry2 = reader
            .read_range(result.index_map[1].1 .0, result.index_map[1].1 .1)
            .expect("Failed to read entry2 range");
        assert_eq!(entry2, Bytes::from(data2.to_vec()));

        let entry3 = reader
            .read_range(result.index_map[2].1 .0, result.index_map[2].1 .1)
            .expect("Failed to read entry3 range");
        assert_eq!(entry3, Bytes::from(data3.to_vec()));
    }

    #[test]
    fn test_reader_validation() {
        // Test 1: Too small data (less than header + footer)
        let small_data = Bytes::from(vec![0u8; 10]);
        let result = BlobBundleReader::new(small_data);
        assert!(matches!(result, Err(BlobBundleError::InvalidFormat)));

        // Test 2: Invalid header magic
        let invalid_header = vec![0xFFu8; 100];
        let result = BlobBundleReader::new(Bytes::from(invalid_header));
        assert!(matches!(result, Err(BlobBundleError::InvalidHeaderMagic)));

        // Test 3: Valid bundle passes all checks - using an already tested bundle
        // Create a simple valid bundle file
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let test_file = temp_dir.path().join("test.txt");
        fs::write(&test_file, b"data").expect("Failed to write test file");

        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");
        let _ = builder
            .build(&[&test_file], &output_path)
            .expect("Failed to build bundle");

        let bundle_data = fs::read(&output_path).expect("Failed to read bundle");
        let reader = BlobBundleReader::new(Bytes::from(bundle_data.clone()));
        assert!(reader.is_ok());

        // Test 4: Mismatched versions between header and footer
        let mut data = bundle_data.clone();
        // Change version in header (at offset 4-7) to be different
        data[4] = 2; // Change version from 1 to 2 in header
        let result = BlobBundleReader::new(Bytes::from(data));
        // This will fail with UnsupportedVersion error first (from Header::read_from)
        assert!(matches!(
            result,
            Err(BlobBundleError::UnsupportedVersion(_))
        ));

        // Test 5: Invalid footer magic - reuse the bundle from test 3
        let mut data = bundle_data.clone();
        // Corrupt footer magic (last 24 bytes, first 4 are magic)
        let footer_start = data.len() - Footer::size();
        data[footer_start] = 0xFF;
        let result = BlobBundleReader::new(Bytes::from(data));
        assert!(matches!(result, Err(BlobBundleError::InvalidFooterMagic)));

        // Test 6: Invalid index offset in footer - reuse the bundle from test 3
        let mut data = bundle_data.clone();

        // Set index_offset to 0 (which is invalid as it's less than header size)
        // Footer layout: magic(4) + version(4) + index_offset(8) + ...
        let footer_start = data.len() - Footer::size();
        let index_offset_pos = footer_start + 8; // After magic and version
        for i in 0..8 {
            data[index_offset_pos + i] = 0;
        }

        // Need to recalculate CRC32 for the modified footer
        let mut hasher = Hasher::new();
        hasher.update(&data[footer_start..footer_start + 4]); // magic
        hasher.update(&data[footer_start + 4..footer_start + 8]); // version
        hasher.update(&data[footer_start + 8..footer_start + 16]); // index_offset
        hasher.update(&data[footer_start + 16..footer_start + 20]); // index_entries
        let new_crc = hasher.finalize();
        data[footer_start + 20..footer_start + 24].copy_from_slice(&new_crc.to_le_bytes());

        let result = BlobBundleReader::new(Bytes::from(data));
        assert!(matches!(result, Err(BlobBundleError::InvalidFormat)));
    }

    #[test]
    fn test_footer_corrupted_data() {
        // Create a valid footer and serialize it
        let footer = Footer::new(4096, 20);
        let mut buffer = Vec::new();
        footer
            .write_to(&mut buffer)
            .expect("Failed to write footer");

        // Corrupt the index_offset bytes (but not the CRC32)
        // Footer layout: magic(4) + version(4) + index_offset(8) + index_entries(4) + crc32(4)
        buffer[8] ^= 0xFF; // Flip bits in the index_offset field (starts at byte 8)

        // Try to read the corrupted footer
        let mut cursor = Cursor::new(&buffer);
        let corrupted_footer = Footer::read_from(&mut cursor);

        // Should fail with CRC mismatch
        match corrupted_footer {
            Err(BlobBundleError::FooterCrcMismatch { .. }) => {}
            _ => panic!("Expected FooterCrcMismatch error for corrupted footer"),
        }
    }

    #[test]
    fn test_lazy_index_parsing_and_read_range_without_index() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test files
        let file1_path = temp_dir.path().join("entry1.txt");
        let file2_path = temp_dir.path().join("entry2.txt");
        let file3_path = temp_dir.path().join("entry3.txt");

        let data1 = b"Hello World!";
        let data2 = b"This is a test";
        let data3 = b"Lazy loading works";

        fs::write(&file1_path, data1).expect("Failed to write file1");
        fs::write(&file2_path, data2).expect("Failed to write file2");
        fs::write(&file3_path, data3).expect("Failed to write file3");

        // Build blob bundle
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");
        let file_paths = vec![&file1_path, &file2_path, &file3_path];
        let result = builder
            .build(&file_paths, &output_path)
            .expect("Failed to build bundle");

        // Read the bundle
        let bundle_data = fs::read(&output_path).expect("Failed to read bundle");
        let reader =
            BlobBundleReader::new(Bytes::from(bundle_data)).expect("Failed to create reader");

        // Verify that index is not parsed initially
        assert!(
            reader.index.borrow().is_none(),
            "Index should not be parsed on creation"
        );

        // Test read_range works without parsing index - read first entry
        let first_offset = result.index_map[0].1 .0;
        let first_length = result.index_map[0].1 .1;
        let data_from_range = reader
            .read_range(first_offset, first_length)
            .expect("read_range should work without index");
        assert_eq!(
            data_from_range,
            Bytes::from(data1.to_vec()),
            "Data from read_range should match original"
        );

        // Verify index is still not parsed after read_range
        assert!(
            reader.index.borrow().is_none(),
            "Index should remain unparsed after read_range"
        );

        // Test read_range with partial data - read middle of second entry
        let second_offset = result.index_map[1].1 .0;
        let partial_data = reader
            .read_range(second_offset + 5, 4)
            .expect("Partial read_range should work without index");
        assert_eq!(
            partial_data,
            Bytes::from("is a"),
            "Partial read should match expected substring"
        );

        // Verify index is still not parsed
        assert!(
            reader.index.borrow().is_none(),
            "Index should still not be parsed"
        );

        // Test read_range with third entry
        let third_offset = result.index_map[2].1 .0;
        let third_length = result.index_map[2].1 .1;
        let third_data = reader
            .read_range(third_offset, third_length)
            .expect("read_range should work for third entry without index");
        assert_eq!(third_data, Bytes::from(data3.to_vec()));

        // Index should still be unparsed
        assert!(
            reader.index.borrow().is_none(),
            "Index should remain unparsed after multiple read_range calls"
        );

        // Now call a method that requires the index
        let entry1 = reader.get("entry1.txt").expect("Failed to get entry1");
        assert_eq!(entry1, Bytes::from(data1.to_vec()));

        // Verify index is now parsed
        assert!(
            reader.index.borrow().is_some(),
            "Index should be parsed after get()"
        );

        // Verify read_range still works after index is parsed
        let data_after_index = reader
            .read_range(first_offset, first_length)
            .expect("read_range should still work after index is parsed");
        assert_eq!(data_after_index, Bytes::from(data1.to_vec()));

        // Test that subsequent index-requiring calls use the cached index
        let entry2 = reader.get("entry2.txt").expect("Failed to get entry2");
        assert_eq!(entry2, Bytes::from(data2.to_vec()));

        // Verify list_ids uses the cached index
        let mut ids = reader.list_ids().expect("Failed to list IDs");
        ids.sort();
        assert_eq!(ids, vec!["entry1.txt", "entry2.txt", "entry3.txt"]);
    }

    #[test]
    fn test_list_ids_order_matches_index() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test files with specific names to test ordering.
        let file1_path = temp_dir.path().join("zebra.txt");
        let file2_path = temp_dir.path().join("apple.txt");
        let file3_path = temp_dir.path().join("middle.txt");
        let file4_path = temp_dir.path().join("001.txt");

        fs::write(&file1_path, b"zebra content").expect("Failed to write file1");
        fs::write(&file2_path, b"apple content").expect("Failed to write file2");
        fs::write(&file3_path, b"middle content").expect("Failed to write file3");
        fs::write(&file4_path, b"001 content").expect("Failed to write file4");

        // Build blob bundle with files in this specific order.
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");
        let file_paths = vec![&file1_path, &file2_path, &file3_path, &file4_path];
        let result = builder
            .build(&file_paths, &output_path)
            .expect("Failed to build bundle");

        // The index_map should preserve the order of files as they were added.
        let expected_order: Vec<String> = vec![
            "zebra.txt".to_string(),
            "apple.txt".to_string(),
            "middle.txt".to_string(),
            "001.txt".to_string(),
        ];
        let index_order: Vec<String> = result.index_map.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(
            index_order, expected_order,
            "Index map should preserve input order"
        );

        // Read the bundle.
        let bundle_data = fs::read(&output_path).expect("Failed to read bundle");
        let reader =
            BlobBundleReader::new(Bytes::from(bundle_data)).expect("Failed to create reader");

        // list_ids should return IDs in the same order as they appear in the index.
        let ids = reader.list_ids().expect("Failed to list IDs");
        assert_eq!(ids, expected_order, "list_ids should preserve index order");

        // Verify that the order is NOT alphabetical (which would be different).
        let mut alphabetical = expected_order.clone();
        alphabetical.sort();
        assert_ne!(ids, alphabetical, "Order should not be alphabetical");
        assert_eq!(
            alphabetical,
            vec!["001.txt", "apple.txt", "middle.txt", "zebra.txt"]
        );

        // Verify entries() also preserves order.
        let entries = reader.entries().expect("Failed to get entries");
        let entry_ids: Vec<String> = entries.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(
            entry_ids, expected_order,
            "entries() should preserve index order"
        );

        // Verify data integrity for each entry.
        assert_eq!(
            reader.get("zebra.txt").expect("Failed to get zebra.txt"),
            Bytes::from("zebra content")
        );
        assert_eq!(
            reader.get("apple.txt").expect("Failed to get apple.txt"),
            Bytes::from("apple content")
        );
        assert_eq!(
            reader.get("middle.txt").expect("Failed to get middle.txt"),
            Bytes::from("middle content")
        );
        assert_eq!(
            reader.get("001.txt").expect("Failed to get 001.txt"),
            Bytes::from("001 content")
        );
    }

    #[test]
    fn test_build_in_memory() {
        // Create temporary directory for test files.
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test files.
        let file1_path = temp_dir.path().join("entry1.txt");
        let file2_path = temp_dir.path().join("entry2.txt");
        let file3_path = temp_dir.path().join("entry3.bin");

        let data1 = b"Hello, World!";
        let data2 = b"This is test data";
        let data3 = vec![1, 2, 3, 4, 5];

        fs::write(&file1_path, data1).expect("Failed to write file1");
        fs::write(&file2_path, data2).expect("Failed to write file2");
        fs::write(&file3_path, &data3).expect("Failed to write file3");

        // Build blob bundle in memory.
        let builder = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let file_paths = vec![&file1_path, &file2_path, &file3_path];
        let result = builder
            .build_in_memory(&file_paths)
            .expect("Failed to build bundle in memory");

        // Verify index map.
        assert_eq!(result.index_map.len(), 3);
        assert_eq!(result.index_map[0].0, "entry1.txt");
        assert_eq!(result.index_map[1].0, "entry2.txt");
        assert_eq!(result.index_map[2].0, "entry3.bin");

        // Verify bundle bytes are not empty.
        assert!(!result.bundle.is_empty());

        // Read blob bundle from memory.
        let reader = BlobBundleReader::new(Bytes::from(result.bundle))
            .expect("Failed to create reader from in-memory bundle");

        // Verify all entries.
        assert_eq!(
            reader.get("entry1.txt").expect("Failed to get entry1"),
            Bytes::from(data1.to_vec())
        );
        assert_eq!(
            reader.get("entry2.txt").expect("Failed to get entry2"),
            Bytes::from(data2.to_vec())
        );
        assert_eq!(
            reader.get("entry3.bin").expect("Failed to get entry3"),
            Bytes::from(data3.clone())
        );

        // Verify IDs.
        let mut ids = reader.list_ids().expect("Failed to list IDs");
        ids.sort();
        assert_eq!(ids, vec!["entry1.txt", "entry2.txt", "entry3.bin"]);
    }

    #[test]
    fn test_build_in_memory_vs_build_to_file() {
        // Test that build_in_memory produces the same output as build.
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create test files.
        let file1_path = temp_dir.path().join("test1.txt");
        let file2_path = temp_dir.path().join("test2.txt");

        let data1 = b"Test data 1";
        let data2 = b"Test data 2";

        fs::write(&file1_path, data1).expect("Failed to write file1");
        fs::write(&file2_path, data2).expect("Failed to write file2");

        let file_paths = vec![&file1_path, &file2_path];

        // Build in memory.
        let builder1 = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let memory_result = builder1
            .build_in_memory(&file_paths)
            .expect("Failed to build in memory");

        // Build to file.
        let builder2 = BlobBundleBuilder::new(NonZeroU16::new(10).expect("10 is non-zero"));
        let output_path = temp_dir.path().join("bundle.blob");
        let file_result = builder2
            .build(&file_paths, &output_path)
            .expect("Failed to build to file");

        // Read file bundle.
        let file_bundle = fs::read(&output_path).expect("Failed to read file bundle");

        // Verify bundles are identical.
        assert_eq!(memory_result.bundle, file_bundle);

        // Verify index maps are identical.
        assert_eq!(memory_result.index_map, file_result.index_map);

        // Verify both can be read correctly.
        let memory_reader = BlobBundleReader::new(Bytes::from(memory_result.bundle))
            .expect("Failed to create reader from memory");
        let file_reader = BlobBundleReader::new(Bytes::from(file_bundle))
            .expect("Failed to create reader from file");

        assert_eq!(
            memory_reader.get("test1.txt").expect("Failed to get test1"),
            file_reader.get("test1.txt").expect("Failed to get test1")
        );
        assert_eq!(
            memory_reader.get("test2.txt").expect("Failed to get test2"),
            file_reader.get("test2.txt").expect("Failed to get test2")
        );
    }

    #[test]
    fn test_build_in_memory_no_padding() {
        // Create temporary directory for test files.
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let file1_path = temp_dir.path().join("test1.txt");
        let file2_path = temp_dir.path().join("test2.txt");

        // Create test files.
        fs::write(&file1_path, b"Hello, world!").expect("Failed to write test file");
        fs::write(&file2_path, b"Blob bundle test content").expect("Failed to write test file");

        let n_shards = NonZeroU16::new(100).unwrap();
        let builder = BlobBundleBuilder::new(n_shards);

        let result = builder
            .build_in_memory(&[file1_path, file2_path])
            .expect("Failed to build in memory");

        // Verify that the buffer has no padding: capacity should equal length.
        assert_eq!(
            result.bundle.capacity(),
            result.bundle.len(),
            "buffer capacity ({}) should equal length ({}) with no padding",
            result.bundle.capacity(),
            result.bundle.len()
        );

        // Additionally verify the bundle is valid and can be read.
        let reader = BlobBundleReader::new(Bytes::from(result.bundle))
            .expect("Failed to create reader from memory");

        assert_eq!(
            reader.get("test1.txt").expect("Failed to get test1"),
            Bytes::from("Hello, world!")
        );
        assert_eq!(
            reader.get("test2.txt").expect("Failed to get test2"),
            Bytes::from("Blob bundle test content")
        );
    }

    #[test]
    fn test_build_in_memory_exceeds_estimated_capacity() {
        // Create temporary directory for test files.
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create multiple files with varying sizes to potentially exceed initial capacity estimate.
        // Use long file names to increase index overhead.
        let mut file_paths = Vec::new();
        let mut expected_contents = Vec::new();

        for i in 0..20 {
            let file_name = format!(
                "very_long_file_name_to_increase_index_overhead_{:03}.txt",
                i
            );
            let file_path = temp_dir.path().join(&file_name);
            // Create content that grows progressively larger.
            let content = format!("File {} content: {}", i, "x".repeat(100 * (i + 1)));
            fs::write(&file_path, &content).expect("Failed to write test file");
            file_paths.push(file_path);
            expected_contents.push((file_name, content));
        }

        let n_shards = NonZeroU16::new(100).unwrap();
        let builder = BlobBundleBuilder::new(n_shards);

        // Get the estimated size for comparison.
        let estimated_size =
            BlobBundleBuilder::estimate_size(&file_paths).expect("Failed to estimate size");

        let result = builder
            .build_in_memory(&file_paths)
            .expect("Failed to build in memory");

        // The actual size might be slightly different from estimate.
        // What matters is that Vec can grow if needed.
        println!(
            "estimated size: {}, actual size: {}",
            estimated_size,
            result.bundle.len()
        );

        // Verify that the buffer has no padding.
        assert_eq!(
            result.bundle.capacity(),
            result.bundle.len(),
            "buffer capacity should equal length with no padding"
        );

        // Verify the bundle is valid and can be read.
        let reader = BlobBundleReader::new(Bytes::from(result.bundle))
            .expect("Failed to create reader from memory");

        // Verify all files are included and have correct content.
        assert_eq!(
            result.index_map.len(),
            expected_contents.len(),
            "index map should contain all files"
        );

        for (file_name, expected_content) in &expected_contents {
            let actual_content = reader
                .get(file_name)
                .unwrap_or_else(|e| panic!("Failed to get file {}: {}", file_name, e));
            assert_eq!(
                actual_content,
                Bytes::from(expected_content.clone()),
                "content mismatch for file: {}",
                file_name
            );
        }

        // Verify that all entries in the index map exist and can be retrieved.
        for (file_name, (_offset, length)) in &result.index_map {
            assert!(
                *length > 0,
                "file {} should have non-zero length",
                file_name
            );
            let content = reader
                .get(file_name)
                .unwrap_or_else(|e| panic!("Failed to get file from index {}: {}", file_name, e));
            assert!(
                !content.is_empty(),
                "content should not be empty for file: {}",
                file_name
            );
        }
    }
}
