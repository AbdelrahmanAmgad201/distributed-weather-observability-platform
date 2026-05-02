package org.example.centralstation.bitcask;

/**
 * An immutable DTO representing exactly where a value lives on disk.
 */
public record RecordPointer(
        String fileId,        // The name/timestamp of the active or read-only .data file
        long valuePosition,   // The exact byte offset where this record starts in the file
        long valueSize,       // How many bytes to read from the position
        long timestamp        // The time the record was written (used for compaction logic)
) {}