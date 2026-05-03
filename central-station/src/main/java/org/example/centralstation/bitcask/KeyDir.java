package org.example.centralstation.bitcask;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory index: maps stationId (String) → RecordPointer (where on disk the latest value lives).
 *
 * Uses ConcurrentHashMap so reads are always lock-free.
 * Writes happen under the SegmentManager's writeLock, which guarantees
 * that the file write completes before the KeyDir entry is updated.
 */
@Component
public class KeyDir {

    private final Map<String, RecordPointer> index = new ConcurrentHashMap<>();

    /** Update the pointer for a key. Called after a successful file append. */
    public void put(String key, RecordPointer pointer) {
        index.put(key, pointer);
    }

    /**
     * Replace a key's pointer only if it still points to the expected old segment.
     * Used during compaction to avoid overwriting a newer write that arrived mid-compaction.
     */
    public boolean replaceIfSameFile(String key, String expectedFileId, RecordPointer newPointer) {
        RecordPointer current = index.get(key);
        if (current != null && current.fileId().equals(expectedFileId)) {
            index.put(key, newPointer);
            return true;
        }
        return false;
    }

    /** Returns null if the key has never been written. */
    public RecordPointer get(String key) {
        return index.get(key);
    }

    /** Returns a snapshot of all current key→pointer mappings. */
    public Map<String, RecordPointer> getAllPointers() {
        return Map.copyOf(index);
    }
}