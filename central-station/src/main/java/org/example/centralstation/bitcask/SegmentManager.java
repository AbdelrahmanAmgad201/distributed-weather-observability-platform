package org.example.centralstation.bitcask;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Manages all segment (.data) and hint (.hint) files on disk.
 *
 * On-disk record format (data file):
 *   [8B timestamp][4B keyLen][4B valueLen][keyLen bytes key][valueLen bytes value]
 *
 * On-disk hint record format (hint file):
 *   [8B timestamp][4B keyLen][8B valueSize][8B valuePosition][keyLen bytes key]
 *
 * Files are named by creation epoch-millis: e.g. "1714600000000.data"
 * Sorting by filename = sorting chronologically.
 * The one .data file that has NO matching .hint file is the active (writable) segment.
 */
@Component
public class SegmentManager {

    private static final Logger log = LoggerFactory.getLogger(SegmentManager.class);

    private static final long MAX_SEGMENT_SIZE = 4L * 1024 * 1024; // 4 MB
    private static final String DATA_EXT = ".data";
    private static final String HINT_EXT = ".hint";

    private final Path dataDir;
    private final KeyDir keyDir;

    // Serializes all writes; reads bypass this lock (positional FileChannel reads are thread-safe)
    private final ReentrantLock writeLock = new ReentrantLock();

    private String activeFileId;
    private FileChannel activeChannel;

    public SegmentManager(
            @Value("${bitcask.data-path}") String dataPath,
            KeyDir keyDir) {
        this.dataDir = Paths.get(dataPath);
        this.keyDir = keyDir;
    }

    // -------------------------------------------------------------------------
    // Startup
    // -------------------------------------------------------------------------

    @PostConstruct
    public void init() throws IOException {
        Files.createDirectories(dataDir);
        recoverKeyDir();
        openOrCreateActiveFile();
    }

    /**
     * Rebuilds the in-memory KeyDir from hint files (fast path) and
     * by scanning the active data file (no hint exists for it yet).
     */
    private void recoverKeyDir() throws IOException {
        List<String> fileIds = listDataFileIds();
        if (fileIds.isEmpty()) {
            log.info("No existing segments found — starting fresh.");
            return;
        }

        log.info("Recovering KeyDir from {} segment(s)...", fileIds.size());
        for (String fileId : fileIds) {
            if (Files.exists(dataDir.resolve(fileId + HINT_EXT))) {
                loadHintFile(fileId);
            } else {
                // Active (un-sealed) segment — scan the raw data file
                log.info("Scanning active data file for recovery: {}", fileId);
                scanDataFile(fileId);
            }
        }
        log.info("KeyDir recovery complete.");
    }

    /** Opens the existing active segment for appending, or creates a new one. */
    private void openOrCreateActiveFile() throws IOException {
        // The active file is the .data file with no matching .hint
        Optional<String> existing = listDataFileIds().stream()
                .filter(id -> !Files.exists(dataDir.resolve(id + HINT_EXT)))
                .reduce((first, second) -> second); // take the last (newest) one

        if (existing.isPresent()) {
            activeFileId = existing.get();
            Path path = dataDir.resolve(activeFileId + DATA_EXT);
            activeChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            activeChannel.position(activeChannel.size()); // seek to end for appending
            log.info("Resumed active segment: {} ({} bytes)", activeFileId, activeChannel.size());
        } else {
            createNewActiveFile();
        }
    }

    private void createNewActiveFile() throws IOException {
        activeFileId = String.valueOf(System.currentTimeMillis());
        Path path = dataDir.resolve(activeFileId + DATA_EXT);
        activeChannel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        log.info("Created new active segment: {}", activeFileId);
    }

    // -------------------------------------------------------------------------
    // Write path (locked)
    // -------------------------------------------------------------------------

    /**
     * Appends a key/value record to the active segment file.
     * Returns a RecordPointer that can be stored in KeyDir.
     * Rotates the segment if it exceeds MAX_SEGMENT_SIZE.
     */
    public RecordPointer append(String key, byte[] value) throws IOException {
        writeLock.lock();
        try {
            long timestamp = System.currentTimeMillis();
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            // Build binary record: [timestamp 8B][keyLen 4B][valueLen 4B][key][value]
            ByteBuffer buf = ByteBuffer.allocate(16 + keyBytes.length + value.length);
            buf.putLong(timestamp);
            buf.putInt(keyBytes.length);
            buf.putInt(value.length);
            buf.put(keyBytes);
            buf.put(value);
            buf.flip();

            // Capture state before writing
            long recordStart = activeChannel.position();
            long valuePosition = recordStart + 16L + keyBytes.length;
            String fileId = activeFileId;

            // Write the full record
            while (buf.hasRemaining()) {
                activeChannel.write(buf);
            }

            // Rotate if segment is full (next write will use the new file)
            if (activeChannel.size() >= MAX_SEGMENT_SIZE) {
                rotate();
            }

            return new RecordPointer(fileId, valuePosition, value.length, timestamp);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Seals the current active segment:
     *  1. Writes a hint file for it.
     *  2. Opens a new active segment.
     */
    private void rotate() throws IOException {
        log.info("Rotating segment: {}", activeFileId);
        String sealedFileId = activeFileId;
        activeChannel.force(true); // flush to disk
        activeChannel.close();

        writeHintFileForSegment(sealedFileId);
        createNewActiveFile();
    }

    // -------------------------------------------------------------------------
    // Read path (lock-free)
    // -------------------------------------------------------------------------

    /**
     * Reads the raw value bytes identified by the given pointer.
     * Uses a positional read, which is thread-safe on FileChannel.
     */
    public byte[] read(RecordPointer pointer) throws IOException {
        Path filePath = dataDir.resolve(pointer.fileId() + DATA_EXT);
        ByteBuffer buf = ByteBuffer.allocate((int) pointer.valueSize());

        // Re-use the active channel if reading from the active segment;
        // otherwise open a dedicated read channel for immutable segments.
        if (pointer.fileId().equals(activeFileId)) {
            activeChannel.read(buf, pointer.valuePosition());
        } else {
            try (FileChannel ch = FileChannel.open(filePath, StandardOpenOption.READ)) {
                ch.read(buf, pointer.valuePosition());
            }
        }
        return buf.array();
    }

    // -------------------------------------------------------------------------
    // Hint file I/O
    // -------------------------------------------------------------------------

    /**
     * Writes a hint file for the given (now-immutable) segment.
     * Scans KeyDir for all entries still pointing to that segment.
     *
     * Hint record: [timestamp 8B][keyLen 4B][valueSize 8B][valuePosition 8B][key bytes]
     */
    public void writeHintFileForSegment(String fileId) throws IOException {
        Path hintPath = dataDir.resolve(fileId + HINT_EXT);

        Map<String, RecordPointer> segmentEntries = keyDir.getAllPointers().entrySet().stream()
                .filter(e -> e.getValue().fileId().equals(fileId))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(hintPath.toFile())))) {

            for (Map.Entry<String, RecordPointer> e : segmentEntries.entrySet()) {
                byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
                RecordPointer ptr = e.getValue();
                dos.writeLong(ptr.timestamp());
                dos.writeInt(keyBytes.length);
                dos.writeLong(ptr.valueSize());
                dos.writeLong(ptr.valuePosition());
                dos.write(keyBytes);
            }
        }
        log.info("Wrote hint file for segment {} ({} entries)", fileId, segmentEntries.size());
    }

    /** Reads a hint file and populates KeyDir. */
    private void loadHintFile(String fileId) throws IOException {
        Path hintPath = dataDir.resolve(fileId + HINT_EXT);

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(hintPath.toFile())))) {
            try {
                while (true) {
                    long timestamp = dis.readLong();
                    int keyLen = dis.readInt();
                    long valueSize = dis.readLong();
                    long valuePosition = dis.readLong();
                    byte[] keyBytes = new byte[keyLen];
                    dis.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);
                    keyDir.put(key, new RecordPointer(fileId, valuePosition, valueSize, timestamp));
                }
            } catch (EOFException ignored) {
                // Normal end of file
            }
        }
    }

    /**
     * Scans a raw data file record-by-record to rebuild KeyDir.
     * Used for the active segment (which has no hint file yet).
     */
    private void scanDataFile(String fileId) throws IOException {
        Path dataPath = dataDir.resolve(fileId + DATA_EXT);

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(dataPath.toFile())))) {
            long position = 0;
            try {
                while (true) {
                    long timestamp = dis.readLong();
                    int keyLen = dis.readInt();
                    int valueLen = dis.readInt();

                    byte[] keyBytes = new byte[keyLen];
                    dis.readFully(keyBytes);
                    byte[] valueBytes = new byte[valueLen]; // read+discard value
                    dis.readFully(valueBytes);

                    String key = new String(keyBytes, StandardCharsets.UTF_8);
                    long valuePosition = position + 16L + keyLen;
                    keyDir.put(key, new RecordPointer(fileId, valuePosition, valueLen, timestamp));

                    position += 16L + keyLen + valueLen;
                }
            } catch (EOFException ignored) {
                // Normal end of file
            }
        }
    }

    // -------------------------------------------------------------------------
    // Compaction helpers
    // -------------------------------------------------------------------------

    /**
     * Returns all immutable (sealed) segment file IDs, sorted oldest-first.
     * These are .data files that have a corresponding .hint file.
     */
    public List<String> getImmutableSegmentIds() throws IOException {
        return listDataFileIds().stream()
                .filter(id -> Files.exists(dataDir.resolve(id + HINT_EXT)))
                .collect(Collectors.toList());
    }

    public String getActiveFileId() {
        return activeFileId;
    }

    /**
     * Writes entries into a brand-new compacted segment file (used by CompactionJob).
     * Returns a map of key → new RecordPointer for updating KeyDir.
     */
    public Map<String, RecordPointer> writeCompactedSegment(Map<String, byte[]> entries) throws IOException {
        if (entries.isEmpty()) return Collections.emptyMap();

        String compactedId = "compact_" + System.currentTimeMillis();
        Path compactedPath = dataDir.resolve(compactedId + DATA_EXT);
        Map<String, RecordPointer> newPointers = new LinkedHashMap<>();

        try (FileChannel ch = FileChannel.open(compactedPath,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            long position = 0;
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                String key = entry.getKey();
                byte[] value = entry.getValue();
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                long timestamp = System.currentTimeMillis();

                ByteBuffer buf = ByteBuffer.allocate(16 + keyBytes.length + value.length);
                buf.putLong(timestamp);
                buf.putInt(keyBytes.length);
                buf.putInt(value.length);
                buf.put(keyBytes);
                buf.put(value);
                buf.flip();
                ch.write(buf);

                long valuePosition = position + 16L + keyBytes.length;
                newPointers.put(key, new RecordPointer(compactedId, valuePosition, value.length, timestamp));
                position += 16L + keyBytes.length + value.length;
            }
        }

        // Write the hint file for the compacted segment immediately
        writeHintFile(compactedId, newPointers);
        log.info("Wrote compacted segment: {} ({} keys)", compactedId, entries.size());
        return newPointers;
    }

    /** Writes a hint file given an explicit pointer map (used after compaction). */
    public void writeHintFile(String fileId, Map<String, RecordPointer> pointers) throws IOException {
        Path hintPath = dataDir.resolve(fileId + HINT_EXT);

        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(hintPath.toFile())))) {

            for (Map.Entry<String, RecordPointer> e : pointers.entrySet()) {
                byte[] keyBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
                RecordPointer ptr = e.getValue();
                dos.writeLong(ptr.timestamp());
                dos.writeInt(keyBytes.length);
                dos.writeLong(ptr.valueSize());
                dos.writeLong(ptr.valuePosition());
                dos.write(keyBytes);
            }
        }
    }

    /** Deletes both the .data and .hint files for a given segment ID. */
    public void deleteSegment(String fileId) throws IOException {
        Files.deleteIfExists(dataDir.resolve(fileId + DATA_EXT));
        Files.deleteIfExists(dataDir.resolve(fileId + HINT_EXT));
        log.info("Deleted old segment: {}", fileId);
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /** Lists all .data file IDs in the data directory, sorted chronologically. */
    private List<String> listDataFileIds() throws IOException {
        try (var stream = Files.list(dataDir)) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(DATA_EXT))
                    .map(p -> p.getFileName().toString().replace(DATA_EXT, ""))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }
}
