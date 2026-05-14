package org.example.centralstation.bitcask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * Background compaction job — merges all immutable (sealed) segment files into one.
 *
 * Algorithm:
 *   1. Snapshot KeyDir → collect only entries whose pointer lives in an immutable segment.
 *   2. Read the raw value bytes for each such key.
 *   3. Write all of them into a single new compacted .data file + .hint file.
 *   4. Atomically update KeyDir entries (only if they haven't been overwritten by a fresh write).
 *   5. Delete the old .data and .hint files.
 *
 * The active segment is never touched, so reads and writes are never blocked.
 */
@Component
public class CompactionJob {

    private static final Logger log = LoggerFactory.getLogger(CompactionJob.class);

    private final KeyDir keyDir;
    private final SegmentManager segmentManager;
    private final BitcaskSerializer serializer;

    private volatile boolean running = false;

    public CompactionJob(KeyDir keyDir, SegmentManager segmentManager, BitcaskSerializer serializer) {
        this.keyDir = keyDir;
        this.segmentManager = segmentManager;
        this.serializer = serializer;
    }

    /** Runs every 5 minutes. Skips if a previous run is still ongoing. */
    @Scheduled(fixedDelayString = "${bitcask.compaction.interval-ms:300000}")
    public void compact() {
        if (running) {
            log.info("Compaction already in progress — skipping.");
            return;
        }
        running = true;
        try {
            doCompact();
        } catch (IOException e) {
            log.error("Compaction failed", e);
        } finally {
            running = false;
        }
    }

    private void doCompact() throws IOException {
        List<String> immutableIds = segmentManager.getImmutableSegmentIds();
        if (immutableIds.isEmpty()) {
            log.debug("No immutable segments to compact.");
            return;
        }

        log.info("Starting compaction of {} immutable segment(s): {}", immutableIds.size(), immutableIds);
        Set<String> immutableSet = new HashSet<>(immutableIds);

        // Step 1 — snapshot: find all KeyDir entries that live in an immutable segment
        Map<String, RecordPointer> snapshot = keyDir.getAllPointers();
        Map<String, RecordPointer> toCompact = new LinkedHashMap<>();
        for (Map.Entry<String, RecordPointer> e : snapshot.entrySet()) {
            if (immutableSet.contains(e.getValue().fileId())) {
                toCompact.put(e.getKey(), e.getValue());
            }
        }

        if (toCompact.isEmpty()) {
            log.info("All keys have already been superseded — cleaning up old segments.");
            deleteAll(immutableIds);
            return;
        }

        // Step 2 — read each value
        Map<String, byte[]> valueMap = new LinkedHashMap<>();
        for (Map.Entry<String, RecordPointer> e : toCompact.entrySet()) {
            valueMap.put(e.getKey(), segmentManager.read(e.getValue()));
        }

        // Step 3 — write compacted segment + hint
        Map<String, RecordPointer> newPointers = segmentManager.writeCompactedSegment(valueMap);

        // Step 4 — update KeyDir only for keys still pointing to the old segments
        int updated = 0;
        for (Map.Entry<String, RecordPointer> e : newPointers.entrySet()) {
            String key = e.getKey();
            String oldFileId = toCompact.get(key).fileId();
            if (keyDir.replaceIfSameFile(key, oldFileId, e.getValue())) {
                updated++;
            }
        }
        log.info("Updated {}/{} KeyDir entries after compaction.", updated, newPointers.size());

        // Step 5 — delete old segment files
        deleteAll(immutableIds);
    }

    private void deleteAll(List<String> fileIds) throws IOException {
        for (String fileId : fileIds) {
            segmentManager.deleteSegment(fileId);
        }
    }
}
