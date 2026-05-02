package org.example.centralstation.bitcask;

import org.example.centralstation.model.WeatherStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Public API for the Bitcask storage engine.
 *
 * Write flow (append-only, Bitcask-style):
 *   1. Serialize value → bytes
 *   2. Append to the active segment file (SegmentManager)
 *   3. Update the in-memory KeyDir with the returned RecordPointer
 *
 * Read flow:
 *   1. Look up KeyDir for the RecordPointer
 *   2. Ask SegmentManager to read the bytes at that position
 *   3. Deserialize and return
 */
@Service
public class BitcaskEngine {

    private static final Logger log = LoggerFactory.getLogger(BitcaskEngine.class);

    private final KeyDir keyDir;
    private final SegmentManager segmentManager;
    private final BitcaskSerializer serializer;

    public BitcaskEngine(KeyDir keyDir, SegmentManager segmentManager, BitcaskSerializer serializer) {
        this.keyDir = keyDir;
        this.segmentManager = segmentManager;
        this.serializer = serializer;
    }

    /**
     * Stores the latest status for a station.
     * Thread-safe: SegmentManager holds the write lock internally.
     */
    public void put(WeatherStatus status) throws IOException {
        String key = String.valueOf(status.stationId());
        byte[] value = serializer.serialize(status);

        // 1. Append to file  →  2. Update KeyDir (order matters for correctness)
        RecordPointer pointer = segmentManager.append(key, value);
        keyDir.put(key, pointer);

        log.debug("PUT station={} → file={} pos={}", key, pointer.fileId(), pointer.valuePosition());
    }

    /**
     * Returns the latest WeatherStatus for the given stationId, or null if not found.
     */
    public WeatherStatus get(String stationId) throws IOException {
        RecordPointer pointer = keyDir.get(stationId);
        if (pointer == null) {
            return null;
        }
        byte[] bytes = segmentManager.read(pointer);
        return serializer.deserialize(bytes);
    }

    /**
     * Returns the latest status for every known station.
     */
    public List<WeatherStatus> getAll() throws IOException {
        Map<String, RecordPointer> pointers = keyDir.getAllPointers();
        List<WeatherStatus> results = new ArrayList<>(pointers.size());

        for (Map.Entry<String, RecordPointer> entry : pointers.entrySet()) {
            byte[] bytes = segmentManager.read(entry.getValue());
            results.add(serializer.deserialize(bytes));
        }
        return results;
    }
}