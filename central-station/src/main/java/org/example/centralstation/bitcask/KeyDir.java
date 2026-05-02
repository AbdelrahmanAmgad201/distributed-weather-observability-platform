package org.example.centralstation.bitcask;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KeyDir {

    private final Map<String, RecordPointer> keys = new ConcurrentHashMap<>();

    public KeyDir() {
        // HINT: This constructor runs when the Spring Boot app starts.
        // Step 1: Scan the storage directory for all `.hint` files.
        // Step 2: Read them sequentially and populate the 'keys' map.
        // Step 3: Find the active `.data` file (which has no hint file yet).
        // Step 4: Scan the active `.data` file to load the most recent keys into memory.
    }

    public void put(String stationId, RecordPointer pointer) {
        keys.put(stationId, pointer);
    }

    public RecordPointer get(String stationId) {
        return keys.get(stationId);
    }

    public Map<String, RecordPointer> getAllPointers() {
        return Map.copyOf(keys);
    }
}