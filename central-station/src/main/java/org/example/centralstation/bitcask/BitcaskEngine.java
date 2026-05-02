package org.example.centralstation.bitcask;

import org.example.centralstation.model.WeatherStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class BitcaskEngine {

    private final KeyDir keyDir;
    // private final SegmentManager segmentManager; // You will need this to handle the FileChannels
    // private final BitcaskSerializer serializer;  // You will need this to convert WeatherStatus <-> byte[]

    public BitcaskEngine(KeyDir keyDir /*, SegmentManager segmentManager, BitcaskSerializer serializer*/) {
        this.keyDir = keyDir;
        // this.segmentManager = segmentManager;
        // this.serializer = serializer;
    }

    public void put(WeatherStatus weatherStatus) {
        // HINT 1: Use the Serializer to convert weatherStatus into a byte[]
        // HINT 2: Tell the SegmentManager to append this byte[] to the active file.
        // HINT 3: The SegmentManager should return the offset and size of where it just wrote.
        // HINT 4: Create a new RecordPointer using that fileId, offset, and size.
        // HINT 5: Update the KeyDir with: keyDir.put(String.valueOf(weatherStatus.stationId()), pointer);
    }

    public WeatherStatus get(String stationId) {
        // HINT 1: Ask KeyDir for the RecordPointer for this stationId.
        // HINT 2: If null, return null.
        // HINT 3: Ask the SegmentManager to read 'valueSize' bytes from 'valuePosition' in 'fileId'.
        // HINT 4: Use the Serializer to convert those bytes back into a WeatherStatus record.
        return null;
    }

    public List<WeatherStatus> getAll() {
        // HINT 1: Call keyDir.getAllPointers().
        // HINT 2: Iterate through the pointers. For every pointer, do the exact same
        //         file read and deserialization as the get() method.
        // HINT 3: Collect them all into a List and return it.
        return null;
    }
}