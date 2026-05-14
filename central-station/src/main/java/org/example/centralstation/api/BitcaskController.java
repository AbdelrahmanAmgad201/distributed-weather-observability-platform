package org.example.centralstation.api;

import org.example.centralstation.bitcask.BitcaskEngine;
import org.example.centralstation.model.WeatherStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api/bitcask")
public class BitcaskController {

    private final BitcaskEngine bitcaskEngine;

    public BitcaskController(BitcaskEngine bitcaskEngine) {
        this.bitcaskEngine = bitcaskEngine;
    }

    // Endpoint for: ./bitcask_client.sh --view --key=SOME_KEY
    @GetMapping("/{stationId}")
    public ResponseEntity<WeatherStatus> getByKey(@PathVariable String stationId) {
        try {
            WeatherStatus status = bitcaskEngine.get(stationId);
            if (status == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(status);
        } catch (IOException e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    // Endpoint for: ./bitcask_client.sh --view-all
    @GetMapping(value = "/all/csv", produces = "text/csv")
    public ResponseEntity<String> getAllAsCsv() {
        try {
            List<WeatherStatus> allStatuses = bitcaskEngine.getAll();
            StringBuilder csv = new StringBuilder();
            csv.append("key,value\n");

            for (WeatherStatus status : allStatuses) {
                String key = String.valueOf(status.getStationId());
                String value = status.toString().replace("\"", "\"\""); // Basic escaping
                csv.append(key).append(",\"").append(value).append("\"\n");
            }

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"dump.csv\"")
                    .body(csv.toString());
        } catch (IOException e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}