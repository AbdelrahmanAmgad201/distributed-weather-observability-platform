package org.example.centralstation.bitcask;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.centralstation.model.WeatherStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Handles conversion between WeatherStatus objects and raw bytes (JSON).
 */
@Component
public class BitcaskSerializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public byte[] serialize(WeatherStatus status) throws IOException {
        return objectMapper.writeValueAsBytes(status);
    }

    public WeatherStatus deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, WeatherStatus.class);
    }
}
