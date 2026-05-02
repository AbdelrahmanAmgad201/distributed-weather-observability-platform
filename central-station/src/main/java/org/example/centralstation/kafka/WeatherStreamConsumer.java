package org.example.centralstation.kafka;

import org.example.centralstation.bitcask.BitcaskEngine;
import org.example.centralstation.model.WeatherStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class WeatherStreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(WeatherStreamConsumer.class);

    private final BitcaskEngine bitcaskEngine;

    public WeatherStreamConsumer(BitcaskEngine bitcaskEngine) {
        this.bitcaskEngine = bitcaskEngine;
    }

    @KafkaListener(topics = "${app.topic.input}", groupId = "central-storage-group")
    public void consumeWeatherStatus(WeatherStatus status) {
        log.info("Received reading from Station: {}", status.stationId());

        try {
            bitcaskEngine.put(status);
        } catch (Exception e) {
            log.error("Failed to store weather status for station {}", status.stationId(), e);
        }
    }
}
