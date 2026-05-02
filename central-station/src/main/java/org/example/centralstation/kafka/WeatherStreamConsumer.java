package org.example.centralstation.kafka;

import org.example.centralstation.model.WeatherStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class WeatherStreamConsumer {

    private static final Logger log = LoggerFactory.getLogger(WeatherStreamConsumer.class);

    // Utils
    //    private final BitcaskEngine bitcaskEngine;
    //    private final ParquetArchiver parquetArchiver;

    // public WeatherStreamConsumer(BitcaskEngine bitcaskEngine, ParquetArchiver parquetArchiver) {
    //     this.bitcaskEngine = bitcaskEngine;
    //     this.parquetArchiver = parquetArchiver;
    // }

    @KafkaListener(topics = "${app.topic.input}", groupId = "central-storage-group")
    public void consumeWeatherStatus(WeatherStatus status) {

        log.info("Received reading from Station: {}", status.stationId());

        try {
            // bitcaskEngine.put(status.stationId(), status);
            // parquetArchiver.add(status);

        } catch (Exception e) {
            log.error("Failed to process weather status for station {}", status.stationId(), e);
        }
    }

}
