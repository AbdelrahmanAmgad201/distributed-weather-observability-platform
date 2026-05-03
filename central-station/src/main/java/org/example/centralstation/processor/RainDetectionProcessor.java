package org.example.centralstation.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.example.centralstation.model.WeatherStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafkaStreams
public class RainDetectionProcessor {

    @Value("${app.topic.input}")
    private String inputTopic;

    @Value("${app.topic.alerts}")
    private String alertsTopic;

    @Bean
    public KStream<String, WeatherStatus> processRainAlerts(StreamsBuilder builder) {

        // 1. Tell Kafka how to deserialize your JSON into the Java Record
        JsonSerde<WeatherStatus> weatherSerde = new JsonSerde<>(WeatherStatus.class);

        // 2. Consume from your main ingestion topic
        KStream<String, WeatherStatus> weatherStream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), weatherSerde)
        );

        // 3. The DSL Pipeline
        weatherStream
                // KEEP only records where humidity > 70%
                .filter((key, status) -> status != null && status.getWeather() != null && status.getWeather().getHumidity() != null && status.getWeather().getHumidity() > 70)

                // TRANSFORM the matched record into a "special message" string
                .mapValues(status -> String.format(
                        "RAIN ALERT: Station %d reporting %d%% humidity at timestamp %d",
                        status.getStationId(),
                        status.getWeather().getHumidity(),
                        status.getStatusTimestamp()
                ))

                // PUSH the special message to the target alert topic
                .to(alertsTopic, Produced.with(Serdes.String(), Serdes.String()));

        return weatherStream;
    }
}
