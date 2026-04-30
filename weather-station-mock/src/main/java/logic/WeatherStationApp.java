package logic;

import java.util.ArrayList;
import java.util.List;

public class WeatherStationApp {
    public static void main(String[] args){
        String bootstrap = System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
        String topic = System.getenv().getOrDefault("TOPIC_NAME", "weather-stations");
        String stationIdStr = System.getenv().getOrDefault("STATION_ID", "1");

        long stationId = 1L;
        try {
            stationId = Long.parseLong(stationIdStr);
        } catch (NumberFormatException e) {
            System.err.println("Invalid STATION_ID provided, defaulting to 1");
        }

        KafkaWeatherProducer producer = new KafkaWeatherProducer(bootstrap, topic);

        WeatherGenerator g = new WeatherGenerator(stationId, producer);
        g.start(0L);
        System.out.println("Started generator for station " + stationId + " targeting topic " + topic);

        // keep main thread alive
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down generator...");
            g.stop();
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {}
    }
}
