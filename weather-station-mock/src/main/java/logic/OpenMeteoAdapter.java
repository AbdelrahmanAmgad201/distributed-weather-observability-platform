package logic;

import com.fasterxml.jackson.databind.ObjectMapper;
import data.BatteryStatus;
import data.OpenMeteoResponse;
import data.Weather;
import data.WeatherStatus;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class OpenMeteoAdapter {
    private final String API_URL; // Changed from static to instance variable
    
    private final KafkaWeatherProducer producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicLong seq = new AtomicLong(0);
    private final Random random = new Random();
    private final long stationId;

    public OpenMeteoAdapter(long stationId, KafkaWeatherProducer producer) {
        this.stationId = stationId;
        this.producer = producer;
        this.API_URL = generateURL();
    }

    private String generateURL() {
        double lat = -90.0 + (random.nextDouble() * 180.0);
        double lon = -180.0 + (random.nextDouble() * 360.0);
        return String.format(
            "https://api.open-meteo.com/v1/forecast?latitude=%.2f&longitude=%.2f&current=temperature_2m,relative_humidity_2m,wind_speed_10m",
            lat, lon
        );
    }

    public void start() {
        // Poll Open-Meteo every 1 second (Warning: this might trigger API limits quickly)
        executor.scheduleAtFixedRate(this::fetchAndSend, 0, 1, TimeUnit.SECONDS);
    }

    private void fetchAndSend() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(API_URL))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                OpenMeteoResponse apiData = mapper.readValue(response.body(), OpenMeteoResponse.class);
                
                // Map API data to your internal WeatherStatus record
                Weather weather = new Weather(
                        apiData.current().humidity(),
                        apiData.current().temperature().intValue(),
                        apiData.current().windSpeed().intValue()
                );

                long sNo = seq.incrementAndGet();

                WeatherStatus status = new WeatherStatus(
                        stationId, 
                        sNo, 
                        BatteryStatus.high, // External API "stations" are always fully powered!
                        Instant.now().getEpochSecond(), 
                        weather
                );

                String json = mapper.writeValueAsString(status);
                
                // Matched the Callback style from WeatherGenerator
                producer.send(String.valueOf(stationId), json, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("[station:" + stationId + "] Failed to send: " + exception.getMessage());
                        } else {
                            System.out.println("[station:" + stationId + "] Sent s_no=" + sNo + " to partition=" + metadata.partition() + " offset=" + metadata.offset());
                        }
                    }
                });
            }
        } catch (Exception ex) {
            // Matched the error output style from WeatherGenerator
            System.err.println("[station:" + stationId + "] Error producing message: " + ex.getMessage());
        }
    }

    public void stop() {
        executor.shutdown();
    }
}