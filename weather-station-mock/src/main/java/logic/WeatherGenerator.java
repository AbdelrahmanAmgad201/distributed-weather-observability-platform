package logic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import data.BatteryStatus;
import data.Weather;
import data.WeatherStatus;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class WeatherGenerator {
	private final long stationId;
	private final AtomicLong seq = new AtomicLong(0);
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
	private final KafkaWeatherProducer producer;
	private final ObjectMapper mapper = new ObjectMapper();
	private final Random random = new Random();

	public WeatherGenerator(long stationId, KafkaWeatherProducer producer) {
		this.stationId = stationId;
		this.producer = producer;
	}

	public void start(long initialDelayMs) {
		executor.scheduleAtFixedRate(this::produceOnce, initialDelayMs, 1000, TimeUnit.MILLISECONDS);
		Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
	}

	private void produceOnce() {
		try {
			// simulate 10% drop
			if (random.nextDouble() < 0.10) {
				System.out.println("[station:" + stationId + "] DROPPED message (simulated)");
				seq.incrementAndGet();
				return;
			}

			long sNo = seq.incrementAndGet();
			BatteryStatus battery = randomBatteryStatus();
			long timestamp = Instant.now().getEpochSecond();

			int humidity = random.nextInt(101);
			int temperature =  -20 + random.nextInt(140); // -20 to 119 F roughly
			int windSpeed = random.nextInt(201); // 0-200 km/h

			Weather w = new Weather(humidity, temperature, windSpeed);
			WeatherStatus status = new WeatherStatus(stationId, sNo, battery, timestamp, w);

			String json = toJson(status);

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
		} catch (Exception ex) {
			System.err.println("[station:" + stationId + "] Error producing message: " + ex.getMessage());
		}
	}

	private BatteryStatus randomBatteryStatus() {
		double v = random.nextDouble();
		if (v < 0.30) return BatteryStatus.low;
		if (v < 0.70) return BatteryStatus.medium;
		return BatteryStatus.high;
	}

	private String toJson(WeatherStatus status) throws JsonProcessingException {
		return mapper.writeValueAsString(status);
	}

	public void stop() {
		try {
			executor.shutdownNow();
			executor.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException ignored) {}
		try {
			producer.close();
		} catch (Exception ignored) {}
	}
}
