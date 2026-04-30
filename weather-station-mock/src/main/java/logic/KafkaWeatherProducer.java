package logic;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaWeatherProducer implements AutoCloseable {
	private final KafkaProducer<String, String> producer;
	private final String topic;

	public KafkaWeatherProducer(String bootstrapServers, String topic) {
		this.topic = topic;
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "1");
		props.put("retries", 3);
		producer = new KafkaProducer<>(props);
	}

	public Future<RecordMetadata> send(String key, String value, Callback callback) {
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
		return producer.send(record, callback);
	}

	@Override
	public void close() {
		try {
			producer.flush();
		} catch (Exception ignored) {}
		producer.close();
	}
}
