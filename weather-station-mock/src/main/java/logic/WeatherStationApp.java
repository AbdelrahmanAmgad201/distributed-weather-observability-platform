package logic;


public class WeatherStationApp {
    public static void main(String[] args){
        String bootstrap = System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
        String topic = System.getenv().getOrDefault("TOPIC_NAME", "weather-stations");
        String mode = System.getenv().getOrDefault("MODE", "random"); // "random" or "open-meteo"
        String stationIdStr = System.getenv().getOrDefault("STATION_ID", "1");

        long stationId = 1L;
        try {
            stationId = Long.parseLong(stationIdStr);
        } catch (NumberFormatException e) {
            System.err.println("Invalid STATION_ID provided, defaulting to 1");
        }

        KafkaWeatherProducer producer = new KafkaWeatherProducer(bootstrap, topic);

        if ("open-meteo".equalsIgnoreCase(mode)) {
            // Run Open-Meteo Adapter Mode
            OpenMeteoAdapter adapter = new OpenMeteoAdapter(stationId, producer);
            adapter.start();
            System.out.println("Pipeline running in OPEN-METEO mode. Adapter station (" + stationId + ") active.");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down Open-Meteo adapter...");
                adapter.stop();
                producer.close();
            }));

        } else {
            // Run Random Simulated Generator Mode (Default)
            WeatherGenerator simulated = new WeatherGenerator(stationId, producer);
            simulated.start(0L);
            System.out.println("Pipeline running in RANDOM mode. Simulated station (" + stationId + ") active.");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down simulated generator...");
                simulated.stop();
                producer.close();
            }));
        }

        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {}
    }
}
