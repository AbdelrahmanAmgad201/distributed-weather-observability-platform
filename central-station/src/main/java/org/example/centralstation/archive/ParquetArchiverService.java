package org.example.centralstation.archive;

import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.example.centralstation.model.WeatherStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Service
public class ParquetArchiverService {

    private static final Logger log = LoggerFactory.getLogger(ParquetArchiverService.class);
    private static final int BATCH_SIZE = 1000;

    @org.springframework.beans.factory.annotation.Value("${parquet.data-path:archive}")
    private String baseArchiveDir;

    private final BlockingQueue<WeatherStatus> queue = new LinkedBlockingQueue<>();

    private final AtomicBoolean flushing = new AtomicBoolean(false);

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneId.systemDefault());

    public void enqueueForArchiving(WeatherStatus status) {
        queue.offer(status);
        System.out.println("Queue size: " + queue.size());
        if (queue.size() >= BATCH_SIZE && !flushing.get()) {
            CompletableFuture.runAsync(this::flush);
        }
    }

    @Scheduled(fixedDelayString = "${archiver.flush.interval-ms:60000}")
    public void flush() {
        System.out.println("Flushing queue" + queue.size());

        if (!flushing.compareAndSet(false, true)) {
            log.warn("Previous flush still running — skipping this tick.");
            return;
        }
        try {
            drainAndWrite();
        } finally {
            flushing.set(false);
        }
    }

    // Drains the entire queue in BATCH_SIZE chunks, writing each chunk to Parquet.
    private void drainAndWrite() {
        int totalWritten = 0;
        while (!queue.isEmpty()) {
            List<WeatherStatus> batch = new ArrayList<>(BATCH_SIZE);
            queue.drainTo(batch, BATCH_SIZE);
            if (batch.isEmpty()) break;

            log.info("Archiving batch of {} records", batch.size());

            // Partition by date and station_id
            Map<String, Map<String, List<WeatherStatus>>> partitioned = batch.stream()
                    .collect(Collectors.groupingBy(
                            this::getDatePartition,
                            Collectors.groupingBy(s -> String.valueOf(s.getStationId()))
                    ));

            try {
                writePartitionsToParquet(partitioned);
                totalWritten += batch.size();
            } catch (IOException e) {
                log.error("Failed to write parquet batch of {} records — records dropped", batch.size(), e);
            }
        }
        if (totalWritten > 0) {
            log.info("Flush complete. Total records archived: {}", totalWritten);
        }
    }

    private void writePartitionsToParquet(
            Map<String, Map<String, List<WeatherStatus>>> partitionedData) throws IOException {

        long timestamp = System.currentTimeMillis();

        for (Map.Entry<String, Map<String, List<WeatherStatus>>> dateEntry : partitionedData.entrySet()) {
            String datePart = dateEntry.getKey();

            for (Map.Entry<String, List<WeatherStatus>> stationEntry : dateEntry.getValue().entrySet()) {
                String stationPart = stationEntry.getKey();
                List<WeatherStatus> records = stationEntry.getValue();

                // Hive-style partitioned path: archive/date=2026-05-03/station_id=1/<timestamp>.parquet
                String pathString = String.format("%s/date=%s/station_id=%s/%d.parquet",
                        baseArchiveDir, datePart, stationPart, timestamp);

                Path filePath = new Path(pathString);

                try (ParquetWriter<WeatherStatus> writer = AvroParquetWriter.<WeatherStatus>builder(filePath)
                        .withSchema(ReflectData.AllowNull.get().getSchema(WeatherStatus.class))
                        .withDataModel(ReflectData.get())
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.GZIP)
                        .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                        .build()) {

                    for (WeatherStatus record : records) {
                        writer.write(record);
                    }
                }

                log.debug("Wrote {}/{} → {} records", datePart, stationPart, records.size());
            }
        }
    }

    private String getDatePartition(WeatherStatus status) {
        return dateFormatter.format(Instant.ofEpochSecond(status.getStatusTimestamp()));
    }
}