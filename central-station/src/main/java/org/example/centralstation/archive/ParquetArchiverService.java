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
    private static final int BATCH_SIZE = 10000;

    @org.springframework.beans.factory.annotation.Value("${parquet.data-path:archive}")
    private String baseArchiveDir;

    private final BlockingQueue<WeatherStatus> queue = new LinkedBlockingQueue<>();

    private final AtomicBoolean flushing = new AtomicBoolean(false);

    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneId.systemDefault());

    public void enqueueForArchiving(WeatherStatus status) {
        queue.offer(status);
        if (queue.size() >= BATCH_SIZE && !flushing.get()) {
            CompletableFuture.runAsync(() -> flush(false));
        }
    }

    @Scheduled(fixedDelayString = "${archiver.flush.interval-ms:60000}")
    public void scheduledFlush() {
        log.info("Starting scheduled flush of all records...");
        flush(true);
    }

    public void flush(boolean forceAll) {
        if (!flushing.compareAndSet(false, true)) {
            return;
        }
        try {
            drainAndWrite(forceAll);
        } finally {
            flushing.set(false);
        }
    }

    private void drainAndWrite(boolean forceAll) {
        List<WeatherStatus> toWrite = new ArrayList<>();

        if (forceAll) {
            queue.drainTo(toWrite);
        } else {
            while (queue.size() >= BATCH_SIZE) {
                List<WeatherStatus> batch = new ArrayList<>(BATCH_SIZE);
                queue.drainTo(batch, BATCH_SIZE);
                toWrite.addAll(batch);
            }
        }

        if (toWrite.isEmpty()) return;

        int totalWritten = 0;
        for (int i = 0; i < toWrite.size(); i += BATCH_SIZE) {
            List<WeatherStatus> batch = toWrite.subList(i, Math.min(i + BATCH_SIZE, toWrite.size()));
            
            log.info("Archiving batch of {} records", batch.size());

            Map<String, Map<String, List<WeatherStatus>>> partitioned = batch.stream()
                    .collect(Collectors.groupingBy(
                            this::getDatePartition,
                            Collectors.groupingBy(s -> String.valueOf(s.getStationId()))));

            try {
                writePartitionsToParquet(partitioned);
                totalWritten += batch.size();
            } catch (IOException e) {
                log.error("Failed to write parquet batch of {} records", batch.size(), e);
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

                String pathString = String.format("%s/date=%s/station_id=%s/%d.parquet",
                        baseArchiveDir, datePart, stationPart, timestamp);

                Path filePath = new Path(pathString);

                Configuration conf = new Configuration();
                conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

                try (ParquetWriter<WeatherStatus> writer = AvroParquetWriter.<WeatherStatus>builder(filePath)
                        .withSchema(ReflectData.AllowNull.get().getSchema(WeatherStatus.class))
                        .withDataModel(ReflectData.get())
                        .withConf(conf)
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