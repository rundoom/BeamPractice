package org.practice.demo.legacy;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.practice.beam.BeamManager;
import org.practice.demo.legacy.model.MeasurementEvent;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class RedisAndPostgres implements Serializable {
    transient DataGenerator generator = new DataGenerator();
    transient BeamManager beamManager = new BeamManager();

    public static void main(String[] args) {
        new RedisAndPostgres().writeToRedis();
    }

    void writeToRedis() {
        var events = generator.generateNEventsToday(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events))
                .apply(WithTimestamps.of(kv -> Instant.ofEpochSecond(kv.timestamp)));

        writeRawToPostgres(eventPCollection);
        writeTimeToRedis(eventPCollection, MeasurementTimeRange.PER_DAY);
        writeTimeToRedis(eventPCollection, MeasurementTimeRange.PER_HOUR);
        writeTimeToRedis(eventPCollection, MeasurementTimeRange.PER_MINUTE);

        beamManager.getPipeline().run().waitUntilFinish();
    }

    private void writeRawToPostgres(PCollection<MeasurementEvent> eventPCollection) {
        eventPCollection
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(JdbcIO.<MeasurementEvent>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "org.postgresql.Driver",
                                        "jdbc:postgresql://localhost:5432/postgres")
                                .withUsername("postgres")
                                .withPassword("example"))
                        .withStatement("insert into measurement_event values(?, ?, ?, ?, ?)")
                        .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<MeasurementEvent>) (element, query) -> {
                            query.setInt(1, element.userId);
                            query.setDouble(2, element.value);
                            query.setString(3, element.measurementType.name());
                            query.setInt(4, element.location);
                            query.setLong(5, element.timestamp);
                        })
                );
    }

    private void writeTimeToRedis(PCollection<MeasurementEvent> eventPCollection, MeasurementTimeRange timeRange) {
        eventPCollection
                .apply(Window.into(FixedWindows.of(timeRange.duration)))
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(
                        m.measurementType.name() + ':' + timeRange.keyPart + ':' + m.location + ':' + timeRange.formatTimestamp(m.timestamp * 1000),
                        m.value)
                ))
                .apply(Mean.perKey())
                .apply(MapElements.into(kvs(strings(), strings())).via(m -> KV.of(m.getKey(), String.valueOf(m.getValue()))))
                .apply(RedisIO.write().withEndpoint("localhost", 6379));
    }

    private enum MeasurementTimeRange {
        PER_MINUTE("per_minute", Duration.standardMinutes(1), new SimpleDateFormat("dd.MM.yyyy_HH:mm:00")),
        PER_HOUR("per_hour", Duration.standardHours(1), new SimpleDateFormat("dd.MM.yyyy_HH:00:00")),
        PER_DAY("per_day", Duration.standardDays(1), new SimpleDateFormat("dd.MM.yyyy_00:00:00"));

        MeasurementTimeRange(String keyPart, Duration duration, DateFormat dateFormat) {
            this.keyPart = keyPart;
            this.duration = duration;
            this.dateFormat = dateFormat;
        }

        String formatTimestamp(long timestamp) {
            return dateFormat.format(timestamp);
        }

        private final String keyPart;
        private final Duration duration;
        private final DateFormat dateFormat;
    }
}
