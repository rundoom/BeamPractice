package org.practice.demo.legacy;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.practice.beam.BeamManager;
import org.practice.demo.legacy.model.MeasurementEvent;
import org.practice.demo.legacy.model.MeasurementEventDeserializer;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class KafkaToPostgresAndRedis implements Serializable {
    transient BeamManager beamManager = new BeamManager();

    public static void main(String[] args) {
        new KafkaToPostgresAndRedis().invoke();
    }

    void invoke() {
        PCollection<MeasurementEvent> eventPCollection = makeKafkaSource();

        writeRawToPostgres(eventPCollection);
        writeTimeToRedis(eventPCollection, KafkaToPostgresAndRedis.MeasurementTimeRange.PER_DAY);
        writeTimeToRedis(eventPCollection, KafkaToPostgresAndRedis.MeasurementTimeRange.PER_HOUR);
        writeTimeToRedis(eventPCollection, KafkaToPostgresAndRedis.MeasurementTimeRange.PER_MINUTE);

        beamManager.getPipeline().run();
    }

    private PCollection<MeasurementEvent> makeKafkaSource() {
        return beamManager.getPipeline()
                .apply(KafkaIO.<String, MeasurementEvent>read()
                        .withBootstrapServers("localhost:29092")
                        .withTopic("event_topic")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(MeasurementEventDeserializer.class)
                        .withoutMetadata()
                )
                .apply(Values.create())
                .apply(WithTimestamps.<MeasurementEvent>of(kv -> Instant.ofEpochSecond(kv.timestamp)).withAllowedTimestampSkew(Duration.standardSeconds(30)));
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

    private void writeTimeToRedis(PCollection<MeasurementEvent> eventPCollection, KafkaToPostgresAndRedis.MeasurementTimeRange timeRange) {
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
