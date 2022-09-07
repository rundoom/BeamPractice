package org.practice.demo;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.practice.beam.BeamManager;
import org.practice.model.MeasurementEventDeserializerProto;
import org.practice.proto.MeasurementEventProto;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class KafkaToPostgresAndRedisLatenessTriggeringFromProto implements Serializable {
    transient BeamManager beamManager = new BeamManager();

    public static void main(String[] args) {
        new KafkaToPostgresAndRedisLatenessTriggeringFromProto().invoke();
    }

    void invoke() {
        PCollection<MeasurementEventProto.MeasurementEvent> eventPCollection = makeKafkaSource();

        writeRawToPostgres(eventPCollection);
        writeTimeToRedis(eventPCollection, KafkaToPostgresAndRedisLatenessTriggeringFromProto.MeasurementTimeRange.PER_DAY);
        writeTimeToRedis(eventPCollection, KafkaToPostgresAndRedisLatenessTriggeringFromProto.MeasurementTimeRange.PER_HOUR);
        writeTimeToRedis(eventPCollection, KafkaToPostgresAndRedisLatenessTriggeringFromProto.MeasurementTimeRange.PER_MINUTE);

        beamManager.getPipeline().run();
    }

    private PCollection<MeasurementEventProto.MeasurementEvent> makeKafkaSource() {
        return beamManager.getPipeline()
                .apply(KafkaIO.<String, MeasurementEventProto.MeasurementEvent>read()
                        .withBootstrapServers("localhost:29092")
                        .withTopic("event_topic_proto")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(MeasurementEventDeserializerProto.class)
                        .withoutMetadata()
                )
                .apply(Values.create())
                .apply(WithTimestamps.<MeasurementEventProto.MeasurementEvent>of(kv -> Instant.ofEpochSecond(kv.getTimestamp())).withAllowedTimestampSkew(Duration.standardSeconds(35)));
    }

    private void writeRawToPostgres(PCollection<MeasurementEventProto.MeasurementEvent> eventPCollection) {
        eventPCollection
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(JdbcIO.<MeasurementEventProto.MeasurementEvent>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "org.postgresql.Driver",
                                        "jdbc:postgresql://localhost:5432/postgres")
                                .withUsername("postgres")
                                .withPassword("example"))
                        .withStatement("insert into measurement_event values(?, ?, ?, ?, ?)")
                        .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<MeasurementEventProto.MeasurementEvent>) (element, query) -> {
                            query.setInt(1, element.getUserId());
                            query.setDouble(2, element.getValue());
                            query.setString(3, element.getMeasurementType().name());
                            query.setInt(4, element.getLocation());
                            query.setLong(5, element.getTimestamp());
                        })
                );
    }

    private void writeTimeToRedis(PCollection<MeasurementEventProto.MeasurementEvent> eventPCollection, KafkaToPostgresAndRedisLatenessTriggeringFromProto.MeasurementTimeRange timeRange) {
        eventPCollection
                .apply(
                        Window.<MeasurementEventProto.MeasurementEvent>into(FixedWindows.of(timeRange.duration))
                                .withAllowedLateness(Duration.standardSeconds(30))
                                .accumulatingFiredPanes()
                                .triggering(Repeatedly.forever(
                                        AfterWatermark.pastEndOfWindow()
                                                .withEarlyFirings(AfterPane.elementCountAtLeast(10)))
                                )
                )
                //Make PCollection<EventGroupingKey, Event>
                //Group per key нужно написать кастомный комбайнер Combine.perKey
                //Add end of window timestamp
                //EventGroupingKey.toString() + end of window timestamp
                //Это уже пишем в Редис
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(
                        m.getMeasurementType().name() + ':' + timeRange.keyPart + ':' + m.getLocation() + ':' + timeRange.formatTimestamp(m.getTimestamp() * 1000),
                        m.getValue())
                ))
                .apply(Mean.perKey())
                .apply(MapElements.into(kvs(strings(), strings())).via(m -> KV.of(m.getKey(), String.valueOf(m.getValue()))))
                .apply(RedisIO.write().withEndpoint("localhost", 6379));
    }

    private enum MeasurementTimeRange {
        PER_MINUTE("minute", Duration.standardMinutes(1), new SimpleDateFormat("dd.MM.yyyy_HH:mm:00")),
        PER_HOUR("hour", Duration.standardHours(1), new SimpleDateFormat("dd.MM.yyyy_HH:00:00")),
        PER_DAY("day", Duration.standardDays(1), new SimpleDateFormat("dd.MM.yyyy_00:00:00"));

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
