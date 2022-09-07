package org.practice.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.practice.model.PracticeOptions;
import org.practice.proto.MeasurementEventProto;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class KafkaToPostgresAndRedisLatenessTriggeringFromProto implements Serializable {
    transient BeamManager beamManager;

    public static void main(String[] args) {
        KafkaToPostgresAndRedisLatenessTriggeringFromProto beam = new KafkaToPostgresAndRedisLatenessTriggeringFromProto();
        beam.beamManager = new BeamManager(PipelineOptionsFactory.fromArgs(args).as(PracticeOptions.class));
        beam.invoke();
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
        Pipeline pipeline = beamManager.getPipeline();
        PracticeOptions options = pipeline.getOptions().as(PracticeOptions.class);
        return pipeline
                .apply(KafkaIO.<String, MeasurementEventProto.MeasurementEvent>read()
                        .withBootstrapServers(options.getKafkaBootstrapServer().get())
                        .withTopic(options.getKafkaTopic().get())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(MeasurementEventDeserializerProto.class)
                        .withoutMetadata()
                )
                .apply(Values.create())
                .apply(WithTimestamps.<MeasurementEventProto.MeasurementEvent>of(kv -> Instant.ofEpochSecond(kv.getTimestamp())).withAllowedTimestampSkew(Duration.standardSeconds(35)));
    }

    private void writeRawToPostgres(PCollection<MeasurementEventProto.MeasurementEvent> eventPCollection) {
        PracticeOptions options = beamManager.getPipeline().getOptions().as(PracticeOptions.class);
        eventPCollection
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(JdbcIO.<MeasurementEventProto.MeasurementEvent>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "org.postgresql.Driver",
                                        options.getPostgresUrl().get())
                                .withUsername(options.getPostgresUsername().get())
                                .withPassword(options.getPostgresPassword().get()))
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
        PracticeOptions options = beamManager.getPipeline().getOptions().as(PracticeOptions.class);
        eventPCollection
                .apply(
                        Window.<MeasurementEventProto.MeasurementEvent>into(FixedWindows.of(timeRange.duration))
                                .withAllowedLateness(Duration.standardSeconds(30))
                                .accumulatingFiredPanes()
                                .triggering(
                                        AfterWatermark.pastEndOfWindow()
                                                .withEarlyFirings(AfterPane.elementCountAtLeast(10))
                                                .withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30)))
                                )
                )
                //Make PCollection<EventGroupingKey, Event>
                //Group per key нужно написать кастомный комбайнер Combine.perKey
                //Add end of window timestamp
                //EventGroupingKey.toString() + end of window timestamp
                //Это уже пишем в Редис
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(
                        m.getMeasurementType().name() + '#' + timeRange.keyPart + '#' + m.getLocation(),
                        m.getValue())
                ))
                .apply(Mean.perKey())
                .apply(MapElements.into(kvs(strings(), strings())).via(m -> KV.of(m.getKey(), String.valueOf(m.getValue()))))
                .apply(RedisIO.write().withEndpoint(options.getRedisHost().get(), options.getRedisPort().get()));
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
