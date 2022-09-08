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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.practice.beam.BeamManager;
import org.practice.demo.producer.ProtobufDataProducerInfinite;
import org.practice.model.MeasurementEventDeserializerProto;
import org.practice.model.PracticeOptions;
import org.practice.proto.MeasurementEventProto;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.Executors;

import static org.apache.beam.sdk.values.TypeDescriptors.*;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class KafkaToPostgresAndRedisLatenessTriggeringFromProto implements Serializable {
    transient BeamManager beamManager;

    public static void main(String[] args) {
        KafkaToPostgresAndRedisLatenessTriggeringFromProto beam = new KafkaToPostgresAndRedisLatenessTriggeringFromProto();
        beam.beamManager = new BeamManager(PipelineOptionsFactory.fromArgs(args).as(PracticeOptions.class));

        Executors.newSingleThreadExecutor().submit(
                () -> new ProtobufDataProducerInfinite().generateProtoToKafka()
        );

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
                        .withBootstrapServers(options.getKafkaBootstrapServer())
                        .withTopic(options.getKafkaTopic())
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(MeasurementEventDeserializerProto.class)
                        .withoutMetadata()
                )
                .apply(Values.create())
                .apply(WithTimestamps.<MeasurementEventProto.MeasurementEvent>of(kv -> Instant.ofEpochSecond(kv.getTimestamp())).withAllowedTimestampSkew(Duration.standardSeconds(240)));
    }

    private void writeRawToPostgres(PCollection<MeasurementEventProto.MeasurementEvent> eventPCollection) {
        PracticeOptions options = beamManager.getPipeline().getOptions().as(PracticeOptions.class);
        eventPCollection
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(JdbcIO.<MeasurementEventProto.MeasurementEvent>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "org.postgresql.Driver",
                                        options.getPostgresUrl())
                                .withUsername(options.getPostgresUsername())
                                .withPassword(options.getPostgresPassword()))
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
                .apply(RedisIO.write().withEndpoint(options.getRedisHost(), options.getRedisPort()));
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

    private static ListMultimap<String, String> parseCommandLine(
            String[] args) {
        ImmutableListMultimap.Builder<String, String> builder = ImmutableListMultimap.builder();
        for (String arg : args) {
            if (Strings.isNullOrEmpty(arg)) {
                continue;
            }
            checkArgument(arg.startsWith("--"), "Argument '%s' does not begin with '--'", arg);
            int index = arg.indexOf('=');
            // Make sure that '=' isn't the first character after '--' or the last character
            checkArgument(
                    index != 2, "Argument '%s' starts with '--=', empty argument name not allowed", arg);
            if (index > 0) {
                builder.put(arg.substring(2, index), arg.substring(index + 1));
            } else {
                builder.put(arg.substring(2), "true");
            }
        }
        return builder.build();
    }
}
