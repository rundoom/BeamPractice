package org.practice.demo.legacy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.practice.beam.BeamManager;
import org.practice.model.MeasurementEvent;
import org.practice.model.MeasurementEventDeserializer;

import java.io.Serializable;

public class ConsumeKafkaSerializePostgres implements Serializable {
    transient BeamManager beamManager = new BeamManager();

    public static void main(String[] args) {
        new ConsumeKafkaSerializePostgres().invoke();
    }

    void invoke() {
        Pipeline pipeline = beamManager.getPipeline();

        pipeline
                .apply(KafkaIO.<String, MeasurementEvent>read()
                        .withBootstrapServers("localhost:29092")
                        .withTopic("event_topic")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(MeasurementEventDeserializer.class)
                        .withoutMetadata()
                )
                .apply(Values.create())
                .apply(WithTimestamps.<MeasurementEvent>of(kv -> Instant.ofEpochSecond(kv.timestamp)).withAllowedTimestampSkew(Duration.standardDays(1)))
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

        pipeline.run();
    }
}
