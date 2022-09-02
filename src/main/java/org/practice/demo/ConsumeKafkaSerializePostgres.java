package org.practice.demo;

import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.practice.beam.BeamManager;
import org.practice.model.MeasurementEvent;

import java.io.Serializable;

public class ConsumeKafkaSerializePostgres implements Serializable {
    transient BeamManager beamManager = new BeamManager();

    public static void main(String[] args) {
        new ConsumeKafkaSerializePostgres().meanGlobal();
    }

    void meanGlobal() {
        Pipeline pipeline = beamManager.getPipeline();

        pipeline
                .apply(KafkaIO.<String, String>read()
                        .withBootstrapServers("localhost:29092")
                        .withTopic("event_topic")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
                        .withoutMetadata()
                )
                .apply(Values.create())
                .apply(MapElements.into(TypeDescriptor.of(MeasurementEvent.class)).via(
                        json -> new Gson().fromJson(json, MeasurementEvent.class)
                ))
//                .apply(WithTimestamps.of(kv -> Instant.ofEpochSecond(kv.timestamp)))
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
