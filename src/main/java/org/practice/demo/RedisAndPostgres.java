package org.practice.demo;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.practice.DataGenerator;
import org.practice.beam.BeamManager;
import org.practice.model.MeasurementEvent;

import java.io.Serializable;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

public class RedisAndPostgres implements Serializable {
    transient DataGenerator generator = new DataGenerator();
    transient BeamManager beamManager = new BeamManager();

    public static void main(String[] args) {
        new RedisAndPostgres().writeToRedis();
    }

    void writeToRedis() {
        var events = generator.generateNEventsToday(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        writeRawToPostgres(eventPCollection);
        writeDaysToRedis(eventPCollection);
        writeHoursToRedis(eventPCollection);

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

    private void writeDaysToRedis(PCollection<MeasurementEvent> eventPCollection) {
        eventPCollection
                .apply(Window.into(FixedWindows.of(Duration.standardDays(1))))
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(
                        m.measurementType.name() + '_' + "per_day" + '_' + m.userId + '_' + m.location,
                        m.value)
                ))
                .apply(Mean.perKey())
                .apply(MapElements.into(kvs(strings(), strings())).via(m -> KV.of(m.getKey(), String.valueOf(m.getValue()))))
                .apply(RedisIO.write().withEndpoint("localhost", 6379));
    }

    private void writeHoursToRedis(PCollection<MeasurementEvent> eventPCollection) {
        eventPCollection
                .apply(Window.into(FixedWindows.of(Duration.standardHours(1))))
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(
                        m.measurementType.name() + '_' + "per_hour" + '_' + m.userId + '_' + m.location,
                        m.value)
                ))
                .apply(Mean.perKey())
                .apply(MapElements.into(kvs(strings(), strings())).via(m -> KV.of(m.getKey(), String.valueOf(m.getValue()))))
                .apply(RedisIO.write().withEndpoint("localhost", 6379));
    }
}
