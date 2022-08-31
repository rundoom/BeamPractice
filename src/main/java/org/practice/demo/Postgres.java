package org.practice.demo;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.practice.DataGenerator;
import org.practice.beam.BeamManager;
import org.practice.model.MeasurementEvent;

import java.io.Serializable;

public class Postgres implements Serializable {
    transient DataGenerator generator = new DataGenerator();
    transient BeamManager beamManager = new BeamManager();
    public static void main(String[] args) {
        new Postgres().writeToPostgres();
    }

    void writeToPostgres() {
        var events = generator.generateNEventsToday(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection
                .apply(Window.<MeasurementEvent>into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(JdbcIO.<MeasurementEvent>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres")
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

        beamManager.getPipeline().run().waitUntilFinish();
    }
}
