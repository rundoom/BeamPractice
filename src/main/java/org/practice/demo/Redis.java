package org.practice.demo;

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

public class Redis implements Serializable {
    transient DataGenerator generator = new DataGenerator();
    transient BeamManager beamManager = new BeamManager();

    public static void main(String[] args) {
        new Redis().writeToRedis();
    }

    void writeToRedis() {
        var events = generator.generateNEventsToday(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection
                .apply(Window.into(FixedWindows.of(Duration.standardDays(1))))
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(
                        m.measurementType.name() + '_' + "per_day" + '_' + m.userId + '_' + m.location,
                        m.value)
                ))
                .apply(Mean.perKey())
                .apply(MapElements.into(kvs(strings(), strings())).via(m -> KV.of(m.getKey(), String.valueOf(m.getValue()))))
                .apply(RedisIO.write().withEndpoint("localhost", 6379));

        beamManager.getPipeline().run().waitUntilFinish();
    }
}
