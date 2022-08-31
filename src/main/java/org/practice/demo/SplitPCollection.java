package org.practice.demo;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.practice.DataGenerator;
import org.practice.beam.BeamManager;
import org.practice.model.MeasurementEvent;

import java.io.Serializable;

import static org.apache.beam.sdk.values.TypeDescriptors.*;
import static org.apache.beam.sdk.values.TypeDescriptors.doubles;

public class SplitPCollection implements Serializable {
    transient DataGenerator generator = new DataGenerator();
    transient BeamManager beamManager = new BeamManager();
    public static void main(String[] args) {
        new SplitPCollection().splitPCollectionOutput();
    }

    void splitPCollectionOutput() {
        var events = generator.generateNEventsToday(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events))
                .apply(WithTimestamps.of(kv -> Instant.ofEpochSecond(kv.timestamp)));

        eventPCollection
                .apply(Window.<MeasurementEvent>into(FixedWindows.of(Duration.standardDays(1))))
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(m.measurementType.name(), m.value)))
                .apply(Mean.perKey())
                .apply(ToString.elements())
                .apply(TextIO.write().to("outdata/out_by_day").withSuffix(".txt").withNumShards(1));

        eventPCollection
                .apply(Window.<MeasurementEvent>into(FixedWindows.of(Duration.standardHours(1))))
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(m.measurementType.name(), m.value)))
                .apply(Mean.perKey())
                .apply(ToString.elements())
                .apply(TextIO.write().to("outdata/out_by_hour").withSuffix(".txt").withNumShards(1));

        eventPCollection
                .apply(Window.<MeasurementEvent>into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(ToString.elements())
                .apply(TextIO.write().to("outdata/out_pure_data").withSuffix(".txt").withNumShards(1));

        beamManager.getPipeline().run().waitUntilFinish();
    }
}
