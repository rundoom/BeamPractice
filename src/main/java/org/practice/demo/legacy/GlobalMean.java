package org.practice.demo.legacy;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.practice.DataGenerator;
import org.practice.beam.BeamManager;
import org.practice.demo.legacy.model.MeasurementEvent;

import java.io.Serializable;

public class GlobalMean implements Serializable {
    transient DataGenerator generator = new DataGenerator();
    transient BeamManager beamManager = new BeamManager();
    public static void main(String[] args) {
        new GlobalMean().meanGlobal();
    }

    void meanGlobal() {
        var events = generator.generateNEvents(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection
                .apply(MapElements.into(TypeDescriptor.of(Double.class)).via(event -> event.value))
                .apply(Mean.globally())
                .apply(ToString.elements())
                .apply(TextIO.write().to("outdata/out").withSuffix(".txt"));

        beamManager.getPipeline().run().waitUntilFinish();
    }
}
