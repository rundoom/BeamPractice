package org.practice.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Regex;
import org.apache.beam.sdk.transforms.ToString;
import org.practice.DataGenerator;
import org.practice.model.MeasurementEvent;

public class BeamManager {
    PipelineOptions options;
    Pipeline pipeline;

    public BeamManager(PipelineOptions options) {
        this.options = options;
        this.pipeline = Pipeline.create(options);
    }

    public BeamManager() {
        this(PipelineOptionsFactory.create());
    }

    public void initBeam() {
        DataGenerator generator = new DataGenerator();
        pipeline.apply(Create.of(generator.generateNEvents(50)))
                .apply(ToString.elements())
//                .setCoder(SerializableCoder.of(MeasurementEvent.class))
                .apply(TextIO.write().to("out").withSuffix(".txt"));
        pipeline.run().waitUntilFinish();
    }
}
