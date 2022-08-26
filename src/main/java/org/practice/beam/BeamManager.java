package org.practice.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

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

    public Pipeline getPipeline() {
        return pipeline;
    }
}
