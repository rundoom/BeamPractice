package org.practice.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.practice.beam.BeamManager;

import java.io.Serializable;

public class ConsumeKafka implements Serializable {
    transient BeamManager beamManager = new BeamManager();

    public static void main(String[] args) {
        new ConsumeKafka().meanGlobal();
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
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(TextIO.write().to("outdata/out").withWindowedWrites().withNumShards(1).withSuffix(".txt"));

        pipeline.run();
    }
}
