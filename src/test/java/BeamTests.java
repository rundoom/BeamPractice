import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;
import org.practice.DataGenerator;
import org.practice.beam.BeamManager;
import org.practice.model.MeasurementEvent;

import static org.apache.beam.sdk.values.TypeDescriptors.*;


public class BeamTests {
    DataGenerator generator = new DataGenerator();
    BeamManager beamManager = new BeamManager();

    @Test
    void parallelPCollections() {
        var events = generator.generateNEvents(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection.apply(ToString.elements())
                .apply(MapElements.into(strings()).via((String word) -> word + "uuuf"))
                .apply(TextIO.write().to("outdata/out").withSuffix(".txt"));

        eventPCollection.apply(ToString.elements())
                .apply(TextIO.write().to("outdata/yooo").withSuffix(".txt"));

        beamManager.getPipeline().run().waitUntilFinish();
    }

    @Test
    void testSum() {
        var events = generator.generateNEvents(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection
                .apply(Group.<MeasurementEvent>globally().aggregateField("value", Sum.ofDoubles(), "sumValue"))
                .apply(ToString.elements())
                .apply(TextIO.write().to("outdata/out").withSuffix(".txt"));

        beamManager.getPipeline().run().waitUntilFinish();
    }

    @Test
    void testMean() {
        var events = generator.generateNEvents(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection
                .apply(MapElements.into(TypeDescriptor.of(Double.class)).via(event -> event.value))
                .apply(Mean.globally())
                .apply(ToString.elements())
                .apply(TextIO.write().to("outdata/out").withSuffix(".txt"));

        beamManager.getPipeline().run().waitUntilFinish();
    }

    @Test
    void testMeanWindowed() {
        var events = generator.generateNEventsToday(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection
                .apply(WithTimestamps.of(kv -> Instant.ofEpochSecond(kv.timestamp)))
                .apply(Window.<MeasurementEvent>into(FixedWindows.of(Duration.standardHours(1))))
                .apply(MapElements.into(kvs(strings(), doubles())).via(m -> KV.of(m.measurementType.name(), m.value)))
                .apply(Mean.perKey())
                .apply(ToString.elements())
                .apply(TextIO.write().to("outdata/out").withSuffix(".txt").withNumShards(1));

        beamManager.getPipeline().run().waitUntilFinish();
    }

    @Test
    void testSplitPCollection() {
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
