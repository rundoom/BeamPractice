import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.jupiter.api.Test;
import org.practice.DataGenerator;
import org.practice.beam.BeamManager;
import org.practice.model.MeasurementEvent;

public class BeamTests {
    DataGenerator generator = new DataGenerator();
    BeamManager beamManager = new BeamManager();

    @Test
    void parallelPCollections() {
        var events = generator.generateNEvents(1000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection.apply(ToString.elements())
                .apply(MapElements.into(TypeDescriptors.strings()).via((String word) -> word + "uuuf"))
                .apply(TextIO.write().to("out").withSuffix(".txt"));

        eventPCollection.apply(ToString.elements())
                .apply(TextIO.write().to("yooo").withSuffix(".txt"));

        beamManager.getPipeline().run().waitUntilFinish();
    }

    @Test
    void testAverage() {
        var events = generator.generateNEvents(100000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection
                .apply(Group.<MeasurementEvent>globally().aggregateField("value", Sum.ofDoubles(), "avgValue"))
                .apply(ToString.elements())
                .apply(TextIO.write().to("yooo").withSuffix(".txt"));

        beamManager.getPipeline().run().waitUntilFinish();
    }

    @Test
    void testBeam2() {
        var events = generator.generateNEvents(100000);
        PCollection<MeasurementEvent> eventPCollection = beamManager.getPipeline().apply(Create.of(events));

        eventPCollection
                .apply(Window.<MeasurementEvent>into(FixedWindows.of(Duration.millis(1))))
                .apply(Group.<MeasurementEvent>globally().aggregateField("value", Sum.ofDoubles(), "avgValue"))
                .apply(ToString.elements())
                .apply(TextIO.write().to("yooo").withSuffix(".txt"));

        beamManager.getPipeline().run().waitUntilFinish();
    }
}
