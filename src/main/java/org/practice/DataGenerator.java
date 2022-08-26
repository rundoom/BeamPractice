package org.practice;

import org.practice.model.MeasurementEvent;
import org.practice.model.MeasurementType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {
    public DataGenerator() {
        this.random = new Random();
    }

    private final Random random;

    public MeasurementEvent generateEvent() {
        return new MeasurementEvent(
                random.nextInt(Integer.SIZE -1),
                random.nextInt(Integer.SIZE -1),
                random.nextInt(Integer.SIZE -1),
                MeasurementType.values()[random.nextInt(MeasurementType.values().length)],
                random.nextDouble()
        );
    }

    public List<MeasurementEvent> generateNEvents(int eventCount) {
        List<MeasurementEvent> list = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) list.add(generateEvent());
        return list;
    }
}
