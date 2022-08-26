package org.practice;

import org.practice.model.MeasurementEvent;
import org.practice.model.MeasurementType;

import java.util.Random;

public class DataGenerator {
    public DataGenerator() {
        this.random = new Random();
    }

    private final Random random;

    public MeasurementEvent generateEvent() {
        return new MeasurementEvent(
                random.nextLong(),
                random.nextLong(),
                random.nextLong(),
                MeasurementType.values()[random.nextInt(4)],
                random.nextDouble()
        );
    }
}
