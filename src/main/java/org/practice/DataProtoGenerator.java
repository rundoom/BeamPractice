package org.practice;

import org.practice.proto.MeasurementEventProto;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataProtoGenerator {
    public DataProtoGenerator() {
        this.random = new Random();
    }

    private final Random random;

    public MeasurementEventProto.MeasurementEvent generateEvent(long minTime, long maxTime) {
        long randomTime = minTime + (long) (random.nextDouble() * (maxTime - minTime));
        return MeasurementEventProto.MeasurementEvent.newBuilder()
                .setTimestamp(randomTime)
                .setUserId(random.nextInt(Integer.MAX_VALUE))
                .setLocation(random.nextInt(Integer.MAX_VALUE))
                .setMeasurementType(
                        MeasurementEventProto.MeasurementType.forNumber(
                                random.nextInt(MeasurementEventProto.MeasurementType.values().length - 1)
                        )
                )
                .setValue(random.nextDouble() * 10000)
                .build();
    }
}
