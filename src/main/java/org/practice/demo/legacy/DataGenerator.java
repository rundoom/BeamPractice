package org.practice.demo.legacy;

import org.practice.demo.legacy.model.MeasurementEvent;
import org.practice.demo.legacy.model.MeasurementType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {
    public DataGenerator() {
        this.random = new Random();
    }

    private final Random random;

    public MeasurementEvent generateEvent() {
        return generateEvent(0, Long.MAX_VALUE);
    }

    public MeasurementEvent generateEvent(long minTime, long maxTime) {
        long randomTime = minTime + (long) (random.nextDouble() * (maxTime - minTime));
        return new MeasurementEvent(
                randomTime,
                random.nextInt(Integer.MAX_VALUE),
                random.nextInt(Integer.MAX_VALUE),
                MeasurementType.values()[random.nextInt(MeasurementType.values().length)],
                random.nextDouble() * 10000
        );
    }

    public List<MeasurementEvent> generateNEvents(int eventCount) {
        return generateNEvents(eventCount, 0, Long.MAX_VALUE);
    }

    public List<MeasurementEvent> generateNEventsToday(int eventCount) {
        DateRange todayTimeRange = getTodayTimeRange();
        return generateNEvents(eventCount, todayTimeRange.getStartDate(), todayTimeRange.getEndDate());
    }

    public List<MeasurementEvent> generateNEvents(int eventCount, long minTime, long maxTime) {
        List<MeasurementEvent> list = new ArrayList<>(eventCount);
        for (int i = 0; i < eventCount; i++) list.add(generateEvent(minTime, maxTime));
        return list;
    }

    private DateRange getTodayTimeRange() {
        return new DateRange(
                LocalDate.now().toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC),
                LocalDate.now().plusDays(1).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC)
        );
    }

    private static class DateRange {
        private final long startDate;
        private final long endDate;

        public DateRange(long startDate, long endDate) {
            this.startDate = startDate;
            this.endDate = endDate;
        }

        public long getStartDate() {
            return startDate;
        }

        public long getEndDate() {
            return endDate;
        }
    }
}
