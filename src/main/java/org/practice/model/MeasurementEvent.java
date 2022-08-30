package org.practice.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

import java.text.DecimalFormat;
import java.util.Objects;

@DefaultSchema(JavaFieldSchema.class)
@DefaultCoder(AvroCoder.class)
public class MeasurementEvent {
    @SchemaFieldName("timestamp")
    public long timestamp;

    @SchemaFieldName("userId")
    public int userId;

    @SchemaFieldName("location")
    public int location;

    @SchemaFieldName("measurementType")
    public MeasurementType measurementType;

    @SchemaFieldName("value")
    public double value;

    private DecimalFormat df = new DecimalFormat("####0.00");


    public MeasurementEvent(long timestamp, int userId, int location, MeasurementType measurementType, double value) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.location = location;
        this.measurementType = measurementType;
        this.value = value;
    }

    public MeasurementEvent() {

    }

    @Override
    public String toString() {
        return "MeasurementEvent{" +
                "timestamp=" + timestamp +
                ", userId=" + userId +
                ", location=" + location +
                ", measurementType=" + measurementType +
                ", value=" + df.format(value) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MeasurementEvent that = (MeasurementEvent) o;
        return timestamp == that.timestamp && userId == that.userId && location == that.location && Double.compare(that.value, value) == 0 && measurementType == that.measurementType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, userId, location, measurementType, value);
    }
}
