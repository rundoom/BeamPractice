package org.practice.model;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

import java.io.Serializable;
import java.text.DecimalFormat;

@DefaultSchema(JavaFieldSchema.class)
public class MeasurementEvent implements Serializable {
    private int timestamp;
    private int userId;
    private int location;
    private MeasurementType measurementType;
    @SchemaFieldName("value")
    public double value;

    private DecimalFormat df = new DecimalFormat("####0.00");


    public MeasurementEvent(int timestamp, int userId, int location, MeasurementType measurementType, double value) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.location = location;
        this.measurementType = measurementType;
        this.value = value;
    }

    public MeasurementEvent() {

    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getUserId() {
        return userId;
    }

    public long getLocation() {
        return location;
    }

    public MeasurementType getMeasurementType() {
        return measurementType;
    }

    public double getValue() {
        return value;
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
}
