package org.practice.model;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.io.Serializable;

public class MeasurementEvent implements Serializable {
    int timestamp;
    int userId;
    int location;
    MeasurementType measurementType;
    double value;

    public MeasurementEvent(int timestamp, int userId, int location, MeasurementType measurementType, double value) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.location = location;
        this.measurementType = measurementType;
        this.value = value;
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
                ", value=" + value +
                '}';
    }
}
