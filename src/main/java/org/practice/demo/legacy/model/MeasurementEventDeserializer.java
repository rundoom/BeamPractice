package org.practice.demo.legacy.model;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public class MeasurementEventDeserializer implements Deserializer<MeasurementEvent> {
    private final Gson gson = new Gson();
    @Override
    public MeasurementEvent deserialize(String topic, byte[] data) {
        return gson.fromJson(new String(data, StandardCharsets.UTF_8), MeasurementEvent.class);
    }
}
