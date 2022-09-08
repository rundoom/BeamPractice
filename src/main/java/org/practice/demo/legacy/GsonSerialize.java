package org.practice.demo.legacy;

import com.google.gson.Gson;
import org.practice.demo.legacy.model.MeasurementEvent;

import java.io.Serializable;

public class GsonSerialize implements Serializable {
    transient DataGenerator generator = new DataGenerator();
    transient Gson gson = new Gson();
    public static void main(String[] args) {
        new GsonSerialize().convertJson();
    }

    void convertJson() {
        MeasurementEvent event = generator.generateNEventsToday(1).get(0);
        String jsonStr = gson.toJson(event);
        MeasurementEvent eventFromJson = gson.fromJson(jsonStr, MeasurementEvent.class);
        System.out.println(jsonStr);
        System.out.println(eventFromJson);
    }
}
