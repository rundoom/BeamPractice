package org.practice.demo;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.practice.DataGenerator;
import org.practice.KafkaManager;
import org.practice.model.MeasurementEvent;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

public class JsonDataProducer implements Serializable {
    transient DataGenerator generator = new DataGenerator();
    transient Gson gson = new Gson();

    public static void main(String[] args) {
        new JsonDataProducer().convertJson();
    }

    void convertJson() {
        try {
            for (int i = 0; i < 100; i++) {
                MeasurementEvent event = generator.generateNEventsToday(1).get(0);
                String jsonStr = gson.toJson(event);
                var record = new ProducerRecord<>("event_topic", String.valueOf(event.userId), jsonStr);
                KafkaManager.getInstance().getProducer().send(record).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }


    }
}
