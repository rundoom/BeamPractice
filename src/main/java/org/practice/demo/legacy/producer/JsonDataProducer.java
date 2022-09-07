package org.practice.demo.legacy.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.LocalDateTime;
import org.practice.DataGenerator;
import org.practice.demo.legacy.KafkaManager;
import org.practice.demo.legacy.model.MeasurementEvent;

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
            for (int i = 0; i < 10; i++) {
                MeasurementEvent event = generator.generateNEvents(1,
                        LocalDateTime.now().minusSeconds(30).toDate().getTime()/1000,
                        LocalDateTime.now().plusSeconds(30).toDate().getTime()/1000
                ).get(0);
                String jsonStr = gson.toJson(event);
                var record = new ProducerRecord<>("event_topic", String.valueOf(event.userId), jsonStr);
                KafkaManager.getInstance().getProducer().send(record).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
