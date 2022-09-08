package org.practice.demo.legacy.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.LocalDateTime;
import org.practice.demo.legacy.DataGenerator;
import org.practice.KafkaManagerBytes;
import org.practice.demo.legacy.model.MeasurementEvent;
import org.practice.proto.MeasurementEventProto;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

public class ProtobufDataProducer implements Serializable {
    transient DataGenerator generator = new DataGenerator();

    public static void main(String[] args) {
        new ProtobufDataProducer().convertProto();
    }

    void convertProto() {
        try {
            for (int i = 0; i < 10; i++) {
                MeasurementEvent event = generator.generateNEvents(1,
                        LocalDateTime.now().minusSeconds(30).toDate().getTime()/1000,
                        LocalDateTime.now().plusSeconds(30).toDate().getTime()/1000
                ).get(0);

                MeasurementEventProto.MeasurementEvent protoEvent = MeasurementEventProto.MeasurementEvent.newBuilder()
                        .setTimestamp(event.timestamp)
                        .setUserId(event.userId)
                        .setLocation(event.location)
                        .setMeasurementType(MeasurementEventProto.MeasurementType.valueOf(event.measurementType.name()))
                        .setValue(event.value).build();

                byte[] protoBytes = protoEvent.toByteArray();
                var record = new ProducerRecord<String, byte[]>("event_topic_proto", String.valueOf(event.userId), protoBytes);
                KafkaManagerBytes.getInstance().getProducer().send(record).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
