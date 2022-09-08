package org.practice.demo.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.LocalDateTime;
import org.practice.DataProtoGenerator;
import org.practice.KafkaManagerBytes;
import org.practice.proto.MeasurementEventProto;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

public class ProtobufDataProducerInfinite implements Serializable {
    transient DataProtoGenerator generator = new DataProtoGenerator();

    public void generateProtoToKafka() {
        try {
            while (true) {
                MeasurementEventProto.MeasurementEvent event = generator.generateEvent(
                        LocalDateTime.now().minusSeconds(30).toDate().getTime() / 1000,
                        LocalDateTime.now().plusSeconds(30).toDate().getTime() / 1000
                );

                byte[] protoBytes = event.toByteArray();
                var record = new ProducerRecord<String, byte[]>("event_topic_proto", String.valueOf(event.getUserId()), protoBytes);
                KafkaManagerBytes.getInstance().getProducer().send(record).get();
                Thread.sleep(300);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
