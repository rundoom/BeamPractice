package org.practice;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaManagerBytes {
    private final static KafkaManagerBytes instance = new KafkaManagerBytes();
    private final KafkaProducer<String, byte[]> producer;

    private KafkaManagerBytes() {
        var props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public static KafkaManagerBytes getInstance() {
        return instance;

    }

    public KafkaProducer<String, byte[]> getProducer() {
        return producer;
    }
}
