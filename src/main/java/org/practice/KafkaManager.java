package org.practice;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaManager {
    private final static KafkaManager instance = new KafkaManager();
    private final KafkaProducer<String, String> producer;

    private KafkaManager() {
        var props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public static KafkaManager getInstance() {
        return instance;

    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }
}
