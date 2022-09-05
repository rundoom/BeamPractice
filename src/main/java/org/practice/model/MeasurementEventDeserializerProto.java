package org.practice.model;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.practice.proto.MeasurementEventProto;

public class MeasurementEventDeserializerProto implements Deserializer<MeasurementEventProto.MeasurementEvent> {
    @Override
    public MeasurementEventProto.MeasurementEvent deserialize(String topic, byte[] data) {
        try {
            return MeasurementEventProto.MeasurementEvent.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
