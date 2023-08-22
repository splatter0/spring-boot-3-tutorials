package com.splatter0.batch.support;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.core.serializer.DefaultSerializer;

public class KafkaStringOrByteSerializer implements Serializer<Object> {

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            return new DefaultSerializer().serializeToByteArray(data);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing MessageDto to byte[]");
        }
    }
}
