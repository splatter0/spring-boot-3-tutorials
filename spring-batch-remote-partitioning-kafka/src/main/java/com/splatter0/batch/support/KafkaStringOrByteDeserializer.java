package com.splatter0.batch.support;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.core.serializer.DefaultDeserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class KafkaStringOrByteDeserializer implements Deserializer<Object> {
    @Override
    public Object deserialize(String s, byte[] bytes) {
        try {
            return new DefaultDeserializer().deserialize(new ByteArrayInputStream(bytes));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
