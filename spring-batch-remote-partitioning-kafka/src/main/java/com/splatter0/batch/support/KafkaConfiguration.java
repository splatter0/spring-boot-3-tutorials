package com.splatter0.batch.support;

import static com.splatter0.batch.support.Constants.MANAGER_REQUEST_TOPIC_NAME;
import static com.splatter0.batch.support.Constants.TOPIC_PARTITION_COUNT;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfiguration {

    @Bean
    public DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    public DirectChannel replies() {
        return new DirectChannel();
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(MANAGER_REQUEST_TOPIC_NAME)
                .partitions(TOPIC_PARTITION_COUNT)
                .build();
    }
}
