package com.splatter0.batch.support;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqConfiguration {
    @Bean
    public Queue requestsQueue() {
        return new Queue("requests");
    }

    @Bean
    public Queue repliesQueue() {
        return new Queue("replies");
    }
}
