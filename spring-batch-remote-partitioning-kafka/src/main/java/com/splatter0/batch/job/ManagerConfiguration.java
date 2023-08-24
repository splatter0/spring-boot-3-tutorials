package com.splatter0.batch.job;

import static com.splatter0.batch.support.Constants.*;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings({"rawtypes", "unchecked", "DataFlowIssue"})
@Profile("manager")
@Configuration
@EnableBatchIntegration
public class ManagerConfiguration {
    private final RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory;

    public ManagerConfiguration(
            RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory) {
        this.managerStepBuilderFactory = managerStepBuilderFactory;
    }

    @Bean
    public IntegrationFlow managerOutboundFlow(
            KafkaTemplate kafkaTemplate, @Qualifier("requests") MessageChannel requests) {
        return IntegrationFlow.from(requests)
                .handle(
                        Kafka.outboundChannelAdapter(kafkaTemplate)
                                .topicExpression(new LiteralExpression(MANAGER_REQUEST_TOPIC_NAME))
                                .partitionIdExpression(
                                        new FunctionExpression<>(this::distributePartitionId)))
                .get();
    }

    private long distributePartitionId(Message<StepExecutionRequest> message) {
        var sequenceSize =
                message.getHeaders()
                        .get(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE, Integer.class);
        var stepExecutionId = message.getPayload().getStepExecutionId();
        if (sequenceSize >= TOPIC_PARTITION_COUNT) {
            return stepExecutionId % TOPIC_PARTITION_COUNT;
        } else {
            return ThreadLocalRandom.current().nextInt(0, TOPIC_PARTITION_COUNT);
        }
    }

    @Bean
    public IntegrationFlow managerInboundFlow(
            ConsumerFactory consumerFactory, @Qualifier("replies") MessageChannel replies) {
        return IntegrationFlow.from(
                        Kafka.messageDrivenChannelAdapter(consumerFactory, WORKER_REPLY_TOPIC_NAME))
                .channel(replies)
                .get();
    }

    @Bean
    public Step managerStep(
            @Qualifier("requests") MessageChannel requests,
            @Qualifier("replies") MessageChannel replies) {
        return this.managerStepBuilderFactory
                .get("partitionerStep")
                .partitioner("workerStep", new CustomerPartitioner())
                .gridSize(16)
                .outputChannel(requests)
                .inputChannel(replies)
                .build();
    }

    @Bean
    public Job remotePartitioningJob(JobRepository jobRepository) {
        return new JobBuilder("partitioningJob-Kafka", jobRepository)
                .start(managerStep(null, null))
                .build();
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        var jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }
}
