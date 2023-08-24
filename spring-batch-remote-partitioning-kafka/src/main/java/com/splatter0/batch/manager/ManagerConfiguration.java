package com.splatter0.batch.manager;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;

@SuppressWarnings({"rawtypes", "unchecked"})
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
    public DirectChannel managerRequests() {
        return new DirectChannel();
    }

    @Bean
    public DirectChannel managerReplies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow managerOutboundFlow(KafkaTemplate kafkaTemplate) {
        return IntegrationFlow.from(managerRequests())
                .handle(
                        Kafka.outboundChannelAdapter(kafkaTemplate)
                                .topicExpression(new LiteralExpression("requests"))
                                .partitionIdExpression(
                                        new FunctionExpression<Message<StepExecutionRequest>>(
                                                (m) -> {
                                                    StepExecutionRequest executionRequest =
                                                            m.getPayload();
                                                    return executionRequest.getStepExecutionId()
                                                            % 3;
                                                })))
                .get();
    }

    @Bean
    public IntegrationFlow managerInboundFlow(ConsumerFactory consumerFactory) {
        return IntegrationFlow.from(Kafka.messageDrivenChannelAdapter(consumerFactory, "replies"))
                .channel(managerReplies())
                .get();
    }

    @Bean
    public Step managerStep() {
        return this.managerStepBuilderFactory
                .get("partitionerStep")
                .partitioner("workerStep", new CustomerPartitioner())
                .gridSize(3)
                .outputChannel(managerRequests())
                .inputChannel(managerReplies())
                .build();
    }

    @Bean
    public Job remotePartitioningJob(JobRepository jobRepository) {
        return new JobBuilder("partitioningJob-Kafka", jobRepository)
                .start(managerStep())
                // .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        var jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }
}
