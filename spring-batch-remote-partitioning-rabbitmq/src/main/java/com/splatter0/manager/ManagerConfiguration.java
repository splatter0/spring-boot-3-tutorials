package com.splatter0.manager;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;

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
    public Queue requestsQueue() {
        return new Queue("requests");
    }

    @Bean
    public Queue repliesQueue() {
        return new Queue("replies");
    }

    @Bean
    public DirectChannel managerRequests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow managerOutboundFlow(RabbitTemplate rabbitTemplate) {
        return IntegrationFlow.from(managerRequests())
                .handle(Amqp.outboundAdapter(rabbitTemplate).routingKey("requests"))
                .get();
    }

    @Bean
    public DirectChannel managerReplies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow managerInboundFlow(ConnectionFactory rabbitmqConnectionFactory) {
        return IntegrationFlow.from(Amqp.inboundAdapter(rabbitmqConnectionFactory, "replies"))
                .channel(managerReplies())
                .get();
    }

    @Bean
    public Step managerStep() {
        return this.managerStepBuilderFactory
                .get("partitionerStep")
                .partitioner("workerStep", new SimplePartitioner())
                .gridSize(3)
                .outputChannel(managerRequests())
                .inputChannel(managerReplies())
                .build();
    }

    @Bean
    public Job remotePartitioningJob(JobRepository jobRepository) {
        return new JobBuilder("partitioningJob", jobRepository)
                .start(managerStep())
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        final JobRegistryBeanPostProcessor answer = new JobRegistryBeanPostProcessor();
        answer.setJobRegistry(jobRegistry);
        return answer;
    }
}
