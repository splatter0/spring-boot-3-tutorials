package com.splatter0.worker;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.transaction.PlatformTransactionManager;

@Profile("worker")
@Configuration
@EnableBatchIntegration
public class WorkerConfiguration {
    private final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory;

    public WorkerConfiguration(
            RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory) {
        this.workerStepBuilderFactory = workerStepBuilderFactory;
    }

    @Bean
    public DirectChannel workerRequests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow workerInboundFlow(ConnectionFactory rabbitmqConnectionFactory) {
        return IntegrationFlow.from(Amqp.inboundAdapter(rabbitmqConnectionFactory, "requests"))
                .channel(workerRequests())
                .get();
    }

    @Bean
    public DirectChannel workerReplies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow workerOutboundFlow(RabbitTemplate rabbitTemplate) {
        return IntegrationFlow.from(workerReplies())
                .handle(Amqp.outboundAdapter(rabbitTemplate).routingKey("replies"))
                .get();
    }

    @Bean
    public Step workerStep(PlatformTransactionManager transactionManager) {
        return this.workerStepBuilderFactory
                .get("workerStep")
                .inputChannel(workerRequests())
                .outputChannel(workerReplies())
                .tasklet(tasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{stepExecutionContext['partition']}") String partition) {
        return (contribution, chunkContext) -> {
            System.out.println("processing " + partition);
            return RepeatStatus.FINISHED;
        };
    }
}
