package com.splatter0.batch.worker;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.sql.DataSource;

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
    public Queue requestsQueue() {
        return new Queue("requests");
    }

    @Bean
    public Queue repliesQueue() {
        return new Queue("replies");
    }

    @Bean
    public DirectChannel workerRequests() {
        return new DirectChannel();
    }

    @Bean
    public DirectChannel workerReplies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow workerInboundFlow(ConnectionFactory rabbitmqConnectionFactory) {
        return IntegrationFlow.from(Amqp.inboundAdapter(rabbitmqConnectionFactory, "requests"))
                .channel(workerRequests())
                .get();
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
                .<Integer, Future<Customer>>chunk(5, transactionManager)
                .reader(workerReader(null))
                .processor(asyncWorkerProcessor())
                .writer(asyncWorkerWriter())
                .build();
    }

    @Bean
    public ItemWriter<Future<Customer>> asyncWorkerWriter() {
        var asyncItemWriter = new AsyncItemWriter<Customer>();
        asyncItemWriter.setDelegate(workerWriter(null));
        return asyncItemWriter;
    }

    @Bean
    public ItemWriter<Customer> workerWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Customer>()
                .sql("insert into customers (id) values (:id)")
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }

    @Bean
    public ItemProcessor<Integer, Future<Customer>> asyncWorkerProcessor() {
        var asyncItemProcessor = new AsyncItemProcessor<Integer, Customer>();
        asyncItemProcessor.setDelegate(workerProcessor());
        asyncItemProcessor.setTaskExecutor(batchAsyncExecutor());
        return asyncItemProcessor;
    }

    @Bean
    public ItemProcessor<Integer, Customer> workerProcessor() {
        return item -> {
            Thread.sleep(1000L);
            System.out.println(Thread.currentThread().getName() + "-item-" + item);
            return new Customer(item);
        };
    }

    @Bean
    @StepScope
    public ItemReader<Integer> workerReader(
            @Value("#{stepExecutionContext['ids']}") List<Integer> ids) {
        return new ListItemReader<>(ids);
    }

    @Bean
    public TaskExecutor batchAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(16);
        executor.setMaxPoolSize(32);
        executor.setQueueCapacity(100);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("batchAsync-");
        return executor;
    }
}
