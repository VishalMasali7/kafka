package com.infosys.config;

import com.infosys.Dto.MessageDTO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
@EnableBatchProcessing
@EnableTransactionManagement
public class BatchConfig {

    @Autowired
    private KafkaTemplate<String, MessageDTO> kafkaTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private DataSource dataSource; // Inject DataSource configured by Spring Boot

    @Bean
    public Job job(JobRepository jobRepository, Step step) {
        return new JobBuilder("job", jobRepository)
                .start(step)
                .build();
    }

    @Bean
    public Step step(JobRepository jobRepository) {
        return new StepBuilder("step",jobRepository)
                .<MessageDTO, MessageDTO>chunk(1000,transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .transactionManager(transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<MessageDTO> reader() {
        List<MessageDTO> messages = IntStream.range(0, 1000)
                .mapToObj(i -> {
                    MessageDTO dto = new MessageDTO();
                    dto.setMessage("Message " + i);
                    return dto;
                })
                .collect(Collectors.toList());
        return new ListItemReader<>(messages);
    }

    @Bean
    @StepScope
    public ItemProcessor<MessageDTO, MessageDTO> processor() {
        return item -> item;
    }

    @Bean
    @StepScope
    public ItemWriter<MessageDTO> writer() {
        return items -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            // Send to Kafka in bulk
            CompletableFuture<Void> kafkaFuture = CompletableFuture.runAsync(() -> {
                items.forEach(item -> kafkaTemplate.send("test-topic", item));
            });
            futures.add(kafkaFuture);

            // Send via REST in parallel
            RestTemplate restTemplate = restTemplate();
            items.forEach(item -> {
                CompletableFuture<Void> restFuture = CompletableFuture.runAsync(() -> {
                    restTemplate.postForObject("http://localhost:8081/endpoint", item, String.class);
                });
                futures.add(restFuture);
            });

            // Wait for all futures to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        };
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
