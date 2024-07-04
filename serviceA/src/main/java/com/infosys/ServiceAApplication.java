package com.infosys;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class ServiceAApplication {
	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job sendMessagesJob;
	public static void main(String[] args) {
		SpringApplication.run(ServiceAApplication.class, args);
	}
	@Bean
	public CommandLineRunner run() {
		return args -> {
			JobExecution jobExecution = jobLauncher.run(sendMessagesJob, new JobParametersBuilder().toJobParameters());
			System.out.println("Job Status : " + jobExecution.getStatus());
			System.out.println("Job completed");
		};
	}
}
