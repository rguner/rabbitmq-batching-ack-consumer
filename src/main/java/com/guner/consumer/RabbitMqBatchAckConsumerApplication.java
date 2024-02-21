package com.guner.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RabbitMqBatchAckConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(RabbitMqBatchAckConsumerApplication.class, args);
	}

}
