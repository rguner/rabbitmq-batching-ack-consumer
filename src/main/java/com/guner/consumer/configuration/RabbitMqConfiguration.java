package com.guner.consumer.configuration;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

@Configuration
public class RabbitMqConfiguration {

    @Value("${batch-consumer.topic-exchange.name}")
    private String topicExchange;

    @Value("${batch-consumer.queue.name.batch-queue}")
    private String queueBatch;

    @Value("${batch-consumer.routing.key.batch-routing}")
    private String routingKeyBatch;

    @Bean
    public Queue queueBatch() {
        return new Queue(queueBatch);
    }


    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(topicExchange);
    }

    @Bean
    public Binding bindingBatch() {
        return BindingBuilder
                .bind(queueBatch())
                .to(topicExchange())
                .with(routingKeyBatch);
    }

    @Bean
    public MessageConverter converter() {
        return new Jackson2JsonMessageConverter();
    }


    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(converter());
        factory.setDeBatchingEnabled(true);
        return factory;
    }


    @Bean
    public SimpleRabbitListenerContainerFactory rabbitBatchListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(converter());
        factory.setConsumerBatchEnabled(true);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        return factory;
    }


}
