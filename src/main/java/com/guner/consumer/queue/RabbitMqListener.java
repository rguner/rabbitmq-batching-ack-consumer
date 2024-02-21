package com.guner.consumer.queue;

import com.guner.consumer.entity.ChargingRecord;
import com.guner.consumer.service.ChargingRecordService;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.springframework.amqp.support.AmqpHeaders.DELIVERY_TAG;

/**
 * https://docs.spring.io/spring-amqp/docs/current/reference/html/#receiving-batch
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RabbitMqListener {

    private final ChargingRecordService chargingRecordService;

    /**
     * with rabbitBatchListenerContainerFactory, it consumes messages as batch
     */
    /*
    //you don’t have access to the delivery_tag property. manual ack is not possible
    @RabbitListener(queues = "${batch-consumer.queue.name.batch-queue}", containerFactory = "rabbitBatchListenerContainerFactory") // ackMode = "MANUAL" which allows you to override the container factory’s acknowledgeMode property.
    public void listenBatch(List<ChargingRecord> listChargingRecord) {
        log.debug("Charging List: Received <{} {}> , thread: {}", listChargingRecord.size(), Thread.currentThread().getName());
        listChargingRecord.forEach(chargingRecord -> chargingRecordService.createChargingRecord(chargingRecord));
    }
     */

    /**
     * with rabbitBatchListenerContainerFactory, it consumes messages as batch. List<Message> is possible but needs to be converted to ChargingRecord
     */
    /*
    With Message, you can access the delivery_tag property and manual ack is possible
    @RabbitListener(queues = "${batch-consumer.queue.name.batch-queue}", containerFactory = "rabbitBatchListenerContainerFactory")
    public void listenBatch(List<Message> messageList,
                            Channel channel) {
        log.debug("Charging List as Message: Received <{} {}> , thread: {}", messageList.size(), Thread.currentThread().getName());
        //messageList.forEach(message -> chargingRecordService.createChargingRecord((ChargingRecord)message.getBody());
        int batchSize = messageList.size();
        long deliveryTag = messageList.get(batchSize - 1).getMessageProperties().getDeliveryTag();
        try {
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("Error while acknowledging message", e);
        }
    }
     */

    /*
        With org.springframework.messaging.Message, you can access the delivery_tag property and manual ack is possible
        With org.springframework.messaging.Message<ChargingRecord>>  you can access ChargingRecord directly
     */
    @RabbitListener(queues = "${batch-consumer.queue.name.batch-queue}", containerFactory = "rabbitBatchListenerContainerFactory")
    public void listenBatchWithSpringMessage(List<org.springframework.messaging.Message<ChargingRecord>> listMessagesChargingRecord,
                                             Channel channel) {
        log.debug("Charging List as Message: Received <{} {}> , thread: {}", listMessagesChargingRecord.size(), Thread.currentThread().getName());
        listMessagesChargingRecord.forEach(message -> chargingRecordService.createChargingRecord(message.getPayload()));
        int batchSize = listMessagesChargingRecord.size();
        // Channel channel = listMessagesChargingRecord.get(batchSize - 1).getHeaders().get(AmqpHeaders.CHANNEL, Channel.class); // is also possible
        long deliveryTag = listMessagesChargingRecord.get(batchSize - 1).getHeaders().get(AmqpHeaders.DELIVERY_TAG, Long.class);
        try {
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("Error while acknowledging message", e);
        }
    }



}
