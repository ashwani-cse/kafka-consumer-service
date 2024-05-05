package com.kafka.consumer.subscriber;

import com.kafka.dto.MessagePayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * @author Ashwani Kumar
 * Created on 29/04/24.
 */
@Slf4j
@Component
public class KafkaMessageListener {

    private static final String TOPIC_WITH_PARTITIONS_3 = "topic-with-partitions-3";
    private static final String MULTI_INSTANCE_CONSUMER_GROUP = "multi-instance-consumer-group";

    private static final String MESSAGE_PAYLOAD_TOPIC = "message-payload-topic";

    private static final String MESSAGE_PAYLOAD_CONSUMER_GROUP = "message-payload-consumer-group";


    @KafkaListener(topics = TOPIC_WITH_PARTITIONS_3, groupId = MULTI_INSTANCE_CONSUMER_GROUP)
    public void consumeMessage1(String message) {
        log.info("Consumer 1: message consumed: {}", message);
    }

    @KafkaListener(topics = TOPIC_WITH_PARTITIONS_3, groupId = MULTI_INSTANCE_CONSUMER_GROUP)
    public void consumeMessage2(String message) {
        log.info("Consumer 2: message consumed: {}", message);
    }

    @KafkaListener(topics = TOPIC_WITH_PARTITIONS_3, groupId = MULTI_INSTANCE_CONSUMER_GROUP)
    public void consumeMessage3(String message) {
        log.info("Consumer 3: message consumed: {}", message);
    }

    @KafkaListener(topics = TOPIC_WITH_PARTITIONS_3, groupId = MULTI_INSTANCE_CONSUMER_GROUP)
    public void consumeMessage4(String message) {
        log.info("Consumer 4: message consumed: {}", message);
    }

    @KafkaListener(topics = MESSAGE_PAYLOAD_TOPIC, groupId = MESSAGE_PAYLOAD_CONSUMER_GROUP,
            topicPartitions = {
                    //@TopicPartition(topic = MESSAGE_PAYLOAD_TOPIC, partitions = {"0"}),
                    //@TopicPartition(topic = MESSAGE_PAYLOAD_TOPIC, partitions = {"1"}),
                    @TopicPartition(topic = MESSAGE_PAYLOAD_TOPIC, partitions = {"2"})   // consumer will consume from partition 2 only
            })
    public void consumeMessagePayload(MessagePayload payload) {
        log.info("MessagePayload Consumer: message consumed: {}", payload);
    }
}
