package kscons.consumers;

import kscons.dto.SimpleMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
public class BaseConsumer {

    private final ObjectMapper objectMapper;

    public BaseConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // Manual ack, while you rerun the spring app, listener of this topic will read from last manual commit offset
    @KafkaListener(topics = "simple-topic",
                   groupId = "simple-group",
                   containerFactory = "manualAckFactory")
    public void listenSimpleTopic(ConsumerRecord<String, String> record) {

        SimpleMessage obj = objectMapper.readValue(record.value(), SimpleMessage.class);

        log.info("🔥 RECEIVED: Value {}", obj.toString());

        log.info("Kafka metadata → key={}, partition={}, offset={}",
                record.key(),
                record.partition(),
                record.offset()
        );

        log.debug("Raw message: {}", record.value());
    }


    @KafkaListener(topics = "simple-topic", groupId = "another-simple-group")
    public void listenSimpleTopicAnotherGroup(ConsumerRecord<String, String> record) {

        SimpleMessage obj = objectMapper.readValue(record.value(), SimpleMessage.class);

        log.info("🔥 RECEIVED: Value {}", obj.toString());

        log.info("Kafka metadata → key={}, partition={}, offset={}",
                record.key(),
                record.partition(),
                record.offset()
        );

        log.debug("Raw message: {}", record.value());
    }

    @KafkaListener(
            topics = "hash-balanced-3partition-topic",
            groupId = "hash-balanced-3partition-group",
            concurrency = "3"
    )
    // Messages with the same key → always go to the same partition!
    public void listenHashTopic3partitions(ConsumerRecord<String, String> record) {

        SimpleMessage obj = objectMapper.readValue(record.value(), SimpleMessage.class);

        log.info(
                "🔥 RECEIVED: value={}, key={}, partition={}, offset={}, thread={}",
                obj,
                record.key(),
                record.partition(),
                record.offset(),
                Thread.currentThread().getName()
        );

        log.debug("Raw message: {}", record.value());
    }


    @KafkaListener(
            topics = "round-robin-balanced-3partition-topic",
            groupId = "round-robin-balanced-3partition-group",
            concurrency = "3"
    )
    public void listenRoundRobinTopic3partitions(ConsumerRecord<String, String> record) {

        SimpleMessage obj = objectMapper.readValue(record.value(), SimpleMessage.class);

        log.info(
                "🔥 RECEIVED: value={}, key={}, partition={}, offset={}, thread={}",
                obj,
                record.key(),
                record.partition(),
                record.offset(),
                Thread.currentThread().getName()
        );

        log.debug("Raw message: {}", record.value());
    }
}
