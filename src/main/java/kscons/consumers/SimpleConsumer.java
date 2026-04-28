package kscons.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SimpleConsumer {

    @KafkaListener(topics = "simple-topic", groupId = "simple-group")
    public void listen(ConsumerRecord<String, String> record) {
        System.out.println("🔥 RECEIVED: " + record.value());

        String key = record.key() != null ? record.key() : "null";
        String value = record.value();
        System.out.println("Key: " + key);
        System.out.println("Value: " + value);
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
    }
}
