package kscons.cfg;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic simpleTopic() {
        return new NewTopic(
                "simple-topic",
                // Topic name (must be unique in Kafka cluster)
                // Used by producers/consumers to send/read messages

                1,
                // Number of partitions
                // -> Defines parallelism and scalability
                // -> More partitions = more consumers can read in parallel
                // -> Ordering is guaranteed ONLY within a single partition

                (short) 1
                // Replication factor
                // -> Number of copies of the data across brokers
                // -> 1 = no redundancy (OK for local dev)
                // -> In production usually 2 or 3 for fault tolerance
        );
    }

    @Bean
    public NewTopic hashBalanced3PartitionsTopic() {
        return new NewTopic(
                "hash-balanced-3partition-topic",
                3,
                (short) 1
        );
    }
}
