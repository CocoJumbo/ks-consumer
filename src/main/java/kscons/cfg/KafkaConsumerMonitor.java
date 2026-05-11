package kscons.cfg;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumerMonitor {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaConsumerMonitor(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @EventListener
    public void onAppStarted(org.springframework.context.event.ContextRefreshedEvent event) {

        log.info("🔥 Kafka containers inspection starting...");

        registry.getListenerContainers().forEach(container -> {

            log.info("========== CONTAINER ==========");
            log.info("id: {}", container.getListenerId());
            log.info("group: {}", container.getGroupId());
            //log.info("concurrency: {}", container.getConcurrency());
            log.info("ackMode: {}", container.getContainerProperties().getAckMode());
            log.info("running: {}", container.isRunning());
        });
    }
}
