package io.github.xoage.barista.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    public static final String ORDERS_STATUS_CHANGED = "orders.status-changed";

    @Bean
    public NewTopic ordersStatusChangedTopic() {
        return TopicBuilder.name(ORDERS_STATUS_CHANGED)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
