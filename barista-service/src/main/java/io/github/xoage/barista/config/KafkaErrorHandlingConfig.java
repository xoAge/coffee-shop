package io.github.xoage.barista.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/// Что делать, если бариста не смог обработать заказ.
@Configuration
public class KafkaErrorHandlingConfig {

    public static final String ORDERS_CREATED_DLT = "orders.created.dlt";

    /// Топик для заказов, которые не удалось обработать.
    @Bean
    public NewTopic ordersCreatedDltTopic() {
        return TopicBuilder.name(ORDERS_CREATED_DLT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // после неудачи - отправить сообщение в <исходный топик>.dlt, в ту же партицию
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".dlt", record.partition()));

        // 1 попытка + 2 повтора с паузой 1 секунда
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));

        // ошибки в данных повторять бессмысленно - сразу в DLT
        handler.addNotRetryableExceptions(IllegalArgumentException.class);

        return handler;
    }
}
