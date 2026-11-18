package io.github.xoage.barista;

import io.github.xoage.barista.events.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/// barista-service + настоящая Kafka. Готовим быстро, чтобы тест не ждал 5 секунд.
@SpringBootTest(properties = "barista.preparation-time=100ms")
@Import(TestcontainersConfiguration.class)
class BaristaIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaContainer kafka;

    @Test
    void orderCreated_publishesPreparingThenReady() {
        try (KafkaConsumer<String, String> consumer = testConsumer("orders.status-changed")) {

            kafkaTemplate.send("orders.created", "42", new OrderCreatedEvent(42L, "капучино", "Тест"));

            List<String> statusesOfOrder42 = new ArrayList<>();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if ("42".equals(record.key())) {
                        statusesOfOrder42.add(record.value());
                    }
                }
                assertThat(statusesOfOrder42).hasSize(2);
            });

            assertThat(statusesOfOrder42.get(0)).contains("\"status\":\"PREPARING\"");
            assertThat(statusesOfOrder42.get(1)).contains("\"status\":\"READY\"");
        }
    }

    private KafkaConsumer<String, String> testConsumer(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        consumer.subscribe(List.of(topic));
        return consumer;
    }
}
