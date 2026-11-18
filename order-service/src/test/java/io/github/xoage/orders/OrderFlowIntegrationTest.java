package io.github.xoage.orders;

import io.github.xoage.orders.events.OrderStatusChangedEvent;
import io.github.xoage.orders.model.Order;
import io.github.xoage.orders.model.OrderStatus;
import io.github.xoage.orders.service.OrderService;
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

/// Весь order-service + настоящие PostgreSQL и Kafka.
@SpringBootTest(properties = "ORDERS_DB_PASSWORD=unused")
@Import(TestcontainersConfiguration.class)
class OrderFlowIntegrationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaContainer kafka;

    @Test
    void createOrder_publishesOrderCreatedEvent() {
        try (KafkaConsumer<String, String> consumer = testConsumer("orders.created")) {

            Order order = orderService.create("латте", "Тест");

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                assertThat(received).anySatisfy(record -> {
                    assertThat(record.key()).isEqualTo(order.getId().toString());
                    assertThat(record.value()).contains("\"drink\":\"латте\"");
                });
            });
        }
    }

    @Test
    void statusChangedEvent_updatesOrderStatus() {
        Order order = orderService.create("раф", "Тест");
        assertThat(order.getStatus()).isEqualTo(OrderStatus.NEW);

        kafkaTemplate.send("orders.status-changed", order.getId().toString(),
                new OrderStatusChangedEvent(order.getId(), "READY"));

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(orderService.findById(order.getId()).getStatus()).isEqualTo(OrderStatus.READY));
    }

    /// Свой читатель топика: уникальная группа, ключ и значение - простые строки.
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
