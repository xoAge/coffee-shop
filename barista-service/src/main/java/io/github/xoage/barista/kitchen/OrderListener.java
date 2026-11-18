package io.github.xoage.barista.kitchen;

import io.github.xoage.barista.config.KafkaTopicsConfig;
import io.github.xoage.barista.events.OrderCreatedEvent;
import io.github.xoage.barista.events.OrderStatusChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Duration preparationTime;

    public OrderListener(KafkaTemplate<String, Object> kafkaTemplate,
                         @Value("${barista.preparation-time}") Duration preparationTime) {
        this.kafkaTemplate = kafkaTemplate;
        this.preparationTime = preparationTime;
    }

    @KafkaListener(topics = "orders.created")
    public void onOrderCreated(OrderCreatedEvent event) throws InterruptedException {
        log.info("☕ Новый заказ {}: {} для {}", event.orderId(), event.drink(), event.customerName());
        // демонстрационная поломка: так можно вызвать ошибку обработки по желанию
        if ("сломанный".equalsIgnoreCase(event.drink())) {
            throw new IllegalStateException("Кофемашина сломалась на заказе " + event.orderId());
        }
        sendStatus(event.orderId(), "PREPARING");

        Thread.sleep(preparationTime.toMillis());

        sendStatus(event.orderId(), "READY");
        log.info("✅ Заказ {} готов: {}", event.orderId(), event.drink());
    }

    private void sendStatus(Long orderId, String status) {
        kafkaTemplate.send(KafkaTopicsConfig.ORDERS_STATUS_CHANGED,
                orderId.toString(),
                new OrderStatusChangedEvent(orderId, status));
    }
}
