package io.github.xoage.barista.kitchen;

import io.github.xoage.barista.config.KafkaTopicsConfig;
import io.github.xoage.barista.events.OrderCreatedEvent;
import io.github.xoage.barista.events.OrderStatusChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderListener(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "orders.created")
    public void onOrderCreated(OrderCreatedEvent event) throws InterruptedException {
        log.info("☕ Новый заказ {}: {} для {}", event.orderId(), event.drink(), event.customerName());
        sendStatus(event.orderId(), "PREPARING");

        Thread.sleep(5000);   // «готовим» 5 секунд

        sendStatus(event.orderId(), "READY");
        log.info("✅ Заказ {} готов: {}", event.orderId(), event.drink());
    }

    private void sendStatus(Long orderId, String status) {
        kafkaTemplate.send(KafkaTopicsConfig.ORDERS_STATUS_CHANGED,
                orderId.toString(),
                new OrderStatusChangedEvent(orderId, status));
    }
}
