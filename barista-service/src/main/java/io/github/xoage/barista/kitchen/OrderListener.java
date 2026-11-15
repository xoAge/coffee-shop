package io.github.xoage.barista.kitchen;

import io.github.xoage.barista.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    @KafkaListener(topics = "orders.created")
    public void onOrderCreated(OrderCreatedEvent event) throws InterruptedException {
        log.info("☕ Новый заказ {}: {} для {}", event.orderId(), event.drink(), event.customerName());

        Thread.sleep(5000);   // «готовим» 5 секунд

        log.info("✅ Заказ {} готов: {}", event.orderId(), event.drink());
    }
}
