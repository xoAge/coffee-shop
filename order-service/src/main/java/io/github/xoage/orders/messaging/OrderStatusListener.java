package io.github.xoage.orders.messaging;

import io.github.xoage.orders.events.OrderStatusChangedEvent;
import io.github.xoage.orders.model.OrderStatus;
import io.github.xoage.orders.service.OrderService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/// Слушает события бариста и обновляет статус заказа.
@Component
public class OrderStatusListener {

    private final OrderService orderService;

    public OrderStatusListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = "orders.status-changed")
    public void onStatusChanged(OrderStatusChangedEvent event) {
        orderService.changeStatus(event.orderId(), OrderStatus.valueOf(event.status()));
    }
}
