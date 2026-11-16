package io.github.xoage.orders.events;

/// Событие из топика orders.status-changed, как его видит order-service.
public record OrderStatusChangedEvent(Long orderId, String status) {
}
