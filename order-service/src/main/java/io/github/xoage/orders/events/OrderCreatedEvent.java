package io.github.xoage.orders.events;

/// Событие «заказ создан». Отправляется в топик orders.created в формате JSON.
public record OrderCreatedEvent(Long orderId, String drink, String customerName) {
}
