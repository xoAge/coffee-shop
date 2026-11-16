package io.github.xoage.barista.events;

/// Событие «статус заказа изменился». Отправляется в топик orders.status-changed.
public record OrderStatusChangedEvent(Long orderId, String status) {
}
