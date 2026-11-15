package io.github.xoage.barista.events;

/// Событие из топика orders.created, как его видит бариста.
public record OrderCreatedEvent(Long orderId, String drink, String customerName) {
}
