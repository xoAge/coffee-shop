package io.github.xoage.orders.web;

import io.github.xoage.orders.model.Order;
import io.github.xoage.orders.model.OrderStatus;

import java.time.Instant;

public record OrderResponse(Long id, String drink, String customerName, OrderStatus status, Instant createdAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(order.getId(), order.getDrink(), order.getCustomerName(),
                order.getStatus(), order.getCreatedAt());
    }
}
