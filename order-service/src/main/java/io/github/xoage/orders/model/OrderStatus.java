package io.github.xoage.orders.model;

/// Жизненный цикл заказа: создан -> бариста готовит -> готов.
public enum OrderStatus {
    NEW,
    PREPARING,
    READY
}
