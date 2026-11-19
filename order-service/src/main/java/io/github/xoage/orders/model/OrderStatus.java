package io.github.xoage.orders.model;

/// Жизненный цикл заказа: создан -> бариста готовит -> готов.
public enum OrderStatus {
    NEW,
    PREPARING,
    READY;

    /// Можно ли перейти из этого статуса в next.
    public boolean canChangeTo(OrderStatus next) {
        return switch (this) {
            case NEW -> next == PREPARING || next == READY;
            case PREPARING -> next == READY;
            case READY -> false;
        };
    }
}
