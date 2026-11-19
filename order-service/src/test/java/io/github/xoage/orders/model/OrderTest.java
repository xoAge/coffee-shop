package io.github.xoage.orders.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderTest {

    @Test
    void newOrder_movesForward() {
        Order order = new Order("капучино", "Тест");

        assertThat(order.changeStatus(OrderStatus.PREPARING)).isTrue();
        assertThat(order.changeStatus(OrderStatus.READY)).isTrue();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.READY);
    }

    @Test
    void readyOrder_ignoresLateOrDuplicateEvents() {
        Order order = new Order("капучино", "Тест");
        order.changeStatus(OrderStatus.READY);

        assertThat(order.changeStatus(OrderStatus.PREPARING)).isFalse();
        assertThat(order.changeStatus(OrderStatus.READY)).isFalse();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.READY);
    }

    @Test
    void newOrder_canSkipPreparing() {
        Order order = new Order("капучино", "Тест");

        assertThat(order.changeStatus(OrderStatus.READY)).isTrue();
    }
}