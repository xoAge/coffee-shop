package io.github.xoage.orders.repository;

import io.github.xoage.orders.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}
