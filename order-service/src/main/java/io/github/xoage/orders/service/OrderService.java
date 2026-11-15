package io.github.xoage.orders.service;

import io.github.xoage.orders.model.Order;
import io.github.xoage.orders.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public Order create(String drink, String customerName) {
        return orderRepository.save(new Order(drink, customerName));
    }

    @Transactional(readOnly = true)
    public Order findById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказа с id " + id + " нет"));
    }
}
