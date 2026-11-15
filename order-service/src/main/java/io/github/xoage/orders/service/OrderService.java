package io.github.xoage.orders.service;

import io.github.xoage.orders.config.KafkaTopicsConfig;
import io.github.xoage.orders.events.OrderCreatedEvent;
import io.github.xoage.orders.model.Order;
import io.github.xoage.orders.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderService(OrderRepository orderRepository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.orderRepository = orderRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public Order create(String drink, String customerName) {
        Order order = orderRepository.save(new Order(drink, customerName));

        OrderCreatedEvent event = new OrderCreatedEvent(order.getId(), order.getDrink(), order.getCustomerName());
        kafkaTemplate.send(KafkaTopicsConfig.ORDERS_CREATED, order.getId().toString(), event);
        log.info("Отправлено событие: заказ {} создан ({})", order.getId(), order.getDrink());

        return order;
    }

    @Transactional(readOnly = true)
    public Order findById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказа с id " + id + " нет"));
    }
}
