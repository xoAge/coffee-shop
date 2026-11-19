package io.github.xoage.orders.service;

import io.github.xoage.orders.config.KafkaTopicsConfig;
import io.github.xoage.orders.events.OrderCreatedEvent;
import io.github.xoage.orders.model.Order;
import io.github.xoage.orders.model.OrderStatus;
import io.github.xoage.orders.outbox.OutboxEvent;
import io.github.xoage.orders.outbox.OutboxRepository;
import io.github.xoage.orders.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Order create(String drink, String customerName) {
        Order order = orderRepository.save(new Order(drink, customerName));

        OrderCreatedEvent event = new OrderCreatedEvent(order.getId(), order.getDrink(), order.getCustomerName());
        outboxRepository.save(new OutboxEvent(
                order.getId().toString(),
                KafkaTopicsConfig.ORDERS_CREATED,
                objectMapper.writeValueAsString(event)));
        log.info("Заказ {} создан, событие записано в outbox", order.getId());

        return order;
    }

    @Transactional
    public void changeStatus(Long orderId, OrderStatus status) {
        orderRepository.findById(orderId).ifPresentOrElse(
                order -> {
                    if (order.changeStatus(status)) {
                        log.info("Заказ {}: статус {}", orderId, status);
                    } else {
                        log.info("Заказ {}: переход {} -> {} не разрешён, событие пропущено",
                                orderId, order.getStatus(), status);
                    }
                },
                () -> log.warn("Событие о статусе для несуществующего заказа {}, пропускаем", orderId)
        );
    }

    @Transactional(readOnly = true)
    public Order findById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказа с id " + id + " нет"));
    }
}
