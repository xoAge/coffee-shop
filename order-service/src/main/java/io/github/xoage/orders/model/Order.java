package io.github.xoage.orders.model;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String drink;

    private String customerName;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private Instant createdAt;

    protected Order() {
    }

    public Order(String drink, String customerName) {
        this.drink = drink;
        this.customerName = customerName;
        this.status = OrderStatus.NEW;
        this.createdAt = Instant.now();
    }

    public Long getId() { return id; }
    public String getDrink() { return drink; }
    public String getCustomerName() { return customerName; }
    public OrderStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }
}
