package io.github.xoage.orders.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/// Событие, ожидающее отправки в Kafka. Сохраняется в той же транзакции, что и бизнес-данные.
@Entity
@Table(name = "outbox")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    private String aggregateId;

    private String topic;

    private String payload;

    private Instant createdAt;

    private Instant sentAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(String aggregateId, String topic, String payload) {
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getAggregateId() { return aggregateId; }
    public String getTopic() { return topic; }
    public String getPayload() { return payload; }
    public Instant getSentAt() { return sentAt; }

    public void markSent() {
        this.sentAt = Instant.now();
    }
}
