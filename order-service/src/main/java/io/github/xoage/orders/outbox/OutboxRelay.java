package io.github.xoage.orders.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/// Ретранслятор: забирает неотправленные события из outbox и публикует их в Kafka.
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxRelay(OutboxRepository outboxRepository,
                       KafkaTemplate<String, Object> kafkaTemplate,
                       ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outboxRepository.lockNextBatch();

        for (OutboxEvent event : batch) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(),
                                objectMapper.readTree(event.getPayload()))
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.warn("Kafka недоступна, событие {} отправим позже: {}", event.getId(), e.getMessage());
                return;
            }
            event.markSent();
            log.info("Событие {} отправлено в {}", event.getId(), event.getTopic());
        }
    }
    /// Раз в час удаляет события, отправленные больше суток назад.
    @Scheduled(cron = "0 0 * * * *")
    @Transactional
    public void cleanUp() {
        int deleted = outboxRepository.deleteSentBefore(Instant.now().minus(Duration.ofDays(1)));
        if (deleted > 0) {
            log.info("Удалено отправленных событий из outbox: {}", deleted);
        }
    }
}