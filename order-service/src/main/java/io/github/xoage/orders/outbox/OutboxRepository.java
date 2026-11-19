package io.github.xoage.orders.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    @Modifying
    @Query("delete from OutboxEvent e where e.sentAt < :before")
    int deleteSentBefore(Instant before);

    /// Пачка неотправленных событий по порядку. Строки блокируются до конца транзакции,
    /// занятые другим экземпляром сервиса пропускаются.
    @Query(value = """
            select * from outbox
            where sent_at is null
            order by created_at
            limit 50
            for update skip locked
            """, nativeQuery = true)
    List<OutboxEvent> lockNextBatch();
}