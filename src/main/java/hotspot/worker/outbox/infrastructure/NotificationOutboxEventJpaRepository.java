package hotspot.worker.outbox.infrastructure;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import hotspot.worker.outbox.infrastructure.entity.NotificationOutboxEventEntity;

public interface NotificationOutboxEventJpaRepository
        extends JpaRepository<NotificationOutboxEventEntity, UUID> {
}
