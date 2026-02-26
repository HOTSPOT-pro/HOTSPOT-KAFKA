package hotspot.worker.outbox.infrastructure.entity;

import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "notification_outbox_event")
public class NotificationOutboxEventEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "aggregatetype", nullable = false, length = 100)
    private String aggregateType;

    @Column(name = "aggregateid", nullable = false, length = 100)
    private String aggregateId;

    @Column(name = "type", nullable = false, length = 100)
    private String type;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    protected NotificationOutboxEventEntity() {
    }

    public NotificationOutboxEventEntity(
            UUID id,
            String aggregateType,
            String aggregateId,
            String type,
            String payload
    ) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.type = type;
        this.payload = payload;
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }
}
