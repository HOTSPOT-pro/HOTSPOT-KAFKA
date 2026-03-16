package hotspot.worker.writer.infrastructure.entity;

import java.time.LocalDateTime;

public class UsageAppliedEventLogEntity {

    private Long appliedSeq;

    private String eventId;

    private long subId;

    private long familyId;

    private Long appId;

    private LocalDateTime occurredAt;

    private String yyyymm;

    private String yyyymmdd;

    private long usageAmount;

    private long giftUsed;

    private long planUsed;

    private long familyUsed;

    private String giftDetailJson;

    // 프레임워크 리플렉션 생성을 위한 기본 생성자다.
    protected UsageAppliedEventLogEntity() {
    }

    // 사용량 적용 로그 저장에 필요한 값을 채우는 생성자다.
    public UsageAppliedEventLogEntity(
            String eventId,
            long subId,
            long familyId,
            Long appId,
            LocalDateTime occurredAt,
            String yyyymm,
            String yyyymmdd,
            long usageAmount,
            long giftUsed,
            long planUsed,
            long familyUsed,
            String giftDetailJson
    ) {
        this.eventId = eventId;
        this.subId = subId;
        this.familyId = familyId;
        this.appId = appId;
        this.occurredAt = occurredAt;
        this.yyyymm = yyyymm;
        this.yyyymmdd = yyyymmdd;
        this.usageAmount = usageAmount;
        this.giftUsed = giftUsed;
        this.planUsed = planUsed;
        this.familyUsed = familyUsed;
        this.giftDetailJson = giftDetailJson;
    }

    // 로그 시퀀스 식별자를 반환한다.
    public Long getAppliedSeq() {
        return appliedSeq;
    }

    // 원본 이벤트 ID를 반환한다.
    public String getEventId() {
        return eventId;
    }

    // 회선 ID를 반환한다.
    public long getSubId() {
        return subId;
    }

    // 가족 ID를 반환한다.
    public long getFamilyId() {
        return familyId;
    }

    // 앱 ID를 반환한다.
    public Long getAppId() {
        return appId;
    }

    // 이벤트 발생 시각을 반환한다.
    public LocalDateTime getOccurredAt() {
        return occurredAt;
    }

    // 월 키(yyyyMM)를 반환한다.
    public String getYyyymm() {
        return yyyymm;
    }

    // 일 키(yyyyMMdd)를 반환한다.
    public String getYyyymmdd() {
        return yyyymmdd;
    }

    // 총 사용량을 반환한다.
    public long getUsageAmount() {
        return usageAmount;
    }

    // 선물 사용량을 반환한다.
    public long getGiftUsed() {
        return giftUsed;
    }

    // 요금제 사용량을 반환한다.
    public long getPlanUsed() {
        return planUsed;
    }

    // 가족풀 사용량을 반환한다.
    public long getFamilyUsed() {
        return familyUsed;
    }

    // 선물 차감 상세 JSON을 반환한다.
    public String getGiftDetailJson() {
        return giftDetailJson;
    }
}
