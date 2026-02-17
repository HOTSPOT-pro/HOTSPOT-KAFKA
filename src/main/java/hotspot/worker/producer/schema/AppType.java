package hotspot.worker.producer.schema;

import java.util.concurrent.ThreadLocalRandom;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum AppType {

    MSG_KAKAO(1L),
    MSG_LINE(2L),

    MEDIA_YOUTUBE(3L),
    MEDIA_NETFLIX(4L),
    MEDIA_CHZZK(5L),
    MEDIA_SOOP(6L),

    SNS_INSTAGRAM(7L),
    SNS_TIKTOK(8L),
    SNS_FACEBOOK(9L),

    STUDY_EBS(10L),
    STUDY_MEGA(11L),

    FIN_UPBIT(12L),
    FIN_KIWOOM(13L),

    WEB_CHROME(14L),
    WEB_SAFARI(15L),

    GAME_TFT(16L),
    GAME_PUBG_M(17L),

    TOON_NAVER(18L),
    TOON_KAKAO(19L),

    GIFT_DATA(20L);

    private final Long appId;

    private static final AppType[] VALUES = values();

    public static AppType random() {
        return VALUES[
                ThreadLocalRandom.current().nextInt(VALUES.length)
                ];
    }

    public static Long randomAppId() {
        return random().getAppId();
    }
}
