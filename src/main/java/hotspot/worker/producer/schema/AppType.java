package hotspot.worker.producer.schema;

import java.util.concurrent.ThreadLocalRandom;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum AppType {

    MSG_KAKAO,
    MSG_LINE,

    MEDIA_YOUTUBE,
    MEDIA_NETFLIX,
    MEDIA_CHZZK,
    MEDIA_SOOP,

    SNS_INSTAGRAM,
    SNS_TIKTOK,
    SNS_FACEBOOK,

    STUDY_EBS,
    STUDY_MEGA,

    FIN_UPBIT,
    FIN_KIWOOM,

    WEB_CHROME,
    WEB_SAFARI,

    GAME_TFT,
    GAME_PUBG_M,

    TOON_NAVER,
    TOON_KAKAO,

    GIFT_DATA;

    private static final AppType[] VALUES = values();

    public static String randomCode() {
        return VALUES[
                ThreadLocalRandom.current().nextInt(VALUES.length)
                ].name();
    }
}
