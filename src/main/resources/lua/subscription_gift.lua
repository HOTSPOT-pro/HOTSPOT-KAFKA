-- KEYS:
-- 1) idemKey                 idem:sub:{eventId}
-- 2) receiverGiftLimitKey    limit:gift:{receiverSubId}:{giftId}:{yyyyMM}
-- 3) receiverGiftIdxKey      idx:gift:{receiverSubId}:{yyyyMM}        (ZSET)
-- 4) giverMonthUsageKey      usage:sub:{giverSubId}:{YYYYMM}          (HASH)
-- 5) giverDayUsageKey        usage:sub:{giverSubId}:{YYYYMMDD}        (HASH)
-- 6) giverMonthAppUsageKey   usage:app:{giverSubId}:{YYYYMM}
-- 7) giverDayAppUsageKey     usage:app:{giverSubId}:{YYYYMMDD}

-- ARGV:
-- 1) giftId                  (string/number)
-- 2) giftLimitBytes          (number)  -- 수신자에게 생성되는 gift 버킷 한도(=선물로 받은 양)
-- 3) giftAmountBytes         (number)  -- 발신자 사용량에 더할 값(=선물로 준 양)
-- 4) usageField              (string)  -- "plan_used"
-- 5) yyyyMM                  (string)  -- 수신자 idx key에도 들어가지만 혹시 검증/확장용
-- 6) yyyyMMDD                (string)  -- idem/검증/확장용 (현재는 key에 이미 반영됨)
-- 7) appId   (20)

-- 0) 멱등키(중복 이벤트 방지)
local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

-- 1) 수신자 gift 버킷 저장 (giftId별 한도)
--    ex) HSET limit:gift:10023:901:202602 gift_limit 1048576
redis.call('HSET', KEYS[2], 'gift_limit', tonumber(ARGV[2]))

-- 2) 수신자 gift 소진 순서 ZSET에 "맨 뒤에" 붙이기 (score 연속 보장)
--    - 현재 ZSET의 최대 score를 읽고 +1
--    - 아무것도 없으면 0부터 시작
local maxScore = redis.call('ZREVRANGE', KEYS[3], 0, 0, 'WITHSCORES')
local nextScore = 0
if #maxScore > 0 then
	nextScore = tonumber(maxScore[2]) + 1
end

-- 멤버는 giftId로 저장 (각 gift는 1번만 등록된다고 가정)
-- 이미 존재하면 score 갱신될 수 있으니 안전장치(선택):
-- "이미 있으면" 새로 append 하지 않고 종료하고 싶으면 아래 2줄을 추가할 수 있음.
-- local exists = redis.call('ZSCORE', KEYS[3], ARGV[1])
-- if exists then return 1 end

redis.call('ZADD', KEYS[3], nextScore, ARGV[1])

-- 3) 발신자 월 사용량 증가
redis.call('HINCRBY', KEYS[4], ARGV[4], tonumber(ARGV[3]))

-- 4) 발신자 일 사용량 증가
redis.call('HINCRBY', KEYS[5], ARGV[4], tonumber(ARGV[3]))

-- 5) 발신자 월 app 사용량 증가
redis.call(
	'ZINCRBY',
	KEYS[6],
	tonumber(ARGV[3]),
	ARGV[7]
)

-- 6) 발신자 일 app 사용량 증가
redis.call(
	'ZINCRBY',
	KEYS[7],
	tonumber(ARGV[3]),
	ARGV[7]
)

return 1