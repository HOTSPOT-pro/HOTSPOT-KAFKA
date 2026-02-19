-- KEYS:
-- 1) idemKey         idem:sub:{eventId}
-- 2) repeatKey       block:repeat:{subId}
-- 3) timeKey         block:time:{subId}
--
-- ARGV:
-- 1) action          APPLY | REMOVE
-- 2) policyType      SCHEDULED | ONCE   (REMOVE면 무시 가능)
-- 3) policyId
-- 4) encoded         (SCHEDULED일 때만 사용)
-- 5) expireEpoch     (ONCE일 때만 사용)

local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

local action = ARGV[1]
local policyType = ARGV[2]
local policyId = ARGV[3]

if action == 'APPLY' then
	if policyType == 'SCHEDULED' then
		local encoded = ARGV[4]
		redis.call('HSET', KEYS[2], policyId, encoded)
	else
		local expireEpoch = tonumber(ARGV[5])
		redis.call('ZADD', KEYS[3], expireEpoch, policyId)
	end
else
-- 제거는 둘 다 시도 (정합성 안전)
	redis.call('HDEL', KEYS[2], policyId)
	redis.call('ZREM', KEYS[3], policyId)
end

return 1