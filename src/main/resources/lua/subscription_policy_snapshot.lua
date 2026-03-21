-- KEYS:
-- 1) idemKey     idem:sub:{eventId}
-- 2) repeatKey   block:repeat:{subId}   (HASH)
-- 3) timeKey     block:time:{subId}     (ZSET)
--
-- ARGV: 4개 단위 반복
-- [policyId, policyType, encoded, expireEpoch] ...

-- idem (중복 방지) + TTL 권장
-- 필요 없으면 EX 제거 가능
local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 604800) -- 7 days
if not ok then
	return 0
end

-- 기존 정책 전부 삭제 (스냅샷 교체)
redis.call('DEL', KEYS[2])
redis.call('DEL', KEYS[3])

local argc = #ARGV
if (argc % 4) ~= 0 then
	return redis.error_reply("ARGV length must be multiple of 4")
end

local i = 1
while i <= argc do
	local policyId = ARGV[i]
	local policyType = ARGV[i + 1]
	local encoded = ARGV[i + 2]
	local expireEpoch = ARGV[i + 3]

	if policyType == 'SCHEDULED' then
	-- repeatKey: HASH(policyId -> encoded)
		redis.call('HSET', KEYS[2], policyId, encoded)
	else
	-- timeKey: ZSET(score=expireEpoch, member=policyId)
		redis.call('ZADD', KEYS[3], tonumber(expireEpoch), policyId)
	end

	i = i + 4
end

return 1