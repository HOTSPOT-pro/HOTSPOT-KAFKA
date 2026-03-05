-- KEYS
-- 1 idem key

-- ARGV
-- 1 policyId
-- 2 policyType
-- 3 encoded
-- 4 expireEpoch
-- 5... subIds

local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 604800)

if not ok then
	return 0
end

local policyId = ARGV[1]
local policyType = ARGV[2]
local encoded = ARGV[3]
local expireEpoch = ARGV[4]

for i = 5, #ARGV do

	local subId = ARGV[i]

	local repeatKey = 'block:repeat:' .. subId
	local timeKey = 'block:time:' .. subId

	-- 기존 정책 제거 (핵심)
	redis.call('HDEL', repeatKey, policyId)
	redis.call('ZREM', timeKey, policyId)

	-- 새 정책 추가
	if policyType == "SCHEDULED" then

		redis.call(
			'HSET',
			repeatKey,
			policyId,
			encoded
		)

	else

		redis.call(
			'ZADD',
			timeKey,
			tonumber(expireEpoch),
			policyId
		)

	end

end

return 1