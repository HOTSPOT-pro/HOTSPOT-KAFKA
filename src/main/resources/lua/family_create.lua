-- KEYS
-- 1) idemKey                 idem:family:{eventId}
-- 2) familyLimitKey          limit:family:{familyId}
-- 3) subFamilyKey            idx:sub:family
-- 4) familySubsKey           idx:family:subs:{familyId}

-- ARGV
-- 1) familyId
-- 2) memberCount
-- 3..n) subIds

local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

local familyId = ARGV[1]
local memberCount = tonumber(ARGV[2])

local LIMIT_PER_MEMBER = 5 * 1024 * 1024
local familyLimit = memberCount * LIMIT_PER_MEMBER

-- family limit 설정
redis.call('HSET', KEYS[2], 'family_limit', familyLimit)

for i = 3, #ARGV do

	local subId = ARGV[i]

	-- 가족 구성원 SET
	redis.call('SADD', KEYS[4], subId)

	-- sub -> family mapping
	redis.call('HSET', KEYS[3], subId, familyId)

	-- 구성원별 limit
	local subLimitKey = "limit:family_sub:" .. familyId .. ":" .. subId
	redis.call('HSET', subLimitKey, 'family_limit', familyLimit)

end

return 1