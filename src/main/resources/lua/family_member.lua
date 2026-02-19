-- KEYS:
-- 1) idemKey                        idem:family:{eventId}
-- 2) familyLimitKey                 limit:family:{familyId}
-- 3) subFamilyKey                   idx:sub:family
-- 4) familySubsKey                  idx:family:subs:{familyId}
-- 5) familySubLimitKey              limit:family_sub:{familyId}:{subId}
-- 6) priorityKey                    priority:family:{familyId}

-- ARGV:
-- 1) subId
-- 2) familyId
-- 3) eventType                      ADD | REMOVE

local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

local subId = ARGV[1]
local familyId = ARGV[2]
local eventType = ARGV[3]

local LIMIT_PER_MEMBER = 5 * 1024 * 1024

if eventType == 'ADD' then

	local added = redis.call('SADD', KEYS[4], subId)

	if added == 1 then

	-- sub ↔ family 매핑
		redis.call('HSET', KEYS[3], subId, familyId)

		-- family_limit 증가
		redis.call('HINCRBY', KEYS[2], 'family_limit', LIMIT_PER_MEMBER)

		-- 증가된 family_limit 조회
		local newLimit = redis.call('HGET', KEYS[2], 'family_limit')

		-- 개인별 가족 한도 저장
		redis.call('HSET', KEYS[5], 'family_limit', newLimit)

		-- PRIORITY 모드일 경우 자동 우선순위 추가
		if redis.call('EXISTS', KEYS[6]) == 1 then
			local maxScore = redis.call('ZREVRANGE', KEYS[6], 0, 0, 'WITHSCORES')
			local nextScore = 0

			if #maxScore > 0 then
				nextScore = tonumber(maxScore[2]) + 1
			end

			redis.call('ZADD', KEYS[6], nextScore, subId)
		end
	end

else

	local removed = redis.call('SREM', KEYS[4], subId)

	if removed == 1 then

		redis.call('HDEL', KEYS[3], subId)

		local current = tonumber(redis.call('HGET', KEYS[2], 'family_limit') or '0')
		local newVal = current - LIMIT_PER_MEMBER
		if newVal < 0 then newVal = 0 end

		redis.call('HSET', KEYS[2], 'family_limit', newVal)

		redis.call('DEL', KEYS[5])

		-- 🔥 PRIORITY 재정렬 포함
		if redis.call('EXISTS', KEYS[6]) == 1 then

			local oldScore = redis.call('ZSCORE', KEYS[6], subId)

			if oldScore then

				redis.call('ZREM', KEYS[6], subId)

				local members = redis.call('ZRANGEBYSCORE', KEYS[6], '(' .. oldScore, '+inf', 'WITHSCORES')

				for i = 1, #members, 2 do
					local member = members[i]
					local score = tonumber(members[i+1])
					redis.call('ZADD', KEYS[6], score - 1, member)
				end
			end
		end
	end
end

return 1