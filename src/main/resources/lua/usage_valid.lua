-- ARGV 구조
-- ARGV[1] = now (epoch seconds)
-- ARGV[2] = appId
-- ARGV[3] = dayOfWeek
-- ARGV[4] = currentTime (HH:mm)

local now = tonumber(ARGV[1])
local appId = ARGV[2]
local dayOfWeek = ARGV[3]
local currentTime = ARGV[4]


-- 정책 체크 함수들

local function is_immediate_blocked(subId)
	return redis.call("EXISTS", "block:immediate:" .. subId) == 1
end

local function is_time_blocked(subId)
	local key = "block:time:" .. subId
	redis.call("ZREMRANGEBYSCORE", key, "-inf", now)
	return redis.call("ZCARD", key) > 0
end

local function is_app_blocked(subId)
	return redis.call("SISMEMBER", "block:app:" .. subId, appId) == 1
end

local function is_repeat_blocked(subId)
	local key = "block:repeat:" .. subId
	local policies = redis.call("HVALS", key)

	for _, value in ipairs(policies) do
		local days, startTime, endTime =
			string.match(value, "([^|]+)|([^|]+)|([^|]+)")

		if days ~= nil then
			for day in string.gmatch(days, "([^,]+)") do
				if day == dayOfWeek then
					if currentTime >= startTime and currentTime <= endTime then
						return true
					end
				end
			end
		end
	end

	return false
end

local function is_policy_blocked(subId)
	return is_immediate_blocked(subId)
	or is_time_blocked(subId)
	or is_app_blocked(subId)
	or is_repeat_blocked(subId)
end

-- =========================================
-- 메인 루프
-- =========================================

local result = {}

for i = 6, #ARGV do

	local subId = ARGV[i]

	if not is_policy_blocked(subId) then
	end
end

return result