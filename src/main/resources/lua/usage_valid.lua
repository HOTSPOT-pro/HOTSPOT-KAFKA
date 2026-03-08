-- ===================================================
-- ARGV 구조
-- ===================================================
-- 1  now (epoch seconds)
-- 2  dayOfWeek
-- 3  currentTime (HH:mm)
-- 4  yyyyMM
-- 5  yyyyMMdd

-- 이후 5개씩 반복
-- eventId
-- subId
-- familyId
-- usageKb
-- appId
-- ===================================================

local now = tonumber(ARGV[1])
local dayOfWeek = ARGV[2]
local currentTime = ARGV[3]
local yyyyMM = ARGV[4]
local yyyyMMdd = ARGV[5]
local INF = 9007199254740991
local DAILY_PLAN_LIMIT_KB = 1048576

local result = {}

local giftRemainingCache = {}
local familyPriorityCache = {}
local memberRemainingCacheByFamily = {}
local familyRemainingCache = {}

-- ===================================================
-- 선물 캐시 함수
-- ===================================================

local function get_gift_remaining_map(subId)

	local key = tostring(subId)

	if giftRemainingCache[key] ~= nil then
		return giftRemainingCache[key]
	end

	local idxKey = "idx:gift:" .. key .. ":" .. yyyyMM
	local giftIds = redis.call("ZRANGE", idxKey, 0, -1)

	local remainList = {}

	for _, giftId in ipairs(giftIds) do
		local limitKey = "limit:gift:" .. key .. ":" .. giftId .. ":" .. yyyyMM
		local usageKey = "usage:gift:" .. key .. ":" .. giftId .. ":" .. yyyyMM

		local limit = tonumber(redis.call("HGET", limitKey, "gift_limit") or "0")
		local used  = tonumber(redis.call("HGET", usageKey, "gift_used") or "0")

		table.insert(remainList, {
			giftId = giftId,
			remain = math.max(0, limit - used)
		})
	end

	giftRemainingCache[key] = remainList
	return remainList
end

local function consume_gift_if_possible(subId, usageKb)

	local remainList = get_gift_remaining_map(subId)
	local need = usageKb

	for _, row in ipairs(remainList) do

		if row.remain > 0 then

			if row.remain >= need then
			-- 실제로 메모리 차감
				row.remain = row.remain - need
				return true
			else
				need = need - row.remain
				row.remain = 0
			end

		end
	end

	return false
end

-- ===================================================
-- 정책 검사
-- ===================================================

local function is_immediate_blocked(subId)
	return redis.call("EXISTS", "block:immediate:" .. subId) == 1
end

local function is_time_blocked(subId)
	local key = "block:time:" .. subId
	redis.call("ZREMRANGEBYSCORE", key, "-inf", now)
	return redis.call("ZCARD", key) > 0
end

local function is_app_blocked(subId, appId)
	return redis.call("SISMEMBER", "block:app:" .. subId, tostring(appId)) == 1
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
					if startTime <= endTime then
					-- 일반 구간 (09:00 ~ 14:00)
						if currentTime >= startTime and currentTime <= endTime then
							return true
						end
					else
					-- 자정 넘어가는 구간 (23:00 ~ 07:00)
						if currentTime >= startTime or currentTime <= endTime then
							return true
						end
					end
				end
			end
		end
	end

	return false
end

local function is_policy_blocked(subId, appId)
	return is_immediate_blocked(subId)
	or is_time_blocked(subId)
	or is_app_blocked(subId, appId)
	or is_repeat_blocked(subId)
end

-- ===================================================
-- 개인 체크
-- ===================================================

local personalRemainingCache = {}

local function consume_personal_if_possible(subId, usageKb)
	local key = tostring(subId)

	if personalRemainingCache[key] == nil then
		local limit = tonumber(redis.call("HGET", "limit:sub:" .. key, "plan_limit") or "0")
		local usagePeriodKey = yyyyMM
		if limit == DAILY_PLAN_LIMIT_KB then
			usagePeriodKey = yyyyMMdd
		end
		local used  = tonumber(redis.call("HGET", "usage:sub:" .. key .. ":" .. usagePeriodKey, "plan_used") or "0")
		if limit < 0 then
			personalRemainingCache[key] = INF
		else
			personalRemainingCache[key] = math.max(0, limit - used)
		end
	end

	if personalRemainingCache[key] >= usageKb then
		personalRemainingCache[key] =
			personalRemainingCache[key] - usageKb
		return true
	end

	return false
end

local familyPending = {}

-- ===================================================
-- 1차 루프 : 정책 → 선물 → 개인
-- ===================================================

for i = 6, #ARGV, 5 do

	local eventId  = ARGV[i]
	local subId    = ARGV[i+1]
	local familyId = ARGV[i+2]
	local usageKb  = tonumber(ARGV[i+3])
	local appId    = ARGV[i+4]

	if not is_policy_blocked(subId, appId) then

		-- 선물
		if consume_gift_if_possible(subId, usageKb) then
			table.insert(result, eventId)

		-- 개인
		elseif consume_personal_if_possible(subId, usageKb) then
			table.insert(result, eventId)

		-- 가족 후보
		else
			if familyId ~= nil and familyId ~= "0" then
				if familyPending[familyId] == nil then
					familyPending[familyId] = {
						order = {},
						bySub = {}
					}
				end

				local eventObj = {
					eventId = eventId,
					subId = subId,
					usageKb = usageKb
				}

				local subKey = tostring(subId)

				if familyPending[familyId].bySub[subKey] == nil then
					familyPending[familyId].bySub[subKey] = {}
				end

				table.insert(familyPending[familyId].bySub[subKey], eventObj)
				table.insert(familyPending[familyId].order, eventObj)

			end
		end
	end
end

-- ===================================================
-- 2차 루프 : 가족 공용 처리
-- ===================================================

local function get_family_priorities(familyId)
	local key = tostring(familyId)
	if familyPriorityCache[key] == nil then
		local priorityKey = "priority:family:" .. key
		familyPriorityCache[key] = redis.call("ZRANGE", priorityKey, 0, -1)
	end
	return familyPriorityCache[key]
end

for familyId, familyData in pairs(familyPending) do

	local famKey = tostring(familyId)

	-- familyRemaining 캐시
	if familyRemainingCache[famKey] == nil then
		local limit = tonumber(redis.call("HGET", "limit:family:" .. famKey, "family_limit") or "0")
		local used  = tonumber(redis.call("HGET", "usage:family:" .. famKey .. ":" .. yyyyMM, "family_used") or "0")
		familyRemainingCache[famKey] = math.max(0, limit - used)
	end

	local remaining = familyRemainingCache[famKey]

	if remaining > 0 then

		local priorities = get_family_priorities(familyId)

		-- =========================================
		-- 우선순위 모드
		-- =========================================
		if #priorities > 0 then

			if memberRemainingCacheByFamily[famKey] == nil then
				memberRemainingCacheByFamily[famKey] = {}
			end

			local famMemberCache = memberRemainingCacheByFamily[famKey]

			for _, memberSubId in ipairs(priorities) do

				local subKey = tostring(memberSubId)

				if famMemberCache[subKey] == nil then
					local memberLimit = tonumber(redis.call(
						"HGET",
						"limit:family_sub:" .. famKey .. ":" .. subKey,
						"family_limit"
					) or "0")

					local memberUsed = tonumber(redis.call(
						"HGET",
						"usage:sub:" .. subKey .. ":" .. yyyyMM,
						"member_family_used"
					) or "0")

					famMemberCache[subKey] =
						math.max(0, memberLimit - memberUsed)
				end

				local memberRemaining = famMemberCache[subKey]
				local eventList = familyData.bySub[subKey]

				if eventList ~= nil then
					for _, event in ipairs(eventList) do
						if remaining >= event.usageKb and memberRemaining >= event.usageKb then
							remaining = remaining - event.usageKb
							memberRemaining = memberRemaining - event.usageKb
							table.insert(result, event.eventId)
						end
					end
				end

				famMemberCache[subKey] = memberRemaining
			end

		-- =========================================
		-- ⏱ 선착순 모드 (음수 방지 적용)
		-- =========================================
		else

			local memberRemainingCache = {}

			for _, event in ipairs(familyData.order) do

				local subKey = tostring(event.subId)

				if memberRemainingCache[subKey] == nil then
					local memberLimit = tonumber(redis.call(
						"HGET",
						"limit:family_sub:" .. famKey .. ":" .. subKey,
						"family_limit"
					) or "0")

					local memberUsed = tonumber(redis.call(
						"HGET",
						"usage:sub:" .. subKey .. ":" .. yyyyMM,
						"member_family_used"
					) or "0")

					memberRemainingCache[subKey] =
						math.max(0, memberLimit - memberUsed)
				end

				local memberRemaining = memberRemainingCache[subKey]

				if remaining >= event.usageKb and memberRemaining >= event.usageKb then

					remaining = remaining - event.usageKb
					memberRemaining = memberRemaining - event.usageKb

					memberRemainingCache[subKey] = memberRemaining
					table.insert(result, event.eventId)
				end
			end
		end

		-- familyRemaining 캐시 업데이트
		familyRemainingCache[famKey] = remaining
	end
end

return result
