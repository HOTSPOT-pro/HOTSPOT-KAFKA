-- KEYS:
-- 1) idemKey
-- 2) priorityKey            priority:family:{familyId}
--
-- ARGV:
-- 1) mode                   PRIORITY | FIFO
-- 2) memberCount
-- 3...) subId, priority 반복

local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

local mode = ARGV[1]

if mode == 'FIFO' then
	redis.call('DEL', KEYS[2])
	return 1
end

-- PRIORITY 모드일 경우
-- 기존 우선순위 전부 삭제
redis.call('DEL', KEYS[2])

local count = tonumber(ARGV[2])
local index = 3

for i = 1, count do
	local subId = ARGV[index]
	local priority = tonumber(ARGV[index + 1])
	redis.call('ZADD', KEYS[2], priority, subId)
	index = index + 2
end

return 1