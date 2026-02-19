-- KEYS:
-- 1) idemKey         idem:sub:{eventId}
-- 2) planLimitKey    limit:sub:{subId}
--
-- ARGV:
-- 1) planLimit

local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

redis.call('HSET', KEYS[2], 'plan_limit', tonumber(ARGV[1]))
return 1