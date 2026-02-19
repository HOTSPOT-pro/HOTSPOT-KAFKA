-- KEYS:
-- 1) idemKey            idem:sub:{eventId}
-- 2) appBlockKey        block:app:{subId}
--
-- ARGV:
-- 1) action             APPLY | REMOVE
-- 2) appId

local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

local action = ARGV[1]
local appId = ARGV[2]

if action == 'APPLY' then
	redis.call('SADD', KEYS[2], appId)
else
	redis.call('SREM', KEYS[2], appId)
end

return 1