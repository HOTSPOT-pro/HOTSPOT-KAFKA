-- KEYS:
-- 1) idemKey               idem:sub:{eventId}
-- 2) immediateBlockKey     block:immediate:{subId}
--
-- ARGV:
-- 1) action                LOCK | UNLOCK

local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

local action = ARGV[1]

if action == 'LOCK' then
	redis.call('SET', KEYS[2], '1')
else
	redis.call('DEL', KEYS[2])
end

return 1