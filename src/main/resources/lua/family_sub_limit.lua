-- KEYS:
-- 1) idemKey
-- 2) familySubLimitKey

-- ARGV:
-- 1) newLimit

local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

redis.call('HSET', KEYS[2], 'family_limit', tonumber(ARGV[1]))

return 1