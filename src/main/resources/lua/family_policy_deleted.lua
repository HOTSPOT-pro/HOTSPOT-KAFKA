-- KEYS
-- 1 idem key

-- ARGV
-- 1 policyId
-- 2.. subIds

local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 604800)

if not ok then
	return 0
end

local policyId = ARGV[1]

for i = 2, #ARGV do

	local subId = ARGV[i]

	local repeatKey = 'block:repeat:' .. subId
	local timeKey = 'block:time:' .. subId

	redis.call('HDEL', repeatKey, policyId)
	redis.call('ZREM', timeKey, policyId)

end

return 1