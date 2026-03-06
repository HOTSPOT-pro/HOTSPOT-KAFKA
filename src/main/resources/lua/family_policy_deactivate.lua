-- KEYS
-- 1 idem key

-- ARGV
-- policyId
-- subId
-- subId
-- END
-- policyId
-- subId
-- subId
-- END

local ok = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 604800)

if not ok then
	return 0
end

local i = 1

while i <= #ARGV do

	local policyId = ARGV[i]
	i = i + 1

	while i <= #ARGV and ARGV[i] ~= "END" do

		local subId = ARGV[i]

		local repeatKey = 'block:repeat:' .. subId
		local timeKey = 'block:time:' .. subId

		redis.call('HDEL', repeatKey, policyId)
		redis.call('ZREM', timeKey, policyId)

		i = i + 1
	end

	i = i + 1
end

return 1