-- KEYS:
-- 1) idemKey            idem:sub:{eventId}
-- 2) appBlockKey        block:app:{subId}
--
-- ARGV:
-- 1...N) appIds

-- 0️⃣ 멱등 처리
local ok = redis.call('SET', KEYS[1], '1', 'NX')
if not ok then
	return 0
end

-- 1️⃣ 기존 차단 목록 전부 삭제
redis.call('DEL', KEYS[2])

-- 2️⃣ 새로운 리스트로 재구성
for i = 1, #ARGV do
	redis.call('SADD', KEYS[2], ARGV[i])
end

return 1