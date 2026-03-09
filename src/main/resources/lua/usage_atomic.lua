-- -------------------------
-- KEYS
-- -------------------------
--  1: limit:sub:{subId}
--  2: limit:family:{familyId}
--  3: limit:family_sub:{familyId}:{subId}
--  4: idx:gift:{subId}:{yyyymm}
--  5: usage:sub:{subId}:{yyyymm}
--  6: usage:sub:{subId}:{yyyymmdd}
--  7: usage:family:{familyId}:{yyyymm}
--  8: usage:family:{familyId}:{yyyymmdd}
--  9: usage:app:{subId}:{yyyymm}
-- 10: usage:app:{subId}:{yyyymmdd}
-- 11: usage:3hourly:{subId}:{yyyymmdd}
-- 12: notify:sub:{subId}:{yyyymm}
-- 13: notify:sub:{subId}:{yyyymmdd}
-- 14: notify:family:{familyId}:{yyyymm}
-- 15: dedup:evt:{eventId}

-- -------------------------
-- ARGV
-- -------------------------
--  1: bytes
--  2: appId
--  3: yyyymm
--  4: giftLimitPrefix
--  5: giftUsagePrefix
--  6: giftNotifyPrefix
--  7: ttlMon
--  8: ttlDay
--  9: ttlNotify
-- 10: ttlDedup
-- 11: eventId
-- 12: occurredAt (ISO8601)
-- 13: subId
-- 14: familyId
-- 15: day3HourlyField (00_used, 03_used ... 21_used)

local bytes = tonumber(ARGV[1])
local appId = ARGV[2]
local yyyymm = ARGV[3]

local giftLimitPrefix = ARGV[4]
local giftUsagePrefix = ARGV[5]
local giftNotifyPrefix = ARGV[6]

local ttlMon = tonumber(ARGV[7])
local ttlDay = tonumber(ARGV[8])
local ttlNotify = tonumber(ARGV[9])
local ttlDedup = tonumber(ARGV[10])

-- Outbox/알림 이벤트 생성에 사용할 이벤트 메타데이터를 수신한다.
local eventId = ARGV[11]
local occurredAt = ARGV[12]
local subId = tonumber(ARGV[13])
local familyId = tonumber(ARGV[14])
local day3HourlyField = ARGV[15]
local INF = 9007199254740991
local DAILY_PLAN_LIMIT_KB = 1048576

-- bytes가 비정상(nil/0/음수)이면 사용량 반영 없이 무시한다.
if bytes == nil or bytes <= 0 then
  return { "INVALID_BYTES" }
end

-- dedup 키가 있으면 동일 이벤트이므로 "사용량 반영 + 알림 판단"을 하지 않고 DUP로 종료한다.
if redis.call('EXISTS', KEYS[15]) == 1 then
  return { "DUP" }
end

-- 총량(quota) 대비 사용량(used)으로 임계치(50/30/10/0/101)와 잔여(rem), 잔여율(pct)을 계산한다.
local function threshold(quota, used)
  if quota == nil or quota <= 0 then
    return 101, 0, 0
  end

  local rem = quota - used
  if rem < 0 then rem = 0 end

  local pct = math.floor(rem * 100 / quota)

  if rem == 0 then return 0, rem, pct end
  if pct <= 10 then return 10, rem, pct end
  if pct <= 30 then return 30, rem, pct end
  if pct <= 50 then return 50, rem, pct end
  return 101, rem, pct
end

local function remaining_quota(limit, used)
  if limit == nil then return 0 end
  if limit < 0 then return INF end
  local rem = limit - used
  if rem < 0 then rem = 0 end
  return rem
end

-- 이전 임계치(last)보다 더 낮아질 때만 notify 상태를 갱신하고 발화 여부(fire)를 반환한다.
local function update_notify(key, new_th)
  if new_th == 101 then
    redis.call('EXPIRE', key, ttlNotify)
    return 0, 101
  end

  local last = tonumber(redis.call('GET', key) or '101')

  if new_th < last then
    redis.call('SET', key, tostring(new_th))
    redis.call('EXPIRE', key, ttlNotify)
    return 1, last
  end

  redis.call('EXPIRE', key, ttlNotify)
  return 0, last
end

-- 개인/가족/가족-개인 한도 정보를 Redis에서 조회한다.
local plan_limit = tonumber(redis.call('HGET', KEYS[1], 'plan_limit') or '0')
local family_limit_total = tonumber(redis.call('HGET', KEYS[2], 'family_limit') or '0')
local family_member_limit = tonumber(redis.call('HGET', KEYS[3], 'family_limit') or '0')

-- 월 누적 사용량(개인/가족/가족풀)을 Redis에서 조회한다.
local mon_plan_used = tonumber(redis.call('HGET', KEYS[5], 'plan_used') or '0')
local day_plan_used = tonumber(redis.call('HGET', KEYS[6], 'plan_used') or '0')
local mon_family_used = tonumber(redis.call('HGET', KEYS[5], 'member_family_used') or '0')
local mon_pool_used = tonumber(redis.call('HGET', KEYS[7], 'family_used') or '0')

local plan_used_base = mon_plan_used
if plan_limit == DAILY_PLAN_LIMIT_KB then
  plan_used_base = day_plan_used
end

-- 이번 이벤트 처리 시점의 잔여량(요금제/가족풀/가족-개인)을 계산한다.
local plan_rem = remaining_quota(plan_limit, plan_used_base)

local pool_rem = family_limit_total - mon_pool_used
if pool_rem < 0 then pool_rem = 0 end

local member_rem = pool_rem
if family_member_limit > 0 then
  member_rem = family_member_limit - mon_family_used
  if member_rem < 0 then member_rem = 0 end
end

-- 남은 처리 용량(remain)을 선물 → 요금제 → 가족풀 순서로 차감하기 위한 상태 변수를 초기화한다.
local remain = bytes
local gift_take_total = 0
local fired_gifts = {}

-- 해당 구독자의 월 선물 인덱스(ZSET)에서 선물 ID 목록을 가져온다.
local gift_ids = redis.call('ZRANGE', KEYS[4], 0, -1)

-- 선물 잔여가 있는 순서대로 remain을 차감하고, 선물 임계치 발화 정보를 수집한다.
for i = 1, #gift_ids do
  if remain <= 0 then break end

  local gid = gift_ids[i]
  local gift_limit_key = giftLimitPrefix .. gid .. ":" .. yyyymm
  local gift_usage_key = giftUsagePrefix .. gid .. ":" .. yyyymm

  local gift_quota = tonumber(redis.call('HGET', gift_limit_key, 'gift_limit') or '0')
  local gift_used = tonumber(redis.call('HGET', gift_usage_key, 'gift_used') or '0')

  local gift_rem = gift_quota - gift_used
  if gift_rem < 0 then gift_rem = 0 end

  if gift_rem > 0 then
    local take = remain
    if take > gift_rem then take = gift_rem end

    remain = remain - take
    gift_take_total = gift_take_total + take
    gift_used = gift_used + take

    redis.call('HINCRBY', gift_usage_key, 'gift_used', take)
    redis.call('EXPIRE', gift_usage_key, ttlMon)
    redis.call('EXPIRE', gift_limit_key, ttlMon)

    -- 선물이 완전 소진되면 인덱스(ZSET)에서 제거해 이후 조회/차감을 줄인다.
    if gift_quota > 0 and gift_used >= gift_quota then
      redis.call('ZREM', KEYS[4], gid)
    end

    local th, rem2, pct = threshold(gift_quota, gift_used)
    local nkey = giftNotifyPrefix .. gid .. ":" .. yyyymm
    local fire, last = update_notify(nkey, th)

    if fire == 1 then
      table.insert(fired_gifts, {
        giftId = gid, th = th, rem = rem2, pct = pct, last = last
      })
    end
  end
end

-- 선물로 다 차감하지 못한 remain을 개인 요금제 잔여에서 차감한다.
local plan_take = remain
if plan_take > plan_rem then plan_take = plan_rem end
remain = remain - plan_take

-- 가족풀에서 차감 가능한 상한을 (가족풀 잔여, 가족-개인 잔여) 중 더 작은 값으로 제한한다.
local fam_cap = pool_rem
if member_rem < fam_cap then fam_cap = member_rem end

-- remain을 가족풀에서 차감하고, 남는 값은 overflow로 처리한다.
local family_take = remain
if family_take > fam_cap then family_take = fam_cap end
remain = remain - family_take

local overflow = remain

-- 월 단위 개인 사용량(usage:sub:{subId}:{yyyymm})을 누적 갱신한다.
redis.call('HINCRBY', KEYS[5], 'total_used', bytes)
redis.call('HINCRBY', KEYS[5], 'plan_used', plan_take)
redis.call('HINCRBY', KEYS[5], 'member_family_used', family_take)
redis.call('HINCRBY', KEYS[5], 'gift_used', gift_take_total)
if overflow > 0 then
  redis.call('HINCRBY', KEYS[5], 'overflow_used', overflow)
end
redis.call('EXPIRE', KEYS[5], ttlMon)

-- 일 단위 개인 사용량(usage:sub:{subId}:{yyyymmdd})도 동일하게 누적 갱신한다.
redis.call('HINCRBY', KEYS[6], 'total_used', bytes)
redis.call('HINCRBY', KEYS[6], 'plan_used', plan_take)
redis.call('HINCRBY', KEYS[6], 'member_family_used', family_take)
redis.call('HINCRBY', KEYS[6], 'gift_used', gift_take_total)
if overflow > 0 then
  redis.call('HINCRBY', KEYS[6], 'overflow_used', overflow)
end
redis.call('EXPIRE', KEYS[6], ttlDay)

-- 월/일 단위 가족풀 사용량(usage:family:*)을 누적 갱신한다.
redis.call('HINCRBY', KEYS[7], 'family_used', family_take)
redis.call('HINCRBY', KEYS[8], 'family_used', family_take)
redis.call('EXPIRE', KEYS[7], ttlMon)
redis.call('EXPIRE', KEYS[8], ttlDay)

-- 월/일 단위 앱별 사용량(usage:app:*)을 ZSET으로 누적 갱신한다.
redis.call('ZINCRBY', KEYS[9], bytes, appId)
redis.call('ZINCRBY', KEYS[10], bytes, appId)
redis.call('EXPIRE', KEYS[9], ttlMon)
redis.call('EXPIRE', KEYS[10], ttlDay)

-- 일 단위 3시간 버킷 사용량(usage:3hourly:{subId}:{yyyymmdd})을 HASH로 누적 갱신한다.
redis.call('HINCRBY', KEYS[11], day3HourlyField, bytes)
redis.call('EXPIRE', KEYS[11], ttlDay)

-- 선물 인덱스(ZSET)가 월 기간 동안 유지되도록 TTL을 갱신한다.
redis.call('EXPIRE', KEYS[4], ttlMon)

-- 요금제 임계치 변화를 계산하고 월/일 notify 키를 갱신해 발화 여부를 결정한다.
local plan_used_new = plan_used_base + plan_take
local plan_th, plan_rem2, plan_pct = threshold(plan_limit, plan_used_new)
local plan_fire, plan_last = update_notify(KEYS[12], plan_th)
update_notify(KEYS[13], plan_th)

-- 가족풀 임계치 변화를 계산하고 notify 키를 갱신해 발화 여부를 결정한다.
local pool_used_new = mon_pool_used + family_take
local fam_th, fam_rem2, fam_pct = threshold(family_limit_total, pool_used_new)
local fam_fire, fam_last = update_notify(KEYS[14], fam_th)

-- 이번 이벤트 처리 완료를 dedup 키로 기록해 중복 처리를 방지한다.
redis.call('SET', KEYS[15], '1', 'EX', ttlDedup)

-- 최종 결과(차감 분배, 임계치 발화 여부/상태, 발화된 선물 목록)를 배열로 반환한다.
return {
  "OK",
  bytes,
  gift_take_total, plan_take, family_take, overflow,

  pool_used_new, (family_limit_total - pool_used_new),

  (mon_family_used + family_take), family_member_limit,

  plan_fire, plan_th, plan_rem2, plan_pct, plan_last,

  fam_fire, fam_th, fam_rem2, fam_pct, fam_last,

  cjson.encode(fired_gifts)
}
