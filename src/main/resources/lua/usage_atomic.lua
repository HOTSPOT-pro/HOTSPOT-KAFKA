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
-- 11: notify:sub:{subId}:{yyyymm}
-- 12: notify:sub:{subId}:{yyyymmdd}
-- 13: notify:family:{familyId}:{yyyymm}
-- 14: dedup:evt:{eventId}
-- 15: outbox:usage-alerts:v1

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
-- 15: outboxMaxLenApprox

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

-- outbox 엔트리 생성을 위한 이벤트 메타
local eventId = ARGV[11]
local occurredAt = ARGV[12]
local subId = tonumber(ARGV[13])
local familyId = tonumber(ARGV[14])
local outboxMaxLenApprox = tonumber(ARGV[15])

-- dedup 키가 있으면 이미 "사용량 반영 + outbox 적재" 완료된 이벤트
if redis.call('EXISTS', KEYS[14]) == 1 then
  return { "DUP" }
end

local now = redis.call('TIME')
local createdAt = now[1]

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

-- 알림 1건을 outbox stream으로 적재
local function add_outbox(alertType, targetKey, kafkaKey, thresholdValue, remainingBytes, remainingPct, giftId)
  local alertId = eventId .. ":" .. alertType .. ":" .. targetKey

  local payload = {
    alertId = alertId,
    eventType = "USAGE_THRESHOLD",
    alertType = alertType,
    threshold = tostring(thresholdValue),
    occurredAt = occurredAt,
    subId = nil,
    familyId = nil,
    giftId = nil,
    remainingBytes = remainingBytes,
    remainingPct = remainingPct,
    sourceEventId = eventId
  }

  if alertType == "PLAN_REMAINING" then
    payload.subId = subId
  elseif alertType == "FAMILY_POOL_REMAINING" then
    payload.familyId = familyId
  elseif alertType == "GIFT_REMAINING" then
    payload.subId = subId
    payload.giftId = giftId
  end

  redis.call(
    'XADD',
    KEYS[15],
    'MAXLEN',
    '~',
    outboxMaxLenApprox,
    '*',
    'alert_id', alertId,
    'event_id', eventId,
    'occurred_time', occurredAt,
    'alert_type', alertType,
    'target_key', targetKey,
    'kafka_key', kafkaKey,
    'payload', cjson.encode(payload),
    'created_time', createdAt
  )
end

local plan_limit = tonumber(redis.call('HGET', KEYS[1], 'plan_limit') or '0')
local family_limit_total = tonumber(redis.call('HGET', KEYS[2], 'family_limit') or '0')
local family_member_limit = tonumber(redis.call('HGET', KEYS[3], 'family_limit') or '0')

local mon_plan_used = tonumber(redis.call('HGET', KEYS[5], 'plan_used') or '0')
local mon_family_used = tonumber(redis.call('HGET', KEYS[5], 'member_family_used') or '0')
local mon_pool_used = tonumber(redis.call('HGET', KEYS[7], 'family_used') or '0')

local plan_rem = plan_limit - mon_plan_used
if plan_rem < 0 then plan_rem = 0 end

local pool_rem = family_limit_total - mon_pool_used
if pool_rem < 0 then pool_rem = 0 end

local member_rem = pool_rem
if family_member_limit > 0 then
  member_rem = family_member_limit - mon_family_used
  if member_rem < 0 then member_rem = 0 end
end

local remain = bytes
local gift_take_total = 0
local fired_gifts = {}

local gift_ids = redis.call('ZRANGE', KEYS[4], 0, -1)

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

local plan_take = remain
if plan_take > plan_rem then plan_take = plan_rem end
remain = remain - plan_take

local fam_cap = pool_rem
if member_rem < fam_cap then fam_cap = member_rem end

local family_take = remain
if family_take > fam_cap then family_take = fam_cap end
remain = remain - family_take

local overflow = remain

redis.call('HINCRBY', KEYS[5], 'total_used', bytes)
redis.call('HINCRBY', KEYS[5], 'plan_used', plan_take)
redis.call('HINCRBY', KEYS[5], 'member_family_used', family_take)
redis.call('HINCRBY', KEYS[5], 'gift_used', gift_take_total)
if overflow > 0 then
  redis.call('HINCRBY', KEYS[5], 'overflow_used', overflow)
end
redis.call('EXPIRE', KEYS[5], ttlMon)

redis.call('HINCRBY', KEYS[6], 'total_used', bytes)
redis.call('HINCRBY', KEYS[6], 'plan_used', plan_take)
redis.call('HINCRBY', KEYS[6], 'member_family_used', family_take)
redis.call('HINCRBY', KEYS[6], 'gift_used', gift_take_total)
if overflow > 0 then
  redis.call('HINCRBY', KEYS[6], 'overflow_used', overflow)
end
redis.call('EXPIRE', KEYS[6], ttlDay)

redis.call('HINCRBY', KEYS[7], 'family_used', family_take)
redis.call('HINCRBY', KEYS[8], 'family_used', family_take)
redis.call('EXPIRE', KEYS[7], ttlMon)
redis.call('EXPIRE', KEYS[8], ttlDay)

redis.call('ZINCRBY', KEYS[9], bytes, appId)
redis.call('ZINCRBY', KEYS[10], bytes, appId)
redis.call('EXPIRE', KEYS[9], ttlMon)
redis.call('EXPIRE', KEYS[10], ttlDay)

redis.call('EXPIRE', KEYS[4], ttlMon)

local plan_used_new = mon_plan_used + plan_take
local plan_th, plan_rem2, plan_pct = threshold(plan_limit, plan_used_new)
local plan_fire, plan_last = update_notify(KEYS[11], plan_th)
update_notify(KEYS[12], plan_th)

local pool_used_new = mon_pool_used + family_take
local fam_th, fam_rem2, fam_pct = threshold(family_limit_total, pool_used_new)
local fam_fire, fam_last = update_notify(KEYS[13], fam_th)

if plan_fire == 1 then
  add_outbox(
    "PLAN_REMAINING",
    "sub:" .. tostring(subId),
    "sub:" .. tostring(subId),
    plan_th,
    plan_rem2,
    plan_pct,
    nil
  )
end

if fam_fire == 1 then
  add_outbox(
    "FAMILY_POOL_REMAINING",
    "family:" .. tostring(familyId),
    "family:" .. tostring(familyId),
    fam_th,
    fam_rem2,
    fam_pct,
    nil
  )
end

for i = 1, #fired_gifts do
  local gf = fired_gifts[i]
  add_outbox(
    "GIFT_REMAINING",
    "gift:" .. gf.giftId,
    "gift:" .. gf.giftId,
    gf.th,
    gf.rem,
    gf.pct,
    gf.giftId
  )
end

-- outbox 적재까지 모두 끝난 뒤 dedup 마킹
redis.call('SET', KEYS[14], '1', 'EX', ttlDedup)

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
