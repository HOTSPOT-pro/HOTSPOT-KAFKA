-- -------------------------
-- KEYS (고정 인자)
-- -------------------------
--  1: limit:sub:{subId}                    (HASH) plan_limit
--  2: limit:family:{familyId}              (HASH) family_limit
--  3: limit:family_sub:{familyId}:{subId}  (HASH) family_member_limit
--  4: idx:gift:{subId}:{yyyymm}            (ZSET) giftId 목록
--  5: usage:sub:{subId}:{yyyymm}           (HASH) 월 사용량
--  6: usage:sub:{subId}:{yyyymmdd}         (HASH) 일 사용량
--  7: usage:family:{familyId}:{yyyymm}     (HASH) 가족 풀 월 사용량
--  8: usage:family:{familyId}:{yyyymmdd}   (HASH) 가족 풀 일 사용량
--  9: usage:app:{subId}:{yyyymm}           (ZSET) 월 앱별 사용량
-- 10: usage:app:{subId}:{yyyymmdd}         (ZSET) 일 앱별 사용량
-- 11: notify:sub:{subId}:{yyyymm}          (STRING) 요금제 월 임계치 상태
-- 12: notify:sub:{subId}:{yyyymmdd}        (STRING) 요금제 일 임계치 상태
-- 13: notify:family:{familyId}:{yyyymm}    (STRING) 가족 풀 월 임계치 상태
-- 14: dedup:evt:{eventId}                  (STRING) 중복 방지 키

-- -------------------------
-- ARGV (가변 인자)
-- -------------------------
--  1: bytes (이번 이벤트 사용량)
--  2: appId
--  3: yyyymm
--  4: giftLimitPrefix   ("limit:gift:")
--  5: giftUsagePrefix   ("usage:gift:")
--  6: giftNotifyPrefix  ("notify:gift:")
--  7: ttlMon
--  8: ttlDay
--  9: ttlNotify
-- 10: ttlDedup

local bytes = tonumber(ARGV[1])
local appId = ARGV[2]
local yyyymm = ARGV[3]

-- gift 관련 실제 키는 prefix + gid + ":" + yyyymm 로 조립
local giftLimitPrefix = ARGV[4]
local giftUsagePrefix = ARGV[5]
local giftNotifyPrefix = ARGV[6]

-- TTL (seconds)
local ttlMon = tonumber(ARGV[7])
local ttlDay = tonumber(ARGV[8])
local ttlNotify = tonumber(ARGV[9])
local ttlDedup = tonumber(ARGV[10])

-- =========================================================
-- 1) Dedup: eventId가 이미 처리되었으면 DUP 반환
-- =========================================================
local ok = redis.call('SET', KEYS[14], '1', 'NX', 'EX', ttlDedup)
if not ok then
  return { "DUP" }
end

-- =========================================================
-- 2) Helper: threshold(quota, used)
--    - quota 기준으로 잔여% 구간을 50/30/10/0으로 환산
--    - 101은 알림 대상 아님 의미(초기/정상 구간)
-- =========================================================
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

-- =========================================================
-- 3) Helper: update_notify(key, new_th)
--    - 마지막 임계치(last)보다 더 낮아질 때만 fire=1
--    - new_th==101이면 상태값 변경은 안 하고 TTL만 연장
-- =========================================================
local function update_notify(key, new_th)
  if new_th == 101 then
    -- 정상 구간: 값은 굳이 쓰지 않고 TTL만 유지
    redis.call('EXPIRE', key, ttlNotify)
    return 0, 101
  end

  local last = tonumber(redis.call('GET', key) or '101')

  if new_th < last then
    -- 임계치가 하락할 때만 알림 발화 대상
    redis.call('SET', key, tostring(new_th))
    redis.call('EXPIRE', key, ttlNotify)
    return 1, last
  end

  -- 이미 더 낮은(또는 같은) 구간에 있으므로 fire 없음, TTL만 갱신
  redis.call('EXPIRE', key, ttlNotify)
  return 0, last
end

-- =========================================================
-- 4) Limit/Usage 로드 (이번 이벤트 처리 전 기준)
-- =========================================================
local plan_limit = tonumber(redis.call('HGET', KEYS[1], 'plan_limit') or '0')
local family_limit_total = tonumber(redis.call('HGET', KEYS[2], 'family_limit') or '0')
local family_member_limit = tonumber(redis.call('HGET', KEYS[3], 'family_limit') or '0')

local mon_plan_used = tonumber(redis.call('HGET', KEYS[5], 'plan_used') or '0')
local mon_family_used = tonumber(redis.call('HGET', KEYS[5], 'member_family_used') or '0')
local mon_pool_used = tonumber(redis.call('HGET', KEYS[7], 'family_used') or '0')

-- 잔여량 계산
local plan_rem = plan_limit - mon_plan_used
if plan_rem < 0 then plan_rem = 0 end

local pool_rem = family_limit_total - mon_pool_used
if pool_rem < 0 then pool_rem = 0 end

-- 가족에서 이 멤버가 더 쓸 수 있는 잔여량
-- (개인한도 0이면 무제한 취급 -> 일단 pool_rem을 기준으로 두고,
--  최종 cap에서 min(pool_rem, member_rem) 적용)
local member_rem = pool_rem
if family_member_limit > 0 then
  member_rem = family_member_limit - mon_family_used
  if member_rem < 0 then member_rem = 0 end
end

-- =========================================================
-- 5) bytes를 어디서 충당할지 계산: Gift -> Plan -> Family -> Overflow
-- =========================================================
local remain = bytes              -- 아직 충당 못한 사용량
local gift_take_total = 0         -- 이번 이벤트에서 gift로 충당한 총량
local fired_gifts = {}            -- gift별 알림 fire 결과 모음

-- -------------------------
-- 5-1) Gift 차감 (idx:gift ZSET 순서대로)
-- -------------------------
local gift_ids = redis.call('ZRANGE', KEYS[4], 0, -1)

for i = 1, #gift_ids do
  if remain <= 0 then break end   -- 이미 전량 충당했으면 종료

  local gid = gift_ids[i]

  -- gift limit/usage 키 조립: prefix + gid + ":" + yyyymm
  local gift_limit_key = giftLimitPrefix .. gid .. ":" .. yyyymm
  local gift_usage_key = giftUsagePrefix .. gid .. ":" .. yyyymm

  local gift_quota = tonumber(redis.call('HGET', gift_limit_key, 'gift_limit') or '0')
  local gift_used = tonumber(redis.call('HGET', gift_usage_key, 'gift_used') or '0')

  local gift_rem = gift_quota - gift_used
  if gift_rem < 0 then gift_rem = 0 end

  if gift_rem > 0 then
    -- 이번 이벤트 remain 중 gift가 커버 가능한 만큼 take
    local take = remain
    if take > gift_rem then take = gift_rem end

    remain = remain - take
    gift_take_total = gift_take_total + take
    gift_used = gift_used + take

    -- gift 사용량 누적 + TTL 유지
    redis.call('HINCRBY', gift_usage_key, 'gift_used', take)
    redis.call('EXPIRE', gift_usage_key, ttlMon)
    redis.call('EXPIRE', gift_limit_key, ttlMon)

    -- gift를 다 썼으면 인덱스에서 제거해서 다음부터 스캔 비용 감소
    if gift_quota > 0 and gift_used >= gift_quota then
      redis.call('ZREM', KEYS[4], gid)
    end

    -- gift 임계치 알림 판정
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

-- -------------------------
-- 5-2) Plan 차감 (개인 요금제 잔여까지)
-- -------------------------
local plan_take = remain
if plan_take > plan_rem then plan_take = plan_rem end
remain = remain - plan_take

-- -------------------------
-- 5-3) Family 차감 (pool 잔여 + 개인 한도 잔여 동시 반영)
-- -------------------------
local fam_cap = pool_rem
if member_rem < fam_cap then fam_cap = member_rem end

local family_take = remain
if family_take > fam_cap then family_take = fam_cap end
remain = remain - family_take

-- -------------------------
-- 5-4) Overflow (어디에도 못 담긴 초과분)
-- -------------------------
local overflow = remain

-- =========================================================
-- 6) 집계 키 업데이트 (월/일/가족/앱별)
-- =========================================================

-- (A) sub 월 usage hash
redis.call('HINCRBY', KEYS[5], 'total_used', bytes)
redis.call('HINCRBY', KEYS[5], 'plan_used', plan_take)
redis.call('HINCRBY', KEYS[5], 'member_family_used', family_take)
redis.call('HINCRBY', KEYS[5], 'gift_used', gift_take_total)
if overflow > 0 then
  redis.call('HINCRBY', KEYS[5], 'overflow_used', overflow)
end
redis.call('EXPIRE', KEYS[5], ttlMon)

-- (B) sub 일 usage hash
redis.call('HINCRBY', KEYS[6], 'total_used', bytes)
redis.call('HINCRBY', KEYS[6], 'plan_used', plan_take)
redis.call('HINCRBY', KEYS[6], 'member_family_used', family_take)
redis.call('HINCRBY', KEYS[6], 'gift_used', gift_take_total)
if overflow > 0 then
  redis.call('HINCRBY', KEYS[6], 'overflow_used', overflow)
end
redis.call('EXPIRE', KEYS[6], ttlDay)

-- (C) family 월/일 usage hash (가족 풀에서 실제로 빠진 양만 기록)
redis.call('HINCRBY', KEYS[7], 'family_used', family_take)
redis.call('HINCRBY', KEYS[8], 'family_used', family_take)
redis.call('EXPIRE', KEYS[7], ttlMon)
redis.call('EXPIRE', KEYS[8], ttlDay)

-- (D) app별 ZSET (월/일)
redis.call('ZINCRBY', KEYS[9], bytes, appId)
redis.call('ZINCRBY', KEYS[10], bytes, appId)
redis.call('EXPIRE', KEYS[9], ttlMon)
redis.call('EXPIRE', KEYS[10], ttlDay)

-- gift index도 월 TTL 유지
redis.call('EXPIRE', KEYS[4], ttlMon)

-- =========================================================
-- 7) Notify 업데이트 (Plan: 월/일, Family: 월)
-- =========================================================

-- Plan notify
local plan_used_new = mon_plan_used + plan_take
local plan_th, plan_rem2, plan_pct = threshold(plan_limit, plan_used_new)

local plan_fire, plan_last = update_notify(KEYS[11], plan_th)  -- 월 fire 여부
update_notify(KEYS[12], plan_th)                               -- 일은 상태만 유지

-- Family pool notify (월)
local pool_used_new = mon_pool_used + family_take
local fam_th, fam_rem2, fam_pct = threshold(family_limit_total, pool_used_new)

local fam_fire, fam_last = update_notify(KEYS[13], fam_th)

-- =========================================================
-- 8) 결과 반환 (컨슈머/프로듀서가 다음 로직에서 사용)
-- =========================================================
return {
  "OK",
  bytes,
  gift_take_total, plan_take, family_take, overflow,

  -- 가족 풀 사용량/잔여
  pool_used_new, (family_limit_total - pool_used_new),

  -- 멤버 개인 가족사용량/개인한도
  (mon_family_used + family_take), family_member_limit,

  -- plan notify: fire 여부, 현재 임계치, 잔여량/잔여%, 이전 임계치
  plan_fire, plan_th, plan_rem2, plan_pct, plan_last,

  -- family notify: fire 여부, 현재 임계치, 잔여량/잔여%, 이전 임계치
  fam_fire, fam_th, fam_rem2, fam_pct, fam_last,

  -- gift notify fire 목록(JSON)
  cjson.encode(fired_gifts)
}
