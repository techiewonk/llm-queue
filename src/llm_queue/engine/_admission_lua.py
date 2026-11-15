"""Lua of the admission scripts: PICK_TAKE, FINISH, UNDO, RENEW and SWEEP (plan §3.4, §3.18).

Each script takes the key prefix and one JSON argument and builds its key names itself, which
needs the single-primary store 0.3 supports. Times are the store's clock in ms. Budgets are GCRA
clocks (`smooth`) or minute counters (`fixed`); class clocks enforce the priority shares.
"""

# ruff: noqa: E501  # Lua source in strings; wrapping its lines hurts reading it

from __future__ import annotations

_COMMON = r"""
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
-- ARGV[3], tests only: an injected clock (ms) instead of the store's (production never sends it).
if ARGV[3] then now = tonumber(ARGV[3]) end
local minute = math.floor(now / 60000)
local P = ARGV[1]
local req = cjson.decode(ARGV[2])
-- Timings and limits: `EngineTimings` (sent as `t` by every call) and the registry's breaker
-- settings (on each row); the numbers after them are the defaults.
local T = req.t or {}
local KEY_MS  -- budget key lifetime: set below, once num() exists
local function mk(alias, name) return P .. ':{m:' .. alias .. '}:' .. name end
local function num(v, d) if v == nil or v == false or v == cjson.null then return d end return tonumber(v) end
local function hnum(key, field, d) return num(redis.call('HGET', key, field), d) end
local function present(v) return v ~= nil and v ~= false and v ~= cjson.null end
-- Costs and refunds go to HINCRBY, which raises on a fraction after earlier writes stayed (FL-9).
local function whole(v) return math.floor(v + 0.5) end
KEY_MS = num(T.budget_key_ms, 180000)  -- budget and limiter keys live this long (> 2 minutes)

-- One budget: field prefix f, limit L per minute, burst fraction bf, window mode.
local function avail(key, f, L, bf, mode)
  local burst = math.max(1, math.floor(L * bf))
  if mode == 'fixed' then
    local n = 0
    if hnum(key, f .. '_win', -1) == minute then n = hnum(key, f .. '_n', 0) end
    return L - n, n
  end
  local per = 60000 / L
  local tat = math.max(hnum(key, f .. '_tat', now), now)
  return (now + burst * per - tat) / per, tat
end

-- Room for cost c behind R reserved units: returns wait ms (0 = fits) and the new state.
local reset_minute = num(redis.call('GET', P .. ':reset_minute'), -1)
local function room(key, f, L, c, R, bf, mode)
  local burst = math.max(1, math.floor(L * bf))
  if mode == 'fixed' then
    -- After a store reset, a fixed window holds until the next minute (plan T10).
    if reset_minute == minute and hnum(key, f .. '_win', -1) ~= minute then
      return (minute + 1) * 60000 - now, nil
    end
    local _, n = avail(key, f, L, bf, mode)
    if n + R + c <= L or (n == 0 and R == 0 and c > L) then return 0, n + c end
    return (minute + 1) * 60000 - now, nil
  end
  local per = 60000 / L
  local tat = math.max(hnum(key, f .. '_tat', now), now)
  if c > burst and R == 0 then
    if tat <= now then return 0, now + c * per end
    return tat - now, nil
  end
  local over = tat + (R + c) * per - now - burst * per
  if over > 0 then return over, nil end
  return 0, tat + c * per
end

local function write(key, f, mode, value)
  if mode == 'fixed' then
    redis.call('HSET', key, f .. '_win', minute, f .. '_n', value)
  else
    redis.call('HSET', key, f .. '_tat', tostring(value))
  end
  redis.call('PEXPIRE', key, KEY_MS)
end

-- Give back g units (negative g charges more). Fixed counters only inside the grant's minute.
local function give(key, f, L, g, mode, granted_minute)
  if not L or L <= 0 or g == 0 then return end
  if mode == 'fixed' then
    if granted_minute ~= minute and g > 0 then return end
    if hnum(key, f .. '_win', -1) ~= minute then
      -- The minute turned: an overrun (g < 0) is charged to the new minute, not dropped.
      if g < 0 then
        redis.call('HSET', key, f .. '_win', minute, f .. '_n', -g)
        redis.call('PEXPIRE', key, KEY_MS)
      end
      return
    end
    redis.call('HSET', key, f .. '_n', math.max(0, hnum(key, f .. '_n', 0) - g))
    redis.call('PEXPIRE', key, KEY_MS)
    return
  end
  local per = 60000 / L
  local tat = math.max(hnum(key, f .. '_tat', now), now)
  local value = tat - g * per
  if g > 0 then value = math.max(value, now) end
  redis.call('HSET', key, f .. '_tat', tostring(value))
  redis.call('PEXPIRE', key, KEY_MS)  -- the key may have expired since the grant
end

-- The budgets of a deployment row for class cls: {key, field, limit, cost kind}.
local function budgets(alias, row, model, cls, account)
  local list = {}
  local limits = row.rate_limits
  local rkey = mk(alias, 'rate:' .. row.key)
  local share = 1
  if cls == 'n' then share = 1 - num(model.priority_reserve, 0) else share = 1 - num(model.normal_min_share, 0) end
  local function add(key, f, L, kind)
    if present(L) and tonumber(L) > 0 then table.insert(list, {key, f, tonumber(L), kind}) end
  end
  -- A class share of 0 (a reserve of 1) leaves the class no room at all: limit 0, which never
  -- fits (FL-18: `add` dropped it, and the class ran with no rate cap of its own).
  local function add_share(key, f, L, sh, kind)
    if not present(L) or tonumber(L) <= 0 then return end
    if sh <= 0 then table.insert(list, {key, f, 0, kind}) else add(key, f, L * sh, kind) end
  end
  if limits == 'both' or limits == 'rpm' then
    add(rkey, 'rpm', row.rpm, 'r')
    if share < 1 then add_share(rkey, cls .. '_rpm', row.rpm, share, 'r') end
  end
  if limits == 'both' or limits == 'tpm' then
    add(rkey, 'tpm', row.tpm, 't')
    if share < 1 then add_share(rkey, cls .. '_tpm', row.tpm, share, 't') end
  end
  if account then
    local akey = P .. ':{a:' .. row.account .. '}:rate'
    local ashare = 1
    if cls == 'n' then ashare = 1 - num(row.account_priority_reserve, 0) end
    add(akey, 'rpm', row.shared_rpm, 'r')
    add(akey, 'tpm', row.shared_tpm, 't')
    if ashare < 1 then
      add_share(akey, 'n_rpm', row.shared_rpm, ashare, 'r')
      add_share(akey, 'n_tpm', row.shared_tpm, ashare, 't')
    end
  end
  return list
end

local function lease_keys(alias, dep)
  return mk(alias, 'lease:' .. dep .. ':p'), mk(alias, 'lease:' .. dep .. ':n')
end

local function limit_of(alias, row)
  local key = mk(alias, 'ac:' .. row.key)
  local cap = num(row.max_concurrency, 200)
  local limit = hnum(key, 'limit', nil)
  if limit == nil then
    -- A new deployment takes its full share at once (plan E74).
    limit = cap
    redis.call('HSET', key, 'limit', tostring(limit))
  end
  return math.max(1, math.floor(math.min(limit, cap))), cap
end

local function cfg_of(alias)
  local raw = redis.call('HGET', mk(alias, 'cfg'), 'numbers')
  if not raw then return nil end
  local doc = cjson.decode(raw)
  local rows = {}
  for _, row in ipairs(doc.deployments) do rows[row.key] = row end
  return doc.model, rows, hnum(mk(alias, 'cfg'), 'changes', -1)
end
"""

PICK_TAKE = (
    _COMMON
    + r"""
local alias = req.alias
local model, rows, changes = cfg_of(alias)
if model == nil then return cjson.encode({r = 'no_config'}) end
if changes ~= num(req.changes, -2) then return cjson.encode({r = 'stale_config', changes = changes}) end
local cls = req.cls
local rid = req.request_id
local entry_key = mk(alias, 'inprog:' .. rid)

-- Finished or cancelled? Checked for every ask: a fallback ask (claim off) once started a
-- call that was already cancelled or answered elsewhere. Only the claim write needs `claim`.
local job_key = P .. ':{r:' .. rid .. '}:job'
if redis.call('EXISTS', P .. ':{r:' .. rid .. '}:res') == 1 then return cjson.encode({r = 'done'}) end
if redis.call('EXISTS', P .. ':{r:' .. rid .. '}:cancel') == 1 then return cjson.encode({r = 'cancelled'}) end
local state = redis.call('HGET', job_key, 'state')
if state == 'done' or state == 'failed' or state == 'expired' or state == 'cancelled' then
  return cjson.encode({r = 'done'})
end
-- A call just received (req.callers = the caller TTL): `gone` when every caller of it is gone,
-- unless the store came back less than a TTL ago (their keys not renewed yet). It was a script
-- of its own (CALLERS_ALIVE in requests.py), one more round trip per call.
if present(req.callers) then
  local epoch = tonumber(redis.call('GET', P .. ':epoch') or '')
  if epoch and tonumber(t[1]) - epoch >= num(req.callers, 30) then
    local members = redis.call('SMEMBERS', P .. ':{r:' .. rid .. '}:callers')
    local alive = #members == 0
    for _, caller in ipairs(members) do
      if redis.call('EXISTS', P .. ':caller:' .. caller) == 1 then alive = true break end
    end
    if not alive then return cjson.encode({r = 'gone'}) end
  end
end
-- Running under another worker with a fresh heartbeat (on another model, or between its
-- retries when no entry exists): a redelivered message is a duplicate, not a new run.
-- The job owner is `worker@tag` (tag: the message id), so a second delivery of one id to the
-- same pod is a duplicate too, not only a delivery to another pod (RESCAN A1).
local job_owner = req.job_owner or req.worker
if req.claim and state == 'running' then
  local runner = redis.call('HGET', job_key, 'owner')
  if runner and runner ~= job_owner
    and now - hnum(job_key, 'hb', 0) < num(req.lease_ms, 60000) then
    return cjson.encode({r = 'duplicate'})
  end
end
local held = redis.call('HGET', entry_key, 'owner')
if held then
  if held == req.owner then
    local again = {r = 'granted', dep = redis.call('HGET', entry_key, 'dep'),
      minute = hnum(entry_key, 'minute', minute), repeat_ = true}
    if req.chunks then again.streamed = redis.call('XLEN', P .. ':{r:' .. rid .. '}:chunks') end
    return cjson.encode(again)
  end
  local holder = redis.call('HGET', entry_key, 'worker')
  local hb = hnum(job_key, 'hb', 0)
  -- Alive by its own lease too (FL-6): with the job record gone (expired, or a call with no
  -- claim) hb read 0 and a live grant was dropped, so the call ran twice.
  local leased = num(redis.call('ZSCORE', mk(alias, 'inprog'), held), 0) > now
  local fresh = (leased or now - hb < num(req.lease_ms, 60000))
    and redis.call('HGET', entry_key, 'started') ~= false
  if fresh then return cjson.encode({r = 'duplicate'}) end
  -- A dead owner (no heartbeat for a lease): drop its slot and entry (no refund).
  local old_dep = redis.call('HGET', entry_key, 'dep')
  local old_cls = redis.call('HGET', entry_key, 'cls')
  if old_dep then
    redis.call('ZREM', mk(alias, 'lease:' .. old_dep .. ':' .. old_cls), held)
    -- Its worker died mid-probe: free the probe (as SWEEP does), or it blocks the breaker.
    local old_cb = mk(alias, 'cb:' .. old_dep)
    if redis.call('HGET', old_cb, 'probe') == held then redis.call('HSET', old_cb, 'probe', '') end
  end
  redis.call('ZREM', mk(alias, 'inprog'), held)
  redis.call('DEL', entry_key)
end

-- Foreign call (model fallback): only while this model has no waiters of its own.
local foreign = present(req.foreign)
local wait_keys = {p = mk(alias, 'wait:p'), n = mk(alias, 'wait:n')}
local info_key = mk(alias, 'waitinfo')
-- Expired waiters go: `waitexp` scores each by its expiry (member `cls|id`), so finding them
-- reads only those (the first 50 of each class were decoded on every call before).
local exp_key = mk(alias, 'waitexp')
for _, member in ipairs(redis.call('ZRANGEBYSCORE', exp_key, '-inf', '(' .. now, 'LIMIT', 0, 100)) do
  local c, id = string.match(member, '^(%a)|(.*)$')
  local raw = c and redis.call('HGET', info_key, id)
  -- A waiter refreshed since (its expiry moved on) stays; only its stale member goes.
  if c and (not raw or num(cjson.decode(raw).exp, 0) < now) then
    redis.call('ZREM', wait_keys[c], id)
    redis.call('HDEL', info_key, id)
  end
  redis.call('ZREM', exp_key, member)
end
if foreign then
  -- Only live waiters count: expired ones past the first 50 (cleaned above) blocked fallback.
  for _, c in ipairs({'p', 'n'}) do
    for _, id in ipairs(redis.call('ZRANGE', wait_keys[c], 0, 199)) do
      local raw = redis.call('HGET', info_key, id)
      if raw and num(cjson.decode(raw).exp, 0) >= now then
        return cjson.encode({r = 'busy', reason = 'waiters'})
      end
    end
  end
end

-- What older waiters set aside, per deployment (plan §3.18 reservation rule).
local reserved = {}
local function reserve_from(list, into)
  into = into or reserved
  for _, id in ipairs(list) do
    if id ~= rid then
      local raw = redis.call('HGET', info_key, id)
      if raw then
        local w = cjson.decode(raw)
        if num(w.exp, 0) >= now then
          for _, dep in ipairs(w.deps) do
            local slot = into[dep] or {r = 0, t = 0, s = 0}
            slot.r = slot.r + num(w.r, 1)
            slot.t = slot.t + num(w.t, 0)
            slot.s = slot.s + 1
            into[dep] = slot
          end
        end
      end
    end
  end
end
local pos = num(req.pos, now * 1000)
if not foreign then
  -- %.17g: plain `..` formats with %.14g and misorders waiters within ~0.1 ms of each other.
  local before = '(' .. string.format('%.17g', pos)
  reserve_from(redis.call('ZRANGEBYSCORE', wait_keys[cls], '-inf', before, 'LIMIT', 0, 200))
end
-- What waiting priority calls need, per deployment: added to a normal call's reservation
-- per candidate (only where that candidate's normal use reached its floor), never to the shared
-- table, so the result no longer depends on the order the candidates are tried in.
local priority_reserved = {}
if cls == 'n' and not foreign then
  reserve_from(redis.call('ZRANGE', wait_keys.p, 0, 199), priority_reserved)
end

local cost_r = whole(num(req.cost_r, 1))
local cost_t = whole(num(req.cost_t, 0))
local best_wait, best_reason = nil, nil
-- A breaker's one probe call is in flight: its answer comes within an attempt, so a call
-- waits for it instead of failing as `model_unavailable` (scenario P7 at 300 %: ten calls
-- that arrived during the probe failed after every region had recovered).
local probe_wait = false
local function note(ms, reason)
  if best_wait == nil or ms < best_wait then best_wait, best_reason = ms, reason end
end

-- Evaluate one candidate: fits (with its new budget states and headroom) or the wait.
local function evaluate(cand)
  local row = rows[cand.key]
  if row == nil then return nil end
  local health = mk(alias, 'health')
  if row.enabled == false or row.account_enabled == false or row.outside_residency or row.blocked
    or redis.call('HEXISTS', health, cand.key .. '|until') == 1 then
    return nil
  end
  local cb = mk(alias, 'cb:' .. cand.key)
  local probe = false
  if redis.call('HGET', cb, 'state') == 'open' then
    if hnum(cb, 'until', 0) > now then return nil end
    local who = redis.call('HGET', cb, 'probe')
    if who and who ~= '' and hnum(cb, 'probe_at', 0) > now - num(row.breaker_probe_secs, 60) * 1000 then
      probe_wait = true
      return nil
    end
    probe = true
  end
  -- Throttled by the provider (Retry-After or a growing backoff, set by FINISH): every pod
  -- skips it until then; with nothing else left the call waits as a waiter (RESCAN P2).
  local thr_until = hnum(cb, 'thr_until', 0)
  if thr_until > now then return {wait = thr_until - now, reason = 'throttled'} end
  local res = reserved[cand.key] or {r = 0, t = 0, s = 0}
  -- Normal calls also set aside what waiting priority calls need, unless normal use is below
  -- normal_min_share this minute.
  local rkey = mk(alias, 'rate:' .. cand.key)
  local prio = priority_reserved[cand.key]
  if prio then
    local floor = num(model.normal_min_share, 0) * num(row.rpm, 0)
    local used = 0
    if hnum(rkey, 'seen_win', -1) == minute then used = hnum(rkey, 'seen_n', 0) end
    if floor <= 0 or used >= floor then
      res = {r = res.r + prio.r, t = res.t + prio.t, s = res.s + prio.s}
    end
  end
  local states = {}
  local headroom = nil
  local most = nil
  local taken = budgets(alias, row, model, cls, true)
  for _, b in ipairs(taken) do
    if b[3] <= 0 then return {wait = (minute + 1) * 60000 - now, reason = 'reserve'} end
    local cost = cost_r
    local R = res.r
    if b[4] == 't' then cost, R = cost_t, res.t end
    local mode = row.rate_window
    local wait, value = room(b[1], b[2], b[3], cost, R, num(row.rate_burst, 0.1), mode)
    if wait > 0 then
      local reason = (b[4] == 'r') and 'rpm' or 'tpm'
      if string.find(b[1], '{a:', 1, true) then reason = 'account'
      elseif string.sub(b[2], 2, 2) == '_' then reason = 'reserve' end
      if R > 0 then reason = 'not_head' end
      return {wait = wait, reason = reason}
    end
    table.insert(states, {b[1], b[2], mode, value})
    local free = avail(b[1], b[2], b[3], num(row.rate_burst, 0.1), mode) - R
    local left = free - cost
    -- Headroom is the share free before this call of what can be taken now: the minute (fixed)
    -- or the bucket (smooth, never more than rate_burst of the minute). Before, not after: a
    -- call larger than the bucket could otherwise never fall back to an idle model.
    local size = b[3]
    if mode ~= 'fixed' then size = math.max(1, math.floor(b[3] * num(row.rate_burst, 0.1))) end
    local frac = free / size
    if headroom == nil or frac < headroom then headroom = frac end
    if b[4] == 'r' and not string.find(b[1], '{a:', 1, true) then most = left end
  end
  -- Concurrency: slots per class, after what older waiters set aside.
  local lp, ln = lease_keys(alias, cand.key)
  redis.call('ZREMRANGEBYSCORE', lp, '-inf', now)
  redis.call('ZREMRANGEBYSCORE', ln, '-inf', now)
  local in_p, in_n = redis.call('ZCARD', lp), redis.call('ZCARD', ln)
  local limit = limit_of(alias, row)
  local share = 1 - num(model.priority_reserve, 0)
  if cls == 'p' then share = 1 - num(model.normal_min_share, 0) end
  local cap = math.floor(share * limit)
  if limit >= 2 and share > 0 then cap = math.max(1, cap) end
  local mine = (cls == 'p') and in_p or in_n
  if in_p + in_n + res.s >= limit or mine >= cap then
    return {wait = num(T.slot_wait_ms, 50) + math.floor(num(req.rand, 0.5) * num(T.slot_jitter_ms, 150)), reason = 'slot'}
  end
  local slot_frac = (limit - in_p - in_n - res.s - 1) / limit
  if headroom == nil or slot_frac < headroom then headroom = slot_frac end
  return {fits = true, states = states, headroom = headroom, most = most or (limit - in_p - in_n),
    probe = probe, limit = limit, taken = taken}
end

-- Sticky session: keep the session's deployment while it fits or would wait only briefly.
local sticky_key = nil
local sticky_dep = nil
if present(req.sticky) then
  sticky_key = mk(alias, 'sticky:' .. req.sticky.hash)
  sticky_dep = redis.call('GET', sticky_key)
end

-- Latency-aware exclusion (plan §3.4): a deployment much slower than the model's median,
-- with enough samples and at least 3 to compare, becomes a last choice (never excluded alone).
local slow = {}
do
  local hs = model.health or {}
  local ratio, min_ms, min_n = num(hs.slow_ratio, 3), num(hs.slow_min_secs, 5) * 1000, num(hs.slow_min_samples, 20)
  local health_key = mk(alias, 'health')
  local values, lat_of = {}, {}
  for _, cand in ipairs(req.candidates) do
    -- An average not updated for slow_stale_ms is ignored: a slow one gets calls again (A5).
    local lat = hnum(health_key, cand.key .. '|lat', nil)
    local fresh = hnum(health_key, cand.key .. '|lat_at', 0) >= now - num(T.slow_stale_ms, 300000)
    if lat and fresh and hnum(health_key, cand.key .. '|n', 0) >= min_n then
      lat_of[cand.key] = lat
      table.insert(values, lat)
    end
  end
  if #values >= 3 then
    table.sort(values)
    local median = values[math.floor((#values + 1) / 2)]
    for key, lat in pairs(lat_of) do
      if lat > ratio * median and lat > min_ms then slow[key] = true end
    end
  end
end

local fitting = {confirmed = {}, fallback = {}}
local last = {}
local results = {}
local any_usable = false
-- `ordered` (the default) takes the first confirmed candidate that fits, so evaluation stops
-- there: evaluating all of them cost ~12 store reads each (30 deployments: p99 above 2 ms).
local first_fit = req.strategy == 'ordered' and not present(req.sticky)
for _, cand in ipairs(req.candidates) do
  local res = evaluate(cand)
  if res ~= nil then
    any_usable = true
    results[cand.key] = res
    if res.fits then
      if cand.last or slow[cand.key] then table.insert(last, cand) else table.insert(fitting[cand.tier], cand) end
      if first_fit and cand.tier == 'confirmed' and not (cand.last or slow[cand.key]) then break end
    else
      note(res.wait, res.reason)
    end
  end
end
if not any_usable then
  if probe_wait then return cjson.encode({r = 'wait', ms = num(T.probe_wait_ms, 200), reason = 'probe'}) end
  return cjson.encode({r = 'none'})
end

local chosen = nil
if sticky_dep and results[sticky_dep] then
  local s = results[sticky_dep]
  if s.fits then
    for _, cand in ipairs(req.candidates) do if cand.key == sticky_dep then chosen = cand end end
  elseif s.wait <= num(req.sticky.move_ms, 1000) then
    best_wait, best_reason = s.wait, s.reason
    chosen = false
  end
end

-- Rotation counters of the weighted pick, written only with a grant (FL-17: a pick that the
-- fallback headroom check then refused had already moved the rotation).
local rot_writes = {}
local function pick(list)
  if #list == 0 then return nil end
  local strategy = req.strategy
  if strategy == 'round_robin' then
    local rot = mk(alias, 'rot')
    local best, best_seq = nil, nil
    for _, cand in ipairs(list) do
      local seq = hnum(rot, 'rr|' .. cand.key, 0)
      if best_seq == nil or seq < best_seq then best, best_seq = cand, seq end
    end
    return best
  elseif strategy == 'weighted' then
    local rot = mk(alias, 'rot')
    local total, best, best_cw = 0, nil, nil
    for _, cand in ipairs(list) do
      local w = num(rows[cand.key].weight, 1)
      local cw = hnum(rot, 'cw|' .. cand.key, 0) + w
      rot_writes['cw|' .. cand.key] = tostring(cw)
      total = total + w
      if best_cw == nil or cw > best_cw then best, best_cw = cand, cw end
    end
    rot_writes['cw|' .. best.key] = tostring(best_cw - total)
    return best
  elseif strategy == 'headroom' then
    local mode = req.pick or 'percent'
    if mode == 'spread' then
      local total = 0
      for _, cand in ipairs(list) do total = total + math.max(0.0001, results[cand.key].headroom) end
      local x = num(req.rand, 0.5) * total
      for _, cand in ipairs(list) do
        x = x - math.max(0.0001, results[cand.key].headroom)
        if x <= 0 then return cand end
      end
      return list[#list]
    end
    local best, best_score = nil, nil
    for _, cand in ipairs(list) do
      local score = (mode == 'max') and results[cand.key].most or results[cand.key].headroom
      if best_score == nil or score > best_score then best, best_score = cand, score end
    end
    return best
  end
  return list[1]
end

if chosen == nil then
  chosen = pick(fitting.confirmed) or pick(fitting.fallback) or pick(last)
end

if foreign and chosen and num(req.foreign.require_pct, 0) > 0 then
  if results[chosen.key].headroom < num(req.foreign.require_pct, 0) / 100 then chosen = nil end
end

if not chosen then
  if foreign then return cjson.encode({r = 'busy', reason = 'headroom'}) end
  -- Refused: register (or refresh) as a waiter, with the deployments it can use.
  local deps = {}
  for _, cand in ipairs(req.candidates) do table.insert(deps, cand.key) end
  local exp = now + num(req.waiter_ms, 2000)
  redis.call('ZADD', wait_keys[cls], pos, rid)
  redis.call('HSET', info_key, rid, cjson.encode({r = cost_r, t = cost_t, deps = deps, exp = exp}))
  redis.call('ZADD', exp_key, exp, cls .. '|' .. rid)
  redis.call('PEXPIRE', exp_key, KEY_MS)
  return cjson.encode({r = 'wait', ms = math.max(1, math.floor(best_wait or num(T.wait_ms, 100))),
    reason = best_reason or 'slot'})
end

-- Take everything at once.
local row = rows[chosen.key]
local res = results[chosen.key]
for _, s in ipairs(res.states) do write(s[1], s[2], s[3], s[4]) end
local rkey = mk(alias, 'rate:' .. chosen.key)
if hnum(rkey, 'seen_win', -1) ~= minute then
  redis.call('HSET', rkey, 'seen_win', minute, 'seen_calls', 0, 'seen_tokens', 0, 'seen_n', 0)
  redis.call('PEXPIRE', rkey, KEY_MS)
end
redis.call('HINCRBY', rkey, 'seen_calls', cost_r)
redis.call('HINCRBY', rkey, 'seen_tokens', cost_t)
if cls == 'n' then redis.call('HINCRBY', rkey, 'seen_n', cost_r) end
local lp, ln = lease_keys(alias, chosen.key)
local lease_ms = num(req.lease_ms, 60000)
redis.call('ZADD', (cls == 'p') and lp or ln, now + lease_ms, req.owner)
redis.call('ZADD', mk(alias, 'inprog'), now + lease_ms, req.owner)
redis.call('HSET', entry_key, 'owner', req.owner, 'worker', req.worker, 'dep', chosen.key,
  'cls', cls, 'minute', minute, 'cost_r', cost_r, 'cost_t', cost_t, 'account', row.account,
  'started', now, 'request', rid, 'served', alias, 'mode', row.rate_window or 'smooth',
  'bud', cjson.encode(res.taken), 'job_owner', job_owner)
redis.call('PEXPIRE', entry_key, lease_ms * 2)
if res.probe then
  local cb = mk(alias, 'cb:' .. chosen.key)
  redis.call('HSET', cb, 'probe', req.owner, 'probe_at', now)
end
-- Running, by this worker: a fallback grant too, so a redelivery of the call is refused
-- while it runs on the other model (it was granted again and ran twice).
if req.claim or redis.call('EXISTS', job_key) == 1 then
  redis.call('HSET', job_key, 'state', 'running', 'owner', job_owner, 'hb', now)
  -- A claim made here (the caller's record was gone) had no TTL and stayed for good.
  if redis.call('PTTL', job_key) < 0 then redis.call('PEXPIRE', job_key, num(T.job_ttl_ms, 3600000)) end
end
redis.call('ZREM', wait_keys[cls], rid)
redis.call('HDEL', info_key, rid)
redis.call('ZREM', exp_key, cls .. '|' .. rid)
if foreign and present(req.foreign.from_alias) then
  redis.call('ZREM', mk(req.foreign.from_alias, 'wait:' .. cls), rid)
  redis.call('HDEL', mk(req.foreign.from_alias, 'waitinfo'), rid)
  redis.call('ZREM', mk(req.foreign.from_alias, 'waitexp'), cls .. '|' .. rid)
end
-- A first attempt counts for the retry budget (req.count_first: the minute of its key, from
-- the worker's clock as the budget check reads it): a pipeline of its own before.
if present(req.count_first) then
  local retries = mk(alias, 'retries:' .. req.count_first)
  redis.call('HINCRBY', retries, 'first', 1)
  redis.call('EXPIRE', retries, 180)
end
if sticky_key then
  redis.call('SET', sticky_key, chosen.key, 'PX', num(req.sticky.ttl_ms, 1800000))
end
if req.strategy == 'round_robin' then
  local rot = mk(alias, 'rot')
  redis.call('HSET', rot, 'rr|' .. chosen.key, redis.call('HINCRBY', rot, 'rr_seq', 1))
end
for field, value in pairs(rot_writes) do redis.call('HSET', mk(alias, 'rot'), field, value) end
local granted = {r = 'granted', dep = chosen.key, minute = minute, limit = res.limit,
  tier = chosen.tier}
-- A stream's call: how far its chunk stream got (another worker may have begun it, R19); an
-- XLEN of its own before.
if req.chunks then granted.streamed = redis.call('XLEN', P .. ':{r:' .. rid .. '}:chunks') end
return cjson.encode(granted)
"""
)

# Undo or finish one grant: give back budgets, release the slot and the entry.
_SETTLE = r"""
local function settle(alias, owner, rid, refund_r, refund_t, learn, outcome, latency, retry_after_ms)
  local entry_key = mk(alias, 'inprog:' .. rid)
  if redis.call('HGET', entry_key, 'owner') ~= owner then return 0 end
  refund_r, refund_t = whole(refund_r), whole(refund_t)
  local dep = redis.call('HGET', entry_key, 'dep')
  local cls = redis.call('HGET', entry_key, 'cls')
  local granted_minute = hnum(entry_key, 'minute', minute)
  -- Never give back more than was taken (a reload between grant and refund changed the estimate).
  refund_r = math.min(refund_r, hnum(entry_key, 'cost_r', refund_r))
  refund_t = math.min(refund_t, hnum(entry_key, 'cost_t', refund_t))
  local model, rows = cfg_of(alias)
  local row = rows and rows[dep]
  -- The budgets and window the grant took (stored at grant time), not today's config row (A7).
  local taken = redis.call('HGET', entry_key, 'bud')
  local mode = redis.call('HGET', entry_key, 'mode')
  if taken and mode then
    for _, b in ipairs(cjson.decode(taken)) do
      local g = (b[4] == 'r') and refund_r or refund_t
      give(b[1], b[2], b[3], g, mode, granted_minute)
    end
  elseif row then
    for _, b in ipairs(budgets(alias, row, model, cls, true)) do
      local g = (b[4] == 'r') and refund_r or refund_t
      give(b[1], b[2], b[3], g, row.rate_window, granted_minute)
    end
  end
  if row then
    local rkey = mk(alias, 'rate:' .. dep)
    if granted_minute == minute and hnum(rkey, 'seen_win', -1) == minute then
      redis.call('HINCRBY', rkey, 'seen_calls', -refund_r)
      redis.call('HINCRBY', rkey, 'seen_tokens', -refund_t)
      if cls == 'n' then redis.call('HINCRBY', rkey, 'seen_n', -refund_r) end
    end
  end
  redis.call('ZREM', mk(alias, 'lease:' .. dep .. ':' .. cls), owner)
  redis.call('ZREM', mk(alias, 'inprog'), owner)
  -- A breaker probe that ended without a verdict (cut, given back, throttled) frees the probe
  -- at once: it blocked every call for breaker_probe_secs, and they waited it out (scenario L4).
  if outcome ~= 'ok' and outcome ~= 'fail' and redis.call('HGET', mk(alias, 'cb:' .. dep), 'probe') == owner then
    redis.call('HSET', mk(alias, 'cb:' .. dep), 'probe', '')
  end
  redis.call('DEL', entry_key)
  if learn and row then
    local ac = mk(alias, 'ac:' .. dep)
    local limit, cap = limit_of(alias, row)
    if outcome == 'throttle' then
      local thr = mk(alias, 'thr:' .. dep .. ':' .. minute)
      redis.call('INCR', thr)
      redis.call('PEXPIRE', thr, KEY_MS)
      if hnum(ac, 'cut_at', 0) < now - num(T.limiter_cut_ms, 1000) then
        redis.call('HSET', ac, 'limit', tostring(math.max(1, math.floor(limit * num(T.limiter_cut_factor, 0.7)))), 'cut_at', now)
      end
    elseif outcome == 'ok' then
      redis.call('HSET', ac, 'limit', tostring(math.min(cap, limit + 1 / math.max(1, limit))))
    end
    local cb = mk(alias, 'cb:' .. dep)
    if outcome == 'ok' then
      -- An open breaker closes only on its probe's answer or once its cooldown ended: the ok
      -- of a call granted before it opened closed it early (A10).
      if redis.call('HGET', cb, 'state') ~= 'open' or redis.call('HGET', cb, 'probe') == owner
        or hnum(cb, 'until', 0) <= now then
        redis.call('HSET', cb, 'state', 'closed', 'failures', 0, 'probe', '')
      end
      redis.call('HSET', cb, 'thr_n', 0)  -- a success ends the throttle streak
      redis.call('DEL', mk(alias, 'terr:' .. dep))
    elseif outcome == 'throttle' then
      -- Throttled until max(Retry-After, a jittered backoff doubling per throttle in a row).
      local n = redis.call('HINCRBY', cb, 'thr_n', 1)
      local top = num(T.throttle_max_ms, 30000)
      local back = math.min(top, num(T.throttle_base_ms, 1000) * 2 ^ math.min(n - 1, 20))
      back = math.min(top, back * (1 + num(req.rand, 0.5)))
      local until_ = now + math.floor(math.max(num(retry_after_ms, 0), back))
      if until_ > hnum(cb, 'thr_until', 0) then redis.call('HSET', cb, 'thr_until', until_) end
    elseif outcome == 'fail' then
      -- Recent failures only: one more than 60 s after the last starts the count again (it
      -- counted forever until a success, so 5 failures over hours opened the breaker).
      if hnum(cb, 'fail_at', 0) < now - num(row.breaker_window_secs, 60) * 1000 then redis.call('HSET', cb, 'failures', 0) end
      redis.call('HSET', cb, 'fail_at', now)
      local fails = redis.call('HINCRBY', cb, 'failures', 1)
      local probing = redis.call('HGET', cb, 'probe') == owner
      if fails >= num(row.breaker_threshold, 5) or probing then
        redis.call('HSET', cb, 'state', 'open', 'probe', '',
          'until', now + math.floor(num(row.breaker_cooldown_secs, 15) * 1000))
      end
    end
    if latency and latency >= 0 then
      local health = mk(alias, 'health')
      local old = hnum(health, dep .. '|lat', latency)
      -- A stale average restarts from this sample (and PICK_TAKE ignores stale ones, A5).
      if hnum(health, dep .. '|lat_at', 0) < now - num(T.slow_stale_ms, 300000) then old = latency end
      redis.call('HSET', health, dep .. '|lat', tostring(old * 0.8 + latency * 0.2), dep .. '|lat_at', now)
      redis.call('HINCRBY', health, dep .. '|n', 1)
    end
  end
  return 1
end
"""

# req: alias, owner, request_id, refund_r, refund_t, outcome (ok|throttle|fail|none), latency_ms,
# retry_after_ms (a throttle's Retry-After), rand (the throttle backoff's jitter).
FINISH = (
    _COMMON
    + _SETTLE
    + r"""
return settle(req.alias, req.owner, req.request_id, num(req.refund_r, 0), num(req.refund_t, 0),
  true, req.outcome, num(req.latency_ms, -1), num(req.retry_after_ms, 0))
"""
)

# The claim of a call this worker lets go (FL-1): `running` under job owner `jo` becomes
# `queued` again, so another pod's receive of it is granted at once. It stayed `running` with a
# fresh `hb`, and every other pod got `duplicate` for about one lease (60 s). Another owner's
# claim, or a final state, is left alone.
_RELEASE_FN = r"""
local function release_claim(rid, jo)
  if not jo or jo == '' then return 0 end
  local job = P .. ':{r:' .. rid .. '}:job'
  if redis.call('HGET', job, 'state') ~= 'running' or redis.call('HGET', job, 'owner') ~= jo then
    return 0
  end
  redis.call('HSET', job, 'state', 'queued', 'hb', 0)
  redis.call('HDEL', job, 'owner')
  return 1
end
-- The job owner (`worker@tag`) inside an owner token `request|job owner|attempt`.
local function job_owner_of(owner) return string.match(owner, '^.*|([^|]*)|[^|]*$') end
"""

# req: alias, owner, request_id, keep_claim (optional). Give everything back; no limiter or
# breaker learning. Every give-back lets go of the call too, so the claim is released (FL-1)
# unless `keep_claim` says this worker still holds the call.
UNDO = (
    _COMMON
    + _SETTLE
    + _RELEASE_FN
    + r"""
local entry_key = mk(req.alias, 'inprog:' .. req.request_id)
if redis.call('HGET', entry_key, 'owner') ~= req.owner then return 0 end
local jo = redis.call('HGET', entry_key, 'job_owner') or job_owner_of(req.owner)
local done = settle(req.alias, req.owner, req.request_id, hnum(entry_key, 'cost_r', 0),
  hnum(entry_key, 'cost_t', 0), false, 'none', -1, 0)
if done == 1 and not req.keep_claim then release_claim(req.request_id, jo) end
return done
"""
)

# req: {jobs: [{request_id, owner}]} (owner: the job owner `worker@tag`). Release the claims of
# calls given back with no grant (waiting, or between retries: the drain's hand-back), as UNDO
# does for a granted one (FL-1). Reply: how many were released.
RELEASE = (
    _COMMON
    + _RELEASE_FN
    + r"""
local n = 0
for _, item in ipairs(req.jobs or {}) do n = n + release_claim(item.request_id, item.owner) end
return n
"""
)

# req: {lease_ms, items: [{alias, owner, request_id}], jobs: [{request_id, owner}]}. Heartbeat of
# held grants, and of the claim of held calls between grants (`jobs`, owner `worker@tag`).
# Reply: {n = renewed, lost = owners whose entry was gone}.
RENEW = (
    _COMMON
    + r"""
local renewed = 0
local lost = {}
local lease_ms = num(req.lease_ms, 60000)
-- The claim of a call this worker runs: hb, and a TTL that outlives the call (FL-12: a call that
-- ran past the job TTL lost its claim, and a redelivery could run it again). Only the claim's
-- own owner writes it: a fallback grant of worker A kept worker B's claim fresh.
local function beat(rid, jo)
  local job = P .. ':{r:' .. rid .. '}:job'
  if not jo or redis.call('HGET', job, 'owner') ~= jo then return end
  redis.call('HSET', job, 'hb', now)
  local ttl = redis.call('PTTL', job)
  if ttl >= 0 and ttl < lease_ms * 2 then redis.call('PEXPIRE', job, num(T.job_ttl_ms, 3600000)) end
end
for _, item in ipairs(req.jobs or {}) do beat(item.request_id, item.owner) end
for _, item in ipairs(req.items) do
  local entry_key = mk(item.alias, 'inprog:' .. item.request_id)
  if redis.call('HGET', entry_key, 'owner') == item.owner then
    local until_ = now + num(req.lease_ms, 60000)
    local dep = redis.call('HGET', entry_key, 'dep')
    local cls = redis.call('HGET', entry_key, 'cls')
    -- No XX: a lease that expired during a missed beat comes back (the owner check above
    -- guards it), so a still-running call holds its slot again.
    redis.call('ZADD', mk(item.alias, 'lease:' .. dep .. ':' .. cls), until_, item.owner)
    redis.call('ZADD', mk(item.alias, 'inprog'), 'XX', until_, item.owner)
    redis.call('PEXPIRE', entry_key, num(req.lease_ms, 60000) * 2)
    beat(item.request_id, redis.call('HGET', entry_key, 'job_owner')
      or string.match(item.owner, '^.*|([^|]*)|[^|]*$'))
    renewed = renewed + 1
  else
    table.insert(lost, item.owner)
  end
end
return cjson.encode({n = renewed, lost = lost})
"""
)

# req: {lease_ms, items: [{alias, owner, request_id, dep, cls, cost_r, cost_t}, optional bud,
# mode, account, job_owner]}. After a store reset, put back the slots and entries of calls still
# running (plan T10), and their claims (FL-13: with the claim gone a redelivery of the call ran it
# a second time). The entry keeps the budgets and window of its grant (sent, or today's row: the
# store lost the counters anyway), so settle refunds the budgets it took. A cancelled call is not
# put back.
REREGISTER = (
    _COMMON
    + r"""
local done = 0
for _, item in ipairs(req.items) do
  -- A call whose result is stored has finished: never bring its slot back. An entry another
  -- owner holds now (it took over a lost grant) is left alone.
  local current = redis.call('HGET', mk(item.alias, 'inprog:' .. item.request_id), 'owner')
  local base = P .. ':{r:' .. item.request_id .. '}:'
  if redis.call('EXISTS', base .. 'res') == 0 and redis.call('EXISTS', base .. 'cancel') == 0
    and (not current or current == item.owner) then
    local until_ = now + num(req.lease_ms, 60000)
    redis.call('ZADD', mk(item.alias, 'lease:' .. item.dep .. ':' .. item.cls), until_, item.owner)
    redis.call('ZADD', mk(item.alias, 'inprog'), until_, item.owner)
    local entry_key = mk(item.alias, 'inprog:' .. item.request_id)
    local worker = string.match(item.owner, '^.*|([^|]*)|[^|]*$') or ''
    redis.call('HSET', entry_key, 'owner', item.owner, 'worker', worker, 'dep', item.dep,
      'cls', item.cls, 'minute', minute, 'cost_r', item.cost_r, 'cost_t', item.cost_t,
      'started', now, 'request', item.request_id, 'served', item.alias)
    local model, rows = cfg_of(item.alias)
    local row = rows and rows[item.dep]
    local bud, mode, account = item.bud, item.mode, item.account
    if not present(bud) and row then bud = budgets(item.alias, row, model, item.cls, true) end
    if not present(mode) and row then mode = row.rate_window or 'smooth' end
    if not present(account) and row then account = row.account end
    if present(bud) and present(mode) then
      if type(bud) == 'table' then bud = cjson.encode(bud) end
      redis.call('HSET', entry_key, 'bud', bud, 'mode', mode)
    end
    if present(account) then redis.call('HSET', entry_key, 'account', account) end
    local jo = item.job_owner
    if not present(jo) then jo = string.match(item.owner, '^.*|([^|]*)|[^|]*$') end
    if jo then
      redis.call('HSET', entry_key, 'job_owner', jo)
      local job = base .. 'job'
      local state = redis.call('HGET', job, 'state')
      if not state or state == 'queued' or state == 'running' then
        redis.call('HSET', job, 'state', 'running', 'owner', jo, 'hb', now)
        if redis.call('PTTL', job) < 0 then redis.call('PEXPIRE', job, num(T.job_ttl_ms, 3600000)) end
      end
    end
    redis.call('PEXPIRE', entry_key, num(req.lease_ms, 60000) * 2)
    done = done + 1
  end
end
return done
"""
)

# req: {alias, deps}. Remove in-progress entries whose owner stopped renewing (the dead worker's
# estimate stays charged for that minute; plan §3.18), and lapsed leases of the deployments
# `deps`. Reply: how many were removed.
SWEEP = (
    _COMMON
    + r"""
local removed = 0
local inprog = mk(req.alias, 'inprog')
for _, owner in ipairs(redis.call('ZRANGEBYSCORE', inprog, '-inf', now, 'LIMIT', 0, 200)) do
  redis.call('ZREM', inprog, owner)
  local rid = string.match(owner, '^(.*)|[^|]*|[^|]*$')
  if rid then
    local entry_key = mk(req.alias, 'inprog:' .. rid)
    if redis.call('HGET', entry_key, 'owner') == owner then
      local dep = redis.call('HGET', entry_key, 'dep')
      local cls = redis.call('HGET', entry_key, 'cls')
      if dep then
        redis.call('ZREM', mk(req.alias, 'lease:' .. dep .. ':' .. cls), owner)
        -- Its worker died mid-probe: free the probe (it blocked the breaker for 60 s).
        local cb = mk(req.alias, 'cb:' .. dep)
        if redis.call('HGET', cb, 'probe') == owner then redis.call('HSET', cb, 'probe', '') end
      end
      redis.call('DEL', entry_key)
    end
  end
  removed = removed + 1
end
-- Leases past their end whose entry was gone (it expires after two leases: a store block or a
-- sweep gap that long) were never removed above, and stayed for good on a deployment no call
-- picked again (scenario L4 left leases). A lease's score is its end: past it, nobody holds it.
for _, dep in ipairs(req.deps or {}) do
  for _, c in ipairs({'p', 'n'}) do
    removed = removed + redis.call('ZREMRANGEBYSCORE', mk(req.alias, 'lease:' .. dep .. ':' .. c),
      '-inf', now)
  end
end
-- Waiters that expired with no `waitexp` member (written by an older library): a batch per
-- sweep, so the waiter sets never keep them for good.
local info_key = mk(req.alias, 'waitinfo')
for _, c in ipairs({'p', 'n'}) do
  local wait_key = mk(req.alias, 'wait:' .. c)
  for _, id in ipairs(redis.call('ZRANGE', wait_key, 0, 99)) do
    local raw = redis.call('HGET', info_key, id)
    if not raw or num(cjson.decode(raw).exp, 0) < now then
      redis.call('ZREM', wait_key, id)
      redis.call('HDEL', info_key, id)
    end
  end
end
return removed
"""
)
