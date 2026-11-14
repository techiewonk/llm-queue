"""Lua scripts of the registry store: publish by version rule, and compare-and-set live edits.

Both write the document, its version record, the audit entry and each model's resolved
numbers in one script call, so a reader never sees half an update. The state store runs as one
primary (cluster mode is not supported), so the keys need not share a hash tag.
"""

from __future__ import annotations

# Audit entries kept at most (besides the age limit): a busy edit loop cannot fill the store.
AUDIT_MAX_ENTRIES = 10_000

_COMMON = f"""
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local now_ms = math.floor(now * 1000)
local function audit(actor, op, version, diff, retention_ms)
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', {AUDIT_MAX_ENTRIES}, '*', 'actor', actor,
    'op', op, 'version', version, 'diff', diff, 'ts', tostring(now))
  redis.call('XTRIM', KEYS[3], 'MINID', tostring(now_ms - tonumber(retention_ms)))
end
local function write_models(first_key, last_key, first_arg, changes)
  for i = first_key, last_key do
    redis.call('HSET', KEYS[i], 'changes', changes, 'numbers', ARGV[first_arg + i - first_key])
  end
end
"""

# KEYS: doc, meta (hash), audit (stream), history (hash version -> doc), one numbers key per
# model, then the numbers keys of models the stored document has and this one does not.
# ARGV: doc, version, config hash, actor, diff JSON, audit retention ms, history size, the change
# counter the caller read, '1' to replace a stored document of the same version (it was invalid:
# treated as missing), the count of removed-model keys at the end of KEYS, then one numbers
# JSON per model key. A publish deletes the removed models' numbers (they stayed forever, CO-20).
# Reply: {action, stored version, ''} with action published | kept | not_bumped |
# ignored | conflict. Rule (plan §3.8): higher replaces, equal keeps the stored one (and its live
# edits), equal with another content hash keeps it and reports not_bumped, lower is ignored.
# `conflict`: the registry changed after the caller read it (the routing it carried over and its
# decision may be stale), so the caller reads again.
PUBLISH = (
    _COMMON
    + """
local version = tonumber(ARGV[2])
local stored_version = tonumber(redis.call('HGET', KEYS[2], 'version') or '0')
local stored_doc = redis.call('GET', KEYS[1])
local replace = ARGV[9] == '1' and stored_version == version
if stored_doc and stored_version >= version and not replace then
  -- The stored document is not sent back: no caller read it, and every pod boot (`kept`)
  -- shipped the whole registry back (CO-20).
  if stored_version > version then return {'ignored', stored_version, ''} end
  if redis.call('HGET', KEYS[2], 'config_hash') == ARGV[3] then
    return {'kept', stored_version, ''}
  end
  return {'not_bumped', stored_version, ''}
end
if tonumber(redis.call('HGET', KEYS[2], 'changes') or '0') ~= tonumber(ARGV[8]) then
  return {'conflict', stored_version, ''}
end
local changes = redis.call('HINCRBY', KEYS[2], 'changes', 1)
redis.call('SET', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'version', version, 'config_hash', ARGV[3], 'published', tostring(now))
redis.call('HSET', KEYS[4], tostring(version), ARGV[1])
local keep = tonumber(ARGV[7])
local versions = redis.call('HKEYS', KEYS[4])
if #versions > keep then
  table.sort(versions, function(a, b) return tonumber(a) < tonumber(b) end)
  for i = 1, #versions - keep do redis.call('HDEL', KEYS[4], versions[i]) end
end
audit(ARGV[4], 'publish', version, ARGV[5], ARGV[6])
local last = #KEYS - tonumber(ARGV[10])
write_models(5, last, 11, changes)
for i = last + 1, #KEYS do redis.call('DEL', KEYS[i]) end
return {'published', version, ''}
"""
)

# KEYS: doc, meta, audit, history, then numbers keys. ARGV: doc, expected changes, actor, diff
# JSON, audit retention ms, then numbers JSONs. Reply: {'ok', changes} or {'conflict', changes}
# when someone else changed the registry since it was read.
EDIT = (
    _COMMON
    + """
local changes = tonumber(redis.call('HGET', KEYS[2], 'changes') or '0')
if changes ~= tonumber(ARGV[2]) or not redis.call('GET', KEYS[1]) then
  return {'conflict', changes}
end
changes = redis.call('HINCRBY', KEYS[2], 'changes', 1)
redis.call('SET', KEYS[1], ARGV[1])
local version = redis.call('HGET', KEYS[2], 'version') or '0'
audit(ARGV[3], 'edit', version, ARGV[4], ARGV[5])
write_models(5, #KEYS, 6, changes)
return {'ok', changes}
"""
)
