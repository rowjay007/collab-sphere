package store

import "github.com/redis/go-redis/v9"

const luaPrelude = `
local function encode_varint(n)
  n = math.floor(tonumber(n))
  if n < 0 then
    error('neg')
  end
  local parts = {}
  repeat
    local b = n % 128
    n = math.floor(n / 128)
    if n > 0 then
      b = b + 128
    end
    parts[#parts + 1] = string.char(b)
  until n == 0
  return table.concat(parts)
end

local function decode_varint(buf, i)
  local n = 0
  local shift = 0
  while true do
    if i > #buf then
      error('short varint')
    end
    local b = string.byte(buf, i)
    i = i + 1
    n = n + (b % 128) * (2 ^ shift)
    if b < 128 then
      return math.floor(n), i
    end
    shift = shift + 7
  end
end

local function skip_varint(buf, i)
  while i <= #buf do
    local b = string.byte(buf, i)
    i = i + 1
    if b < 128 then
      return i
    end
  end
  error('short varint')
end

local function field_varint_bytes(buf, want)
  local i = 1
  while i <= #buf do
    local tag, ni = decode_varint(buf, i)
    i = ni
    local field = math.floor(tag / 8)
    local wire = tag % 8
    if wire == 0 then
      local start = i
      i = skip_varint(buf, i)
      if field == want then
        return string.sub(buf, start, i - 1)
      end
    elseif wire == 2 then
      local len, nj = decode_varint(buf, i)
      i = nj + len
    else
      error('wire')
    end
  end
  return ''
end

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function next_version(key, now)
  local stored = tonumber(redis.call('GET', key)) or 0
  local ver = math.max(stored + 1, now * 1000)
  redis.call('SET', key, ver)
  return ver
end

local function encode_key(field, wire)
  return encode_varint(field * 8 + wire)
end

local function encode_string(field, s)
  if s == nil or s == '' then
    return ''
  end
  return encode_key(field, 2) .. encode_varint(#s) .. s
end

local function encode_uint64(field, n)
  n = tonumber(n)
  if n == 0 then
    return ''
  end
  return encode_key(field, 0) .. encode_varint(n)
end

local function encode_uint64_bytes(field, raw)
  if raw == nil or raw == '' or raw == string.char(0) then
    return ''
  end
  return encode_key(field, 0) .. raw
end

local function encode_bytes(field, s)
  if s == nil or s == '' then
    return ''
  end
  return encode_key(field, 2) .. encode_varint(#s) .. s
end

local function hex_encode(raw)
  return (string.gsub(raw, '.', function(c)
    return string.format('%02x', string.byte(c))
  end))
end

local function hex_decode(encoded)
  return (string.gsub(encoded, '..', function(pair)
    return string.char(tonumber(pair, 16))
  end))
end

local function field_varint(buf, want)
  local raw = field_varint_bytes(buf, want)
  if raw == '' then
    return 0
  end
  local n = decode_varint(raw, 1)
  return n
end

local function stamp_state(raw, ver)
  return raw .. encode_uint64(7, ver)
end

local function event_state(doc, client_varint, ver, state)
  return encode_string(1, doc) .. encode_uint64_bytes(2, client_varint) .. encode_uint64(3, ver) .. encode_bytes(4, state)
end

local function event_left(doc, client_varint, ver, session_varint, reason)
  local left = ''
  if session_varint ~= nil and session_varint ~= '' then
    left = encode_key(1, 0) .. session_varint
  end
  left = left .. encode_uint64(2, reason)
  return encode_string(1, doc) .. encode_uint64_bytes(2, client_varint) .. encode_uint64(3, ver) .. encode_bytes(5, left)
end
`

var joinUpsertScript = redis.NewScript(luaPrelude + `
local raw = redis.call('GET', KEYS[1])
if raw then
  local same_session = field_varint_bytes(raw, 3) == ARGV[9]
  local stored_epoch = tonumber(field_varint(raw, 9))
  local new_epoch = tonumber(ARGV[10])
  if not same_session and new_epoch <= stored_epoch and not (new_epoch == 0 and stored_epoch == 0) then
    return {2, 0}
  end
end
local now = now_ms()
local ver = next_version(KEYS[4], now)
local state = stamp_state(ARGV[1], ver)
redis.call('SET', KEYS[1], state, 'PX', tonumber(ARGV[8]))
redis.call('SADD', KEYS[2], ARGV[3])
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[2]), ARGV[4])
redis.call('PUBLISH', ARGV[7], event_state(ARGV[5], ARGV[6], ver, state))
return {1, ver}
`)

var applyUpsertScript = redis.NewScript(luaPrelude + `
local raw = redis.call('GET', KEYS[1])
if raw and field_varint_bytes(raw, 3) ~= ARGV[9] then
  return {2, 0}
end
local now = now_ms()
local ver = next_version(KEYS[4], now)
local state = stamp_state(ARGV[1], ver)
redis.call('SET', KEYS[1], state, 'PX', tonumber(ARGV[8]))
redis.call('SADD', KEYS[2], ARGV[3])
redis.call('ZADD', KEYS[3], now + tonumber(ARGV[2]), ARGV[4])
redis.call('PUBLISH', ARGV[7], event_state(ARGV[5], ARGV[6], ver, state))
return {1, ver}
`)

var touchScript = redis.NewScript(luaPrelude + `
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
if field_varint_bytes(raw, 3) ~= ARGV[1] then
  return 2
end
local now = now_ms()
local score = redis.call('ZSCORE', KEYS[2], ARGV[3])
if not score or tonumber(score) <= now then
  return 0
end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]))
redis.call('ZADD', KEYS[2], now + tonumber(ARGV[2]), ARGV[3])
return 1
`)

var removeScript = redis.NewScript(luaPrelude + `
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
if field_varint_bytes(raw, 3) ~= ARGV[1] then
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[3])
local ver = next_version(KEYS[4], now_ms())
redis.call('PUBLISH', ARGV[6], event_left(ARGV[4], ARGV[5], ver, ARGV[1], 1))
return ver
`)

var sweepScript = redis.NewScript(luaPrelude + `
local now = now_ms()
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, ARGV[1])
local n = 0
for _, member in ipairs(members) do
  if redis.call('ZREM', KEYS[1], member) == 1 then
    local first_sep = string.find(member, '|', 1, true)
    local second_sep = string.find(member, '|', first_sep + 1, true)
    if second_sep then
      local doc = string.sub(member, 1, first_sep - 1)
      local client = string.sub(member, first_sep + 1, second_sep - 1)
      local client_varint = hex_decode(string.sub(member, second_sep + 1))
      local skey = 'presence:' .. doc .. ':' .. client
      local raw = redis.call('GET', skey)
      local session = ''
      if raw then
        session = field_varint_bytes(raw, 3)
      end
      redis.call('DEL', skey)
      redis.call('SREM', 'presence-members:' .. doc, client)
      local ver = next_version('presence-ver:' .. doc, now)
      redis.call('PUBLISH', 'presence-events:' .. doc, event_left(doc, client_varint, ver, session, 2))
      n = n + 1
    end
  end
end
return n
`)

var listScript = redis.NewScript(luaPrelude + `
local members = redis.call('SMEMBERS', KEYS[1])
local ver = redis.call('GET', KEYS[3])
if not ver then
  ver = '0'
end
local now = now_ms()
local out = {ver}
for _, client in ipairs(members) do
  local raw = redis.call('GET', 'presence:' .. ARGV[1] .. ':' .. client)
  if raw then
    local client_varint = field_varint_bytes(raw, 2)
    local member = ARGV[1] .. '|' .. client .. '|' .. hex_encode(client_varint)
    local score = redis.call('ZSCORE', KEYS[2], member)
    if score and tonumber(score) > now then
      out[#out + 1] = raw
    end
  end
end
return out
`)
