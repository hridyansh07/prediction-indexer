-- Dedicated disposable keys only. No script retries: Redis scripts do not roll
-- back a prefix on command failure. The caller poisons on every error/ambiguity.
-- Every published entry's payload size lives in the state hash as
-- `size:<redis entry sequence>` until the entry is deleted from the stream, so
-- deleting the two attempt keys still removes all attempt state.
local stream, state = KEYS[1], KEYS[2]
local op = ARGV[1]
local function fail(message) return redis.error_reply('REPLAY ' .. message) end
if op == 'setup' then
    if redis.call('EXISTS', stream, state) ~= 0 then return fail('scope_exists') end
    local groups = cjson.decode(ARGV[2])
    redis.call('HSET', state, 'groups', ARGV[2], 'published', '-1', 'terminal', '', 'poisoned', '0', 'bytes', '0', 'limit', ARGV[3], 'entry', ARGV[4])
    for _, group in ipairs(groups) do
        redis.call('XGROUP', 'CREATE', stream, group, '0', 'MKSTREAM')
        redis.call('XGROUP', 'CREATECONSUMER', stream, group, 'worker')
        redis.call('HSET', state, 'done:' .. group, '-1', 'joined:' .. group, '0')
    end
    return 'OK'
end
-- Fixed membership is verified only where a participant (re)enters or polls:
-- `join` and `check`. Publish and ACK rely on the trusted-server contract.
local function membership(expected)
    local actual = redis.call('XINFO', 'GROUPS', stream)
    if #actual ~= #expected then return false end
    local names = {}
    for _, fields in ipairs(actual) do
        for i=1,#fields,2 do if fields[i] == 'name' then names[fields[i+1]] = true end end
    end
    for _, group in ipairs(expected) do
        if not names[group] then return false end
        local consumers = redis.call('XINFO', 'CONSUMERS', stream, group)
        if #consumers ~= 1 or consumers[1][2] ~= 'worker' then return false end
    end
    return true
end
if op == 'publish' then
    local s = redis.call('HMGET', state, 'poisoned', 'published', 'terminal', 'entry', 'bytes', 'limit')
    if s[1] ~= '0' then return fail('poisoned') end
    if s[2] ~= ARGV[2] or s[3] ~= '' then return fail('sequence') end
    local size = string.len(ARGV[4])
    -- An entry larger than the per-entry cap can never fit: fatal.
    if size > tonumber(s[4]) then return fail('resource_limit') end
    -- A full queue is not an error. Nothing is written; the publisher keeps the
    -- same sequence and entry and retries after a bounded backoff. Poisoned is
    -- checked first above, so a failed attempt still stops a waiting publisher.
    if size + tonumber(s[5]) > tonumber(s[6]) then return 'FULL' end
    redis.call('XADD', stream, ARGV[3] .. '-0', 'record', ARGV[4])
    redis.call('HINCRBY', state, 'bytes', size)
    if ARGV[5] == '1' then
        redis.call('HSET', state, 'published', ARGV[3], 'size:' .. ARGV[3], size, 'terminal', ARGV[3])
    else
        redis.call('HSET', state, 'published', ARGV[3], 'size:' .. ARGV[3], size)
    end
    return 'OK'
end
if op == 'ack' then
    -- ARGV: group, previous done, last sequence, then ascending entry IDs.
    local group, previous, last = ARGV[2], ARGV[3], ARGV[4]
    local n = #ARGV - 4
    local s = redis.call('HMGET', state, 'poisoned', 'done:' .. group)
    if s[1] ~= '0' then return fail('poisoned') end
    if n < 1 or s[2] ~= previous then return fail('sequence') end
    local ids, sizes = {}, {}
    for i=1,n do
        ids[i] = ARGV[4 + i]
        local seq = string.match(ids[i], '^(%d+)%-0$')
        if not seq then return fail('not_pending') end
        sizes[i] = 'size:' .. seq
    end
    local pending = redis.call('XPENDING', stream, group, ids[1], ids[n], n, 'worker')
    if #pending ~= n then return fail('not_pending') end
    for i=1,n do
        if pending[i][1] ~= ids[i] or pending[i][2] ~= 'worker' then return fail('not_pending') end
    end
    local stored = redis.call('HMGET', state, unpack(sizes))
    for i=1,n do if not stored[i] then return fail('not_pending') end end
    local result = redis.call('XACKDEL', stream, group, 'ACKED', 'IDS', n, unpack(ids))
    local freed, deleted = 0, {}
    for i=1,n do
        if result[i] == 1 then
            freed = freed + tonumber(stored[i])
            deleted[#deleted + 1] = sizes[i]
        elseif result[i] ~= 2 then
            return fail('ack_failed')
        end
    end
    if #deleted > 0 then
        redis.call('HINCRBY', state, 'bytes', -freed)
        redis.call('HDEL', state, unpack(deleted))
    end
    redis.call('HSET', state, 'done:' .. group, last)
    return n
end
local status = redis.call('HMGET', state, 'poisoned', 'groups', 'joined:' .. (ARGV[2] or ''))
if status[1] ~= '0' then return fail('poisoned') end
local expected = cjson.decode(status[2] or '[]')
if not membership(expected) then return fail('membership') end
if op == 'check' then
    -- Fixed progress fields only; per-entry size fields are bookkeeping.
    local fields = {'groups', 'published', 'terminal', 'poisoned', 'bytes', 'limit', 'entry'}
    for _, group in ipairs(expected) do
        fields[#fields + 1] = 'done:' .. group
        fields[#fields + 1] = 'joined:' .. group
    end
    local values = redis.call('HMGET', state, unpack(fields))
    local result = {}
    for i=1,#fields do
        if values[i] then
            result[#result + 1] = fields[i]
            result[#result + 1] = values[i]
        end
    end
    return result
end
if op == 'join' then
    if status[3] ~= '0' then return fail('already_joined') end
    redis.call('HSET', state, 'joined:' .. ARGV[2], '1')
    return 'OK'
end
return fail('unknown_operation')
