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
-- `join` and `check`. Append and ACK rely on the trusted-server contract.
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
if op == 'append' then
    -- ARGV: previous published entry sequence ('-1' before initial), first
    -- entry sequence, terminal flag ('1' when the last entry is terminal),
    -- then one or more contiguous encoded entries. Appends the longest prefix
    -- that fits under the queue byte limit and returns its length; 0 is the
    -- non-error FULL reply. Every check precedes every write.
    local previous, first, last_terminal = ARGV[2], ARGV[3], ARGV[4]
    local n = #ARGV - 4
    local s = redis.call('HMGET', state, 'poisoned', 'published', 'terminal', 'entry', 'bytes', 'limit')
    -- Poisoned first, so a failed attempt still stops a waiting publisher.
    if s[1] ~= '0' then return fail('poisoned') end
    if n < 1 or s[2] ~= previous or s[3] ~= '' then return fail('sequence') end
    local start = tonumber(first)
    local expected = previous == '-1' and 1 or tonumber(previous) + 1
    if not string.match(first, '^[1-9]%d*$') or start ~= expected then return fail('sequence') end
    -- An entry larger than the per-entry cap can never fit: fatal.
    local cap, sizes = tonumber(s[4]), {}
    for i=1,n do
        sizes[i] = string.len(ARGV[4 + i])
        if sizes[i] > cap then return fail('resource_limit') end
    end
    -- A full queue is not an error: append only the prefix that fits. The
    -- publisher keeps the remainder and retries it, never dropping data.
    local bytes, limit, k = tonumber(s[5]), tonumber(s[6]), 0
    while k < n and bytes + sizes[k + 1] <= limit do
        k = k + 1
        bytes = bytes + sizes[k]
    end
    if k == 0 then return 0 end
    local fields, total = {}, 0
    for i=1,k do
        local id = string.format('%d', start + i - 1)
        redis.call('XADD', stream, id .. '-0', 'record', ARGV[4 + i])
        fields[#fields + 1] = 'size:' .. id
        fields[#fields + 1] = sizes[i]
        total = total + sizes[i]
    end
    local last = string.format('%d', start + k - 1)
    fields[#fields + 1] = 'published'
    fields[#fields + 1] = last
    if k == n and last_terminal == '1' then
        fields[#fields + 1] = 'terminal'
        fields[#fields + 1] = last
    end
    redis.call('HINCRBY', state, 'bytes', total)
    redis.call('HSET', state, unpack(fields))
    return k
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
