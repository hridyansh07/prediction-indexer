-- Dedicated disposable keys only. No script retries: Redis scripts do not roll
-- back a prefix on command failure. The caller poisons on every error/ambiguity.
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
if redis.call('HGET', state, 'poisoned') ~= '0' then return fail('poisoned') end
local expected = cjson.decode(redis.call('HGET', state, 'groups') or '[]')
local actual = redis.call('XINFO', 'GROUPS', stream)
if #actual ~= #expected then return fail('membership') end
local names = {}
for _, fields in ipairs(actual) do
    for i=1,#fields,2 do if fields[i] == 'name' then names[fields[i+1]] = true end end
end
for _, group in ipairs(expected) do
    if not names[group] then return fail('membership') end
    local consumers = redis.call('XINFO', 'CONSUMERS', stream, group)
    if #consumers ~= 1 or consumers[1][2] ~= 'worker' then return fail('membership') end
end
if op == 'check' then return redis.call('HGETALL', state) end
if op == 'join' then
    if redis.call('HGET', state, 'joined:' .. ARGV[2]) ~= '0' then return fail('already_joined') end
    redis.call('HSET', state, 'joined:' .. ARGV[2], '1')
    return 'OK'
end
if op == 'publish' then
    if redis.call('HGET', state, 'published') ~= ARGV[2] or redis.call('HGET', state, 'terminal') ~= '' then return fail('sequence') end
    local size = string.len(ARGV[4])
    if size > tonumber(redis.call('HGET', state, 'entry')) or size + tonumber(redis.call('HGET', state, 'bytes')) > tonumber(redis.call('HGET', state, 'limit')) then return fail('resource_limit') end
    redis.call('XADD', stream, ARGV[3] .. '-0', 'record', ARGV[4])
    redis.call('HINCRBY', state, 'bytes', size)
    redis.call('HSET', state, 'published', ARGV[3])
    if ARGV[5] == '1' then redis.call('HSET', state, 'terminal', ARGV[3]) end
    return 'OK'
end
if op == 'ack' then
    local group, previous, seq, id = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
    if redis.call('HGET', state, 'done:' .. group) ~= previous then return fail('sequence') end
    local pending = redis.call('XPENDING', stream, group, id, id, 1)
    local entries = redis.call('XRANGE', stream, id, id)
    if #pending ~= 1 or pending[1][2] ~= 'worker' or #entries ~= 1 then return fail('not_pending') end
    local result = redis.call('XACKDEL', stream, group, 'ACKED', 'IDS', 1, id)
    if result[1] ~= 1 and result[1] ~= 2 then return fail('ack_failed') end
    if result[1] == 1 then redis.call('HINCRBY', state, 'bytes', -string.len(entries[1][2][2])) end
    redis.call('HSET', state, 'done:' .. group, seq)
    return result[1]
end
return fail('unknown_operation')
