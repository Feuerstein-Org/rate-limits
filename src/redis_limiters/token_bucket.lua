--- Lua scripts are run atomically by default, and since redis
--- is single threaded, there are no race conditions to worry about.
---
--- This script does three things, in order:
--- 1. Retrieves token bucket state, which means the last slot assigned,
---    and how many tokens are left to be assigned for that slot
--- 2. Works out whether we need to move to the next slot(s), or consume
---    tokens from the current one.
--- 3. Saves the token bucket state and returns the slot.
---
--- The token bucket implementation is forward looking, so we're really just handing
--- out the next time there would be tokens in the bucket, and letting the client
--- decide wait until that time.
---
--- returns:
--- * The assigned slot, as a millisecond timestamp

redis.replicate_commands()

-- Arguments
local capacity = tonumber(ARGV[1])
local refill_amount = tonumber(ARGV[2])
local initial_tokens = tonumber(ARGV[3])
local time_between_slots = tonumber(ARGV[4]) * 1000 -- Convert to milliseconds
local seconds = tonumber(ARGV[5])
local microseconds = tonumber(ARGV[6])
local expiry_seconds = tonumber(ARGV[7])
local tokens_to_consume = tonumber(ARGV[8]) -- Number of tokens to consume

-- Validate that tokens_to_consume doesn't exceed capacity
if tokens_to_consume > capacity then
    return redis.error_reply("Requested tokens exceed bucket capacity")
end

-- Validate that tokens_to_consume is positive
if tokens_to_consume <= 0 then
    return redis.error_reply("Must consume at least 1 token")
end

-- Keys
local data_key = KEYS[1]

-- Get current time in milliseconds
local now = (tonumber(seconds) * 1000) + (tonumber(microseconds) / 1000)

-- Default bucket values (used if no bucket exists yet)
local tokens = math.min(initial_tokens, capacity)
local slot = now

-- Retrieve stored state, if any
local data = redis.call('GET', data_key)
if data then
    local last_slot, stored_tokens = data:match('(%S+) (%S+)')
    slot = tonumber(last_slot)
    tokens = tonumber(stored_tokens)

    -- Calculate the number of slots that have passed since the last update
    local slots_passed = math.floor((now - slot) / time_between_slots)
    if slots_passed > 0 then
        -- Refill the tokens based on the number of slots passed, capped by capacity
        tokens = math.min(tokens + slots_passed * refill_amount, capacity)
        -- Update the slot to this run
        -- The previously added +20 ms execution penalty was removed as it was not needed
        -- and all it did was add additional latency to all requests and in our case,
        -- timing is handled gracefully with the condition used (wake_up_time < now)
        slot = now
    end
end

-- If not enough tokens are available, move to the next slot(s) and refill accordingly
if tokens < tokens_to_consume then
    -- Calculate how many additional tokens we need
    local needed_tokens = tokens_to_consume - tokens
    -- Calculate how many slots we need to move forward to get enough tokens
    local needed_slots = math.ceil(needed_tokens / refill_amount)
    slot = slot + needed_slots * time_between_slots
    tokens = tokens + needed_slots * refill_amount
    -- Clamp tokens to capacity
    tokens = math.min(tokens, capacity)
end

-- Consume tokens
tokens = tokens - tokens_to_consume

-- Save updated state and set expiry
redis.call('SETEX', data_key, expiry_seconds, string.format('%d %d', slot, tokens))

-- Return the slot when the next token(s) will be available
return slot
