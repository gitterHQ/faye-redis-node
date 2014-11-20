local messages_key = KEYS[1]
local message_count_key = KEYS[2]

local client_id = ARGV[1]

local result = {};

local messages = redis.call("LRANGE", messages_key, 0, -1);

local count = table.getn(messages)
local total
if count > 0 then
  redis.call("DEL", messages_key);
  total = tonumber(redis.call("ZINCRBY", message_count_key, count, client_id))
else
  total = 0
end

return { total, messages }
