local clients_key = KEYS[1]
local timeout_key = KEYS[2]

local client_id = ARGV[1]
local date = ARGV[2];
local default_timeout = ARGV[3];

local client_cutoff = tonumber(redis.call("ZSCORE", clients_key, client_id))

if (client_cutoff == nil) then
  -- Client does not exist
  return 0
end

-- Ensure that the client cutoff has not been reached
local client_timeout = tonumber(redis.call("ZSCORE", timeout_key, client_id))
if client_timeout == nil then
  client_timeout = default_timeout
end

local cutoff = date - 1000 * 1.6 * client_timeout;

if (client_cutoff <= cutoff) then
  -- Client does not exist
  return 0
end

redis.call("ZADD", clients_key, date, client_id);
return 1
