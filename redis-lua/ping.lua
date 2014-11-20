local clients_key = KEYS[1]

local client_id = ARGV[1]
local date = ARGV[2];
local cutoff = tonumber(ARGV[3]);

-- Ensure that the client cutoff has not been reached
local client_cutoff = tonumber(redis.call("ZSCORE", clients_key, client_id));

if (client_cutoff == nil) or (client_cutoff <= cutoff) then
  -- Client does not exist
  return 0
end

redis.call("ZADD", clients_key, date, client_id);
return 1
