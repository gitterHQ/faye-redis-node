-- Keys:   [/clients, /notifications/messages, /client/clientId/messages...]
-- Values: [cutoff, jsonMessage, clientIds...]

local clients_key = table.remove(KEYS, 1)
local message_channel_key = table.remove(KEYS, 1)

local cutoff = tonumber(table.remove(ARGV, 1))
local json_message = table.remove(ARGV, 1)

local result = {};

for i, client_message_key in ipairs(KEYS) do
  local client_id = ARGV[i];
      
  -- Ensure that the client cutoff has not been reached
  local client_cutoff = tonumber(redis.call("ZSCORE", clients_key, client_id));
  
  if client_cutoff > cutoff then
    redis.call("RPUSH", client_message_key, json_message);
    redis.call("PUBLISH", message_channel_key, client_id);
  else
    -- Notify caller of expired client, for removal
    table.insert(result, client_id)
  end

end

return result
