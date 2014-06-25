var redis = require('redis');

exports.extend = extend;
exports.createRedisClient = createRedisClient;

var hasOwn = {}.hasOwnProperty;
function extend(target, source) {
  for (var propName in source) {
    if (hasOwn.call(source, propName) && source[propName] != null) {
      target[propName] = source[propName];
    }
  }
  return target;
}

function createRedisClient(config) {
  var client = redis.createClient(config.port, config.host, config);
  client.select(config.db);
  return client;
}
