var redis = require('redis');

exports.extend = extend;
exports.createRedisClient = createRedisClient;

var hasOwn = {}.hasOwnProperty;
function extend(target, source) {
  for (var propName in source) {
    if (hasOwn.call(source, propName)) {
      target[propName] = source[propName];
    }
  }
  return target;
}

function createRedisClient(config) {
  var port = config.port || 6379;
  var host = config.host || 'localhost';
  var db = (config.db == null) ? 0 : config.db;
  var client = redis.createClient(port, host, config);
  client.select(db);
  return client;
}
