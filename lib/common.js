var redis = require('redis');
var crypto = require('crypto');

var htmlSafe = {'/': '_', '+': '-'};
var hasOwn = {}.hasOwnProperty;

exports.extend = extend;
exports.createRedisClient = createRedisClient;
exports.uuid = uuid;

function extend(target, source) {
  for (var propName in source) {
    if (hasOwn.call(source, propName)) {
      target[propName] = source[propName];
    }
  }
  return target;
}

function createRedisClient(config) {
  var client = redis.createClient(config.port, config.host, config);
  if (config.db != null) {
    client.select(config.db);
  }
  return client;
}

function uuid() {
  return rando(24).toString('base64').replace(/[\/\+]/g, function(x) {
    return htmlSafe[x];
  });
}

function rando(size) {
  try {
    return crypto.randomBytes(size);
  } catch (err) {
    return crypto.pseudoRandomBytes(size);
  }
}
