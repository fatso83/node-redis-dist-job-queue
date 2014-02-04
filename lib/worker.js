var common = require('./common');
var createRedisClient = common.createRedisClient;
var extend = common.extend;
var Shavaluator = require('redis-evalsha');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var makeUuid = require('uuid').v4;
var Pend = require('pend');

module.exports = Worker;

var shavaluator = new Shavaluator();

shavaluator.add('unlock',
    'if redis.call("get",KEYS[1]) == ARGV[1]\n' +
    'then\n' +
    '    redis.call("lrem",KEYS[2],1,ARGV[2])\n' +
    '    return redis.call("del",KEYS[1])\n' +
    'else\n' +
    '    return 0\n' +
    'end\n');

shavaluator.add('renew',
    'if redis.call("get",KEYS[1]) == ARGV[1]\n' +
    'then\n' +
    '    return redis.call("pexpire",KEYS[1],ARGV[2])\n' +
    'else\n' +
    '    return 0\n' +
    'end\n');

var taskDefaults = {
  // number of milliseconds since the last heartbeat to wait before
  // considering a job failed
  timeout: 10000,

  // perform: the actual task to do.
  perform: function(params, callback) {
    callback(new Error("implement this function"));
  },
};

util.inherits(Worker, EventEmitter);
function Worker(options) {
  EventEmitter.call(this);

  this.namespace = options.namespace;
  this.queueId = options.queueId;

  this.queueKey = this.namespace + "queue." + this.queueId;
  this.processingQueueKey = this.namespace + "queue_processing." + this.queueId;
  this.workerCount = options.workerCount;
  this.redisConfig = options.redisConfig;
  this.tasks = {};
  
  for (var i = 0; i < options.modulePaths.length; i += 1) {
    var taskModule = require(options.modulePaths[i]);
    this.tasks[taskModule.id] = extend(extend({}, taskDefaults), taskModule);
  }
}

Worker.prototype.start = function() {
  this.shuttingDown = false;
  this.pend = new Pend();
  this.blockingRedisClients = [];
  this.redisClientStatus = [];
  for (var i = 0; i < this.workerCount; i += 1) {
    this.spawnWorker();
  }
}

Worker.prototype.shutdown = function(callback) {
  var self = this;
  self.shuttingDown = true;
  self.blockingRedisClients.forEach(function (info) {
    if (info.blocking) {
      info.redisClient.end();
    }
  });
  self.pend.wait(function() {
    callback();
  });
};

Worker.prototype.spawnWorker = function() {
  var self = this;

  // create new client because blpop blocks the connection
  var redisClient = createRedisClient(self.redisConfig);
  var clientObj = {
    redisClient: redisClient,
    blocking: false,
  };
  self.blockingRedisClients.push(clientObj);

  handleOne();

  function handleOne() {
    if (self.shuttingDown) return;
    self.processOneItem(clientObj, handleOne);
  }
};

Worker.prototype.processOneItem = function(clientObj, cb) {
  var self = this;

  var redisClient = clientObj.redisClient;
  clientObj.blocking = true;
  redisClient.send_command('brpoplpush', [self.queueKey, self.processingQueueKey, 0], onResult);

  function onResult(err, jobListValue) {
    // so later we can do pend.wait and get a callback when
    // shutting down
    clientObj.blocking = false;
    self.pend.go(function(pendCb) {
      goForIt(err, jobListValue, function() {
        pendCb();
        cb();
      });
    });
  }

  function goForIt(err, jobListValue, cb) {
    if (err) {
      self.emit('error', err);
      return cb();
    }

    var obj;
    try {
      obj = JSON.parse(jobListValue);
    } catch (err) {
      self.emit('error', err);
      return cb();
    }

    var task = self.tasks[obj.type];
    if (!task) {
      self.emit('error', new Error("unregistered task: " + obj.type));
      return cb();
    }

    var heartbeatTimeout = Math.floor(task.timeout / 2);
    if (isNaN(heartbeatTimeout)) {
      self.emit('error', new Error("invalid heartbeat timeout"));
      return cb();
    }

    var resourceKey = self.namespace + "lock." + obj.resource;
    var jobInstanceId = makeUuid();
    var args = [resourceKey, jobInstanceId, 'NX', 'PX', task.timeout];
    redisClient.send_command('set', args, onSetResult);
    
    function onSetResult(err, result) {
      if (err) {
        self.emit('error', err);
        return cb();
      }

      var interval = null;

      // If we fail to acquire the lock, it means that this resource is
      // already being processed, no need to continue.
      if (!result) return onComplete();

      // start the heart beating. This prevents the job from timing out
      // while it is still running.
      interval = setInterval(doHeartBeat, heartbeatTimeout);

      // now actually do the thing
      task.perform(obj.params, onComplete);

      function doHeartBeat() {
        shavaluator.execWithClient(redisClient, 'renew',
          [resourceKey], [jobInstanceId, task.timeout], function(err, result)
        {
          if (err) {
            self.emit('error', err);
          }
          if (err || !result) {
            if (interval) clearInterval(interval);
            interval = null;
          }
        });
      }

      function onComplete(err) {
        if (err) {
          self.emit('error', err);
          // no return; we still want to delete the lock
        }
        if (interval) clearInterval(interval);
        shavaluator.execWithClient(redisClient, 'unlock',
          [resourceKey, self.processingQueueKey],
          [jobInstanceId, jobListValue], function(err, result)
        {
          if (err) {
            self.emit('error', err);
          }
          cb();
        });
      }
    }
  }
};
