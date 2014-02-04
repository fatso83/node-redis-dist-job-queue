var redis = require('redis');
var Shavaluator = require('redis-evalsha');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var makeUuid = require('uuid').v4;
var cpuCount = require('os').cpus().length;
var Pend = require('pend');

module.exports = JobQueue;

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

shavaluator.add('moveAll',
    'while redis.call("rpoplpush",KEYS[1],KEYS[2]) do\n' +
    'end\n' +
    'return nil\n');

var queueDefaults = {
  namespace: "redis-dist-job-queue.",
  queueId: "default",
  workerCount: cpuCount,
  redisConfig: {},
  flushStaleTimeout: 60000,
};

var redisConfigDefaults = {
  port: 6379,
  host: "127.0.0.1",
  db: 1,
};

util.inherits(JobQueue, EventEmitter);
function JobQueue(options) {
  EventEmitter.call(this);

  options = extend(extend({}, queueDefaults), options || {});
  this.namespace = options.namespace;
  this.queueId = options.queueId;

  this.queueKey = this.namespace + "queue." + this.queueId;
  this.processingQueueKey = this.namespace + "queue_processing." + this.queueId;
  this.workerCount = options.workerCount;
  this.redisConfig = extend(extend({}, redisConfigDefaults), options.redisConfig);
  this.tasks = {};
  this.flushStaleTimeout = options.flushStaleTimeout;
  this.redisClient = createRedisClient(this.redisConfig);
}

JobQueue.prototype.start = function() {
  this.shuttingDown = false;
  this.pend = new Pend();
  this.blockingRedisClients = [];
  this.flushStaleInterval = setInterval(this.flushStaleJobs.bind(this), this.flushStaleTimeout);
  for (var i = 0; i < this.workerCount; i += 1) {
    this.spawnWorker();
  }
  this.flushStaleJobs();
}

var taskDefaults = {
  // number of milliseconds since the last heartbeat to wait before
  // considering a job failed
  timeout: 10000,

  // perform: the actual task to do.
  perform: function(params, callback) {
    callback(new Error("implement this function"));
  },
};

JobQueue.prototype.registerTask = function(taskId, task) {
  this.tasks[taskId] = extend(extend({}, taskDefaults), task);
};

// begin a process job. If the resource is already ongoing processing, nothing happens.
JobQueue.prototype.submitJob = function(taskId, resourceId, params, cb) {
  var json = JSON.stringify({
    type: taskId,
    resource: resourceId,
    params: params,
  });
  this.redisClient.send_command('lpush', [this.queueKey, json], function(err, result) {
    cb(err);
  });
};

JobQueue.prototype.shutdown = function(callback) {
  var self = this;
  self.shuttingDown = true;
  clearInterval(self.flushStaleInterval);
  self.pend.wait(function() {
    self.blockingRedisClients.forEach(function (redisClient) {
      redisClient.end();
    });
    self.redisClient.quit(function(err, result) {
      if (err) {
        self.emit('error', err);
      }
      callback();
    });
  });
};

JobQueue.prototype.flushStaleJobs = function() {
  var self = this;
  shavaluator.execWithClient(self.redisClient, "moveAll",
      [self.processingQueueKey, self.queueKey], [], function(err, result)
  {
    if (err) {
      self.emit('error', err);
    }
    if (result) {
      self.emit('error', new Error("unable to flush stale jobs"));
    }
  });
};

JobQueue.prototype.spawnWorker = function() {
  var self = this;

  // create new client because blpop blocks the connection
  var redisClient = createRedisClient(self.redisConfig);
  self.blockingRedisClients.push(redisClient);

  handleOne();

  function handleOne() {
    if (self.shuttingDown) return;
    self.processOneItem(redisClient, handleOne);
  }
};

JobQueue.prototype.processOneItem = function(redisClient, cb) {
  var self = this;

  redisClient.send_command('brpoplpush', [self.queueKey, self.processingQueueKey, 0], onResult);

  function onResult(err, jobListValue) {
    // so later we can do pend.wait and get a callback when
    // shutting down
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

    var job = self.tasks[obj.type];
    if (!job) {
      self.emit('error', new Error("unregistered task: " + obj.type));
      return cb();
    }

    var heartbeatTimeout = Math.floor(job.timeout / 2);
    if (isNaN(heartbeatTimeout)) {
      self.emit('error', new Error("invalid heartbeat timeout"));
      return cb();
    }

    var resourceKey = self.namespace + "lock." + obj.resource;
    var jobInstanceId = makeUuid();
    var args = [resourceKey, jobInstanceId, 'NX', 'PX', job.timeout];
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
      job.perform(obj.params, onComplete);

      function doHeartBeat() {
        shavaluator.execWithClient(redisClient, 'renew',
          [resourceKey], [jobInstanceId, job.timeout], function(err, result)
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
  var client = redis.createClient(config.port, config.host, config);
  client.select(config.db);
  return client;
}
