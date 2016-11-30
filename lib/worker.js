var common = require('./common');
var createRedisClient = common.createRedisClient;
var extend = common.extend;
var Shavaluator = require('redis-evalsha');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var makeUuid = common.uuid;
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

// KEYS[1] - the resource
// KEYS[2] - the pending jobs queue
// KEYS[3] - the processing jobs queue
// KEYS[4] - the failed jobs queue
// ARGV[1] - the job instance id
// ARGV[2] - the job payload
shavaluator.add('unlockMove',
    'if redis.call("get", KEYS[1]) == ARGV[1]\n' +
    'then\n' +
    '  local payload = ARGV[2]\n' +
    '  redis.call("lrem", KEYS[3], 1, payload)\n' +
    '  local json = cjson.decode(payload)\n' +
    '  local retries = json.retries or 0\n' +
    '  if retries > 0 then\n' +
    // we have more retries; put it back in the pending queue
    '    retries = retries - 1\n' +
    '    json.retries = retries\n' +
    '    payload = cjson.encode(json)\n' +
    '    redis.call("lpush", KEYS[2], payload)\n' +
    '  else\n' +
    // we have run out of retries; it goes into the failed jobs queue
    '    redis.call("lpush", KEYS[4], payload)\n' +
    '  end\n' +
    '  return redis.call("del",KEYS[1])\n' +
    'else\n' +
    '  return 0\n' +
    'end\n');

shavaluator.add('renew',
    'if redis.call("get",KEYS[1]) == ARGV[1]\n' +
    'then\n' +
    '    return redis.call("pexpire",KEYS[1],ARGV[2])\n' +
    'else\n' +
    '    return 0\n' +
    'end\n');

// KEYS[1] - source list
// KEYS[2] - dest list
// ARGV[1] - the list value to move
shavaluator.add('move',
    'redis.call("lrem",KEYS[1],1,ARGV[1])\n' +
    'redis.call("lpush",KEYS[2],ARGV[1])\n');

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

  this.keyPrefix = this.namespace + this.queueId + ".";
  this.queueKey = this.keyPrefix + "queue";
  this.processingQueueKey = this.keyPrefix + "queue_processing";
  this.failedQueueKey = this.keyPrefix + "queue_failed";
  this.resourceKeyPrefix = this.keyPrefix + "lock.";
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
  var workerInternalRedisClient = createRedisClient(self.redisConfig);
  var clientObj = {
    redisClient: workerInternalRedisClient,
    blocking: false,
  };
  self.blockingRedisClients.push(clientObj);

  // avoid uncaught errors from the worker internal redis client killing the Node process
  workerInternalRedisClient.on('error', self.emit.bind(self,'error'));

  handleOne();

  function handleOne() {
    if (self.shuttingDown) return;
    self.processOneItem(clientObj, handleOne);
  }
};

Worker.prototype.processOneItem = function(clientObj, nextJob) {
  var self = this;

  var redisClient = clientObj.redisClient;
  clientObj.blocking = true;

  // this command will trigger the callback synchronously if the connection is down
  redisClient.send_command('brpoplpush', [self.queueKey, self.processingQueueKey, 0], onResult);

  function onResult(err, jobListValue) {
    // so later we can do pend.wait and get a callback when
    // shutting down
    clientObj.blocking = false;
    self.pend.go(function(pendCb) {
      goForIt(err, jobListValue, function() {
        pendCb();

        // allow Node to process the event loop before initiating the next job
        // this remedies a situation where we create a non-ending (synchronous) loop on connection down
        process.nextTick(nextJob);
      });
    });
  }

  function goForIt(err, jobListValue, cb) {
    if (err) {
      self.emit('error', err);
      return cb();
    }

    var job;
    try {
      job = JSON.parse(jobListValue);
    } catch (err) {
      self.emit('error', err);
      return cb();
    }

    var task = self.tasks[job.type];
    if (!task) {
      self.emit('error', new Error("unregistered task: " + job.type));
      return cb();
    }

    var heartbeatTimeout = Math.floor(task.timeout / 2);
    if (isNaN(heartbeatTimeout)) {
      self.emit('error', new Error("invalid heartbeat timeout"));
      return cb();
    }

    var resourceKey = self.resourceKeyPrefix + job.resource;
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
      // already being processed. We move the item from the processing
      // queue to the end of the waiting queue and then GTFO
      if (!result) {
        shavaluator.execWithClient(redisClient, 'move',
            [self.processingQueueKey, self.queueKey], [jobListValue],
            function(err, result)
        {
          if (err) {
            self.emit('error', err);
          }
          cb();
        });
        return;
      }

      // start the heart beating. This prevents the job from timing out
      // while it is still running.
      interval = setInterval(doHeartBeat, heartbeatTimeout);

      // emit an error if the user calls the callback twice
      var callbackCalled = false;

      // now actually do the thing
      task.perform(job.params, onComplete);

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
        if (callbackCalled) {
          self.emit('error', new Error(job.type + ": callback called twice"));
          return;
        }
        callbackCalled = true;

        if (interval) clearInterval(interval);
        if (err) {
          if (!(err instanceof Error)) {
            err = new Error("task returned non error object");
          }
          job.errorStack = err.stack;
          shavaluator.execWithClient(redisClient, 'unlockMove',
            [resourceKey, self.queueKey, self.processingQueueKey, self.failedQueueKey],
            [jobInstanceId, jobListValue], finishFail);
        } else {
          shavaluator.execWithClient(redisClient, 'unlock',
            [resourceKey, self.processingQueueKey],
            [jobInstanceId, jobListValue], finishSuccess);
        }

        function finishFail(err) {
          self.emit('jobFail', job);
          if (err) {
            self.emit('error', err);
          }
          cb();
        }

        function finishSuccess(err) {
          self.emit('jobSuccess', job);
          if (err) {
            self.emit('error', err);
          }
          cb();
        }
      }
    }
  }
};
