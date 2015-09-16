var common = require('./common');
var createRedisClient = common.createRedisClient;
var extend = common.extend;
var redis = require('redis');
var Shavaluator = require('redis-evalsha');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var makeUuid = common.uuid;
var cpuCount = require('os').cpus().length;
var Pend = require('pend');
var path = require('path');
var JobWorker = require('./worker');
var spawn = require('child_process').spawn;

module.exports = JobQueue;

var childModulePath = path.join(__dirname, "child.js");

var queueDefaults = {
  namespace: "redis-dist-job-queue.",
  queueId: "default",
  redisConfig: {},
  workerCount: cpuCount,
  childProcessCount: 0,
  flushStaleTimeout: 30000,
};

var redisConfigDefaults = {
  port: 6379,
  host: "127.0.0.1",
  db: 0,
};

util.inherits(JobQueue, EventEmitter);
function JobQueue(options) {
  EventEmitter.call(this);

  options = extend(extend({}, queueDefaults), options || {});
  this.namespace = options.namespace;
  this.queueId = options.queueId;

  this.keyPrefix = this.namespace + this.queueId + ".";
  this.queueKey = this.keyPrefix + "queue";
  this.processingQueueKey = this.keyPrefix + "queue_processing";
  this.scratchQueueKey = this.keyPrefix + "queue_scratch";
  this.failedQueueKey = this.keyPrefix + "queue_failed";
  this.resourceKeyPrefix = this.keyPrefix + "lock.";
  this.flushStaleKey = this.keyPrefix + "flushStaleLock";
  this.flushStaleTimeout = options.flushStaleTimeout;
  this.workerCount = options.workerCount;
  this.childProcessCount = options.childProcessCount;
  this.redisConfig = extend(extend({}, redisConfigDefaults), options.redisConfig);
  this.modulePaths = [];
  this.redisClient = createRedisClient(this.redisConfig);
  this.childProcesses = [];
  this.childWorker = null;

  this.shavaluator = new Shavaluator();

  // KEYS[1] - pending job queue list
  // KEYS[2] - processing list
  // KEYS[3] - failed jobs list
  // KEYS[4] - scratch space list
  // KEYS[5] - flush stale timeout resource lock
  // ARGV[1] - the string to use for the flush stale timeout resource lock
  // ARGV[2] - number of milliseconds to hold the lock
  this.shavaluator.add('flushStale',
    // move list items one by one from source list to either scratch space
    // list or dest list.
    'local newFailedJobs = {}\n' +
    'if not redis.call("set", KEYS[5], ARGV[1], "NX", "PX", ARGV[2]) then\n' +
    '  return newFailedJobs\n' +
    'end\n' +
    'while true do\n' +
    '  local payload = redis.call("rpop", KEYS[2])\n' +
    '  if not payload then\n' +
    '    break\n' +
    '  end\n' +
    '  local json = cjson.decode(payload)\n' +
    '  local resourceId = json.resource\n' +
    '  local key = "' + this.resourceKeyPrefix + '" .. resourceId\n' +
    '  local ttl = redis.call("pttl", key)\n' +
    '  if ttl < 0 then\n' +
    '    local retries = json.retries or 0\n' +
    '    if retries > 0 then\n' +
    // we have retries left; move to end of pending queue
    '      retries = retries - 1\n' +
    '      json.retries = retries\n' +
    '      payload = cjson.encode(json)\n' +
    '      redis.call("lpush", KEYS[1], payload)\n' +
    '    else\n' +
    // ran out of retries, task is moved to failed list
    '      redis.call("lpush", KEYS[3], payload)\n' +
    '      table.insert(newFailedJobs, payload)\n' +
    '    end\n' +
    '  else\n' +
    '    redis.call("lpush", KEYS[4], payload)\n' +
    '  end\n' +
    'end\n' +
    // now we rename the scratch space list to the processing list
    'redis.pcall("rename", KEYS[4], KEYS[2])\n' +
    'return newFailedJobs\n');

  // KEYS[1] - source list
  // KEYS[2] - dest list
  this.shavaluator.add('moveAll',
      'while redis.call("rpoplpush",KEYS[1],KEYS[2]) do\n' +
      'end\n' +
      'return nil\n');
}

JobQueue.prototype.start = function() {
  var self = this;
  self.shuttingDown = false;
  if (self.childProcessCount > 0) {
    self.startChildProcesses();
  } else {
    self.startWorkers();
  }
  self.flushStaleInterval = setInterval(doFlushStaleJobs, self.flushStaleTimeout);
  doFlushStaleJobs();

  function doFlushStaleJobs() {
    self.flushStaleJobs(function(err) {
      if (err) {
        self.emit('error', err);
      }
    });
  }
};

JobQueue.prototype.forceFlushStaleJobs = function(callback) {
  var self = this;
  self.redisClient.send_command('del', [self.flushStaleKey], function(err) {
    if (err) return callback(err);
    self.flushStaleJobs(callback);
  });
};

JobQueue.prototype.flushStaleJobs = function(callback) {
  var self = this;
  self.shavaluator.execWithClient(self.redisClient, 'flushStale',
      [self.queueKey, self.processingQueueKey, self.failedQueueKey, self.scratchQueueKey,
      self.flushStaleKey], ["1", self.flushStaleTimeout],
      function(err, failedJobs)
  {
    if (err) {
      var newErr = new Error("flushStaleJobs failed");
      newErr.stack += "\n" + err.stack;
      return callback(newErr);
    }
    failedJobs.forEach(function(failedJob) {
      var job = JSON.parse(failedJob);
      job.errorStack = new Error("timed out").stack;
      self.emit('jobFail', job);
    });
    callback();
  });
};

JobQueue.prototype.startChildProcesses = function() {
  var self = this;
  var args = [
    childModulePath,
    JSON.stringify(self.serializeOptions()),
  ];
  var opts = {
    stdio: [process.stdin, process.stdout, process.stderr, 'ipc'],
  };
  for (var i = 0; i < self.childProcessCount; i += 1) {
    createChild();
  }

  function createChild() {
    var child = spawn(process.execPath, args, opts);
    self.childProcesses.push(child);
    child.on('exit', createOnExit(child));
    child.on('message', onMessage);
  }

  function createOnExit(child) {
    return function() {
      var index = self.childProcesses.indexOf(child);
      if (index >= 0) self.childProcesses.splice(index, 1);
      if (self.shuttingDown) return;
      self.emit('childRestart');
      createChild();
    };
  }

  function onMessage(msg) {
    if (self.shuttingDown) return;

    if (msg.type === 'error') {
      self.emit('error', new Error(msg.value));
    } else if (msg.type === 'jobFail') {
      self.emit('jobFail', msg.value);
    } else if (msg.type === 'jobSuccess') {
      self.emit('jobSuccess', msg.value);
    } else {
      throw new Error("unrecognized message type: " + msg.type);
    }
  }
};

JobQueue.prototype.serializeOptions = function() {
  return {
    namespace: this.namespace,
    queueId: this.queueId,
    workerCount: this.workerCount,
    redisConfig: this.redisConfig,
    modulePaths: this.modulePaths,
  };
};

JobQueue.prototype.startWorkers = function() {
  var self = this;
  self.childWorker = new JobWorker(self.serializeOptions());
  self.childWorker.on('error', function(err) {
    self.emit('error', err);
  });
  self.childWorker.on('jobFail', function(job) {
    self.emit('jobFail', job);
  });
  self.childWorker.on('jobSuccess', function(job) {
    self.emit('jobSuccess', job);
  });
  self.childWorker.start();
};

JobQueue.prototype.registerTask = function(modulePath) {
  var dirname = path.dirname(module.parent.filename);
  var modulePathAbs = require.resolve(path.resolve(dirname, modulePath));
  this.modulePaths.push(modulePathAbs);
};

// begin a process job. If the resource is already ongoing processing,
// nothing happens.
JobQueue.prototype.submitJob = function(taskId, options, cb) {
  options = options || {};
  if (typeof options === 'function') {
    cb = options;
    options = {};
  }

  var resourceId = options.resourceId || makeUuid();
  var params = options.params || null;
  var retries = parseInt(options.retries, 10) || 0;
  var json = JSON.stringify({
    type: taskId,
    resource: resourceId,
    params: params,
    retries: retries,
  });
  this.redisClient.send_command('lpush', [this.queueKey, json], function(err, result) {
    cb(err);
  });
};

JobQueue.prototype.shutdown = function(callback) {
  var self = this;

  self.shuttingDown = true;
  clearInterval(self.flushStaleInterval);

  var pend = new Pend();
  pend.go(function(cb) {
    self.redisClient.quit(function(err) {
        /* We want to kill this instance of the redis client, so at at this point we
         * are not interested in what connection errors Redis comes up with.
         */

        self.redisClient.end();
      cb();
    });
  });
  self.childProcesses.forEach(function(child) {
    pend.go(function(cb) {
      child.on('exit', cb);
      child.send('shutdown');
    });
  });
  if (self.childWorker) {
    pend.go(shutdownChildWorker);
  }
  pend.wait(function() {
    callback();
  });

  function shutdownChildWorker(cb) {
    self.childWorker.shutdown(cb);
  }
};

JobQueue.prototype.deleteFailedJobs = function(callback) {
  this.redisClient.send_command('del', [this.failedQueueKey], callback);
};

JobQueue.prototype.retryFailedJobs = function(callback) {
  this.shavaluator.execWithClient(this.redisClient, 'moveAll',
      [this.failedQueueKey, this.queueKey], [], callback);
}
