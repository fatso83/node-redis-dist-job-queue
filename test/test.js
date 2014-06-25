var JobQueue = require('..');
var assert = require('assert');
var path = require('path');
var shared = require('./shared');
var fs = require('fs');
var redis = require('redis');

var describe = global.describe;
var it = global.it;
var before = global.before;
var after = global.after;
var beforeEach = global.beforeEach;

var hitCountFile = path.resolve(__dirname, "hitCount.txt");
var twiceFile = path.resolve(__dirname, "twice.txt");

describe("JobQueue", function() {
  beforeEach(function(done) {
    var redisClient = redis.createClient();
    redisClient.send_command('flushdb', [], done);
  });

  after(function(done) {
    fs.unlink(hitCountFile, function(err) {
      done();
    });
  });

  before(tryUnlickTwiceFile);
  after(tryUnlickTwiceFile);

  function tryUnlickTwiceFile(done) {
    fs.unlink(twiceFile, function(err) {
      done();
    });
  }

  it("processes a thing and then shuts down - in process", function(done) {
    var jobQueue = new JobQueue({ workerCount: 8, redisConfig: {host: 'localhost'} });
    shared.processedString = null;
    shared.callback = function(params) {
      assert.strictEqual(params, null);
      jobQueue.shutdown(function() {
        assert.strictEqual(shared.processedString, "DERP");
        done();
      });
    };

    jobQueue.registerTask('./task1');
    jobQueue.registerTask('./task_cb');

    jobQueue.start();

    jobQueue.submitJob('thisIsMyTaskId', {params: {theString: "derp"}}, assert.ifError);
    jobQueue.submitJob('callback', assert.ifError);
  });

  it("processes a thing and then shuts down - child processes", function(done) {
    var jobQueue = new JobQueue({ workerCount: 1, childProcessCount: 8 });

    fs.writeFileSync(hitCountFile, "0");

    jobQueue.registerTask('./task_hit_count');

    jobQueue.start();

    var options = {
      resourceId: 'hit_count',
      params: hitCountFile,
    };
    var count = 20;
    for (var i = 0; i < count; i += 1) {
      jobQueue.submitJob('hitCount', options, assert.ifError);
    }

    jobQueue.on('jobSuccess', function(err) {
      count -= 1;
      if (count === 0) return checkIt();
      if (count < 0) throw new Error("more job completions than expected");
    });

    function checkIt() {
      jobQueue.shutdown(function() {
        fs.readFile(hitCountFile, function(err, data) {
          if (err) throw err;
          assert.strictEqual(parseInt(data, 10), 20);
          done();
        });
      });
    }
  });

  it("moves jobs to failed queue and back", function(done) {
    shared.count = 0;

    var jobQueue = new JobQueue({ workerCount: 4 });

    jobQueue.once('jobFail', function() {
      jobQueue.retryFailedJobs(function(err) {
        if (err) return done(err);
      });
    });

    jobQueue.once('jobSuccess', function() {
      assert.strictEqual(shared.count, 2);
      jobQueue.shutdown(done);
    });

    jobQueue.registerTask('./task_fail');

    jobQueue.start();

    jobQueue.submitJob('failFirst', assert.ifError);
  });

  it("ability to delete failed jobs", function(done) {
    shared.count = 0;

    var jobQueue = new JobQueue({ workerCount: 1 });

    jobQueue.on('jobFail', function() {
      jobQueue.deleteFailedJobs(function(err) {
        if (err) return done(err);
        jobQueue.retryFailedJobs(function(err) {
          if (err) return done(err);
          shared.callback = function() {
            assert.strictEqual(shared.count, 1);
            jobQueue.shutdown(done);
          };
          jobQueue.submitJob('callback', assert.ifError);
        });
      });
    });

    jobQueue.registerTask('./task_fail');
    jobQueue.registerTask('./task_cb');

    jobQueue.start();

    jobQueue.submitJob('failFirst', assert.ifError);
  });

  it("handles jobs that crash while processing", function(done) {
    var jobQueue = new JobQueue({ childProcessCount: 3, workerCount: 1});

    jobQueue.once('childRestart', function() {
      jobQueue.once('jobFail', function(job) {
        assert.strictEqual(job.type, 'crashTask');
        jobQueue.shutdown(done);
      });

      setTimeout(function() {
        jobQueue.forceFlushStaleJobs(assert.ifError);
      }, 200);
    });


    jobQueue.registerTask('./task_crash');

    jobQueue.start();

    jobQueue.submitJob('crashTask', assert.ifError);
  });

  it("tries jobs twice if requested", function(done) {
    var jobQueue = new JobQueue();

    jobQueue.registerTask('./task_twice');

    jobQueue.on('jobSuccess', function(job) {
      jobQueue.shutdown(done);
    });

    jobQueue.start();

    jobQueue.submitJob('twice', {retries: 1}, assert.ifError);
  });

  it("retries jobs that time out", function(done) {
    var jobQueue = new JobQueue({childProcessCount: 1, workerCount: 1});

    jobQueue.registerTask('./task_twice_cb');

    jobQueue.on('jobSuccess', function(job) {
      jobQueue.shutdown(done);
    });

    jobQueue.start();

    jobQueue.submitJob('twice_cb', {params: twiceFile, retries: 1}, assert.ifError);

    setTimeout(function() {
      jobQueue.forceFlushStaleJobs(assert.ifError);
    }, 200);
  });
});
