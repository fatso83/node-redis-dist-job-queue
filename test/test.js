var JobQueue = require('..');
var assert = require('assert');
var path = require('path');
var shared = require('./shared');
var fs = require('fs');

var describe = global.describe;
var it = global.it;
var after = global.after;

var hitCountFile = path.resolve(__dirname, "hitCount.txt");

describe("JobQueue", function() {
  after(function(done) {
    fs.unlink(hitCountFile, function(err) {
      done();
    });
  });

  it("processes a thing and then shuts down - in process", function(done) {
    var jobQueue = new JobQueue({ workerCount: 8 });
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

    jobQueue.submitJob('thisIsMyTaskId', 'resource_id', {theString: "derp"}, assert.ifError);
    jobQueue.submitJob('callback', 'resource_id_2', null, assert.ifError);
  });

  it("processes a thing and then shuts down - child processes", function(done) {
    var jobQueue = new JobQueue({ workerCount: 1, childProcessCount: 8 });

    fs.writeFileSync(hitCountFile, "0");

    jobQueue.registerTask('./task_hit_count');

    jobQueue.start();

    var count = 20;
    for (var i = 0; i < count; i += 1) {
      jobQueue.submitJob('hitCount', 'hit_count', hitCountFile, assert.ifError);
    }

    setTimeout(checkIt, 1000);

    function checkIt() {
      jobQueue.shutdown(function() {
        fs.readFile(hitCountFile, function(err, data) {
          if (err) throw err;
          assert.strictEqual(parseInt(data, 10), 1);
          done();
        });
      });
    }
  });
});
