var JobQueue = require('..');
var assert = require('assert');

var describe = global.describe;
var it = global.it;

describe("JobQueue", function() {
  it("processes a thing and then shuts down", function(done) {
    var jobQueue = new JobQueue({ workerCount: 8 });
    var processedString = null;

    jobQueue.registerTask('thisIsMyJobTypeId', {
      perform: function(params, callback) {
        processedString = params.theString.toUpperCase();
        callback();
      },
    });

    jobQueue.registerTask('pleaseShutDownNow', {
      perform: function(params, callback) {
        callback();

        jobQueue.shutdown(function() {
          assert.strictEqual(processedString, "DERP");
          done();
        });
      },
    });

    jobQueue.start();

    jobQueue.submitJob('thisIsMyJobTypeId', 'resource_id', {theString: "derp"}, assert.ifError);
    jobQueue.submitJob('pleaseShutDownNow', 'resource_id_2', null, assert.ifError);
  });
});
