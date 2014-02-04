var JobWorker = require('./worker');
var args = JSON.parse(process.argv[2]);
var worker = new JobWorker(args);

worker.on('error', function(err) {
  try {
    process.send({
      type: 'error',
      value: err.stack,
    });
  } catch (err) {

  }
});

worker.on('jobFail', function(job) {
  try {
    process.send({
      type: 'jobFail',
      value: job,
    });
  } catch (err) {

  }
});

worker.on('jobSuccess', function(job) {
  try {
    process.send({
      type: 'jobSuccess',
      value: job,
    });
  } catch (err) {

  }
});

worker.start();

process.on('message', function(message) {
  if (message === 'shutdown') {
    worker.shutdown(function() {
      process.disconnect();
    });
  }
});
