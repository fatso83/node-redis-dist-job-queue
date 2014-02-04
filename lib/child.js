var JobWorker = require('./worker');
var args = JSON.parse(process.argv[2]);
var worker = new JobWorker(args);

worker.on('error', function(err) {
  try {
    process.send('message', {
      type: 'error',
      value: err.stack,
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
