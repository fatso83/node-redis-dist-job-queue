# Distributed Job Queue for Node.js backed by Redis

## How it works

 * Register tasks that can be performed, giving each task an ID and a
   JavaScript function.
 * When you submit a processing job, you choose which task to run and
   supply a resource ID. If that resource ID is already being processed,
   the job will not run.
 * Jobs are taken from a redis queue; so the task could be performed on any
   computer with access to the resource.
 * When you want to shutdown, call `shutdown` and wait for the callback. This
   will shutdown the queue gracefully, allowing any in progress jobs to
   complete. This works well in tandem with [naught](https://github.com/andrewrk/naught)

## Synopsis

```js
var JobQueue = require('redis-dist-job-queue');
var jobQueue = new JobQueue();

jobQueue.registerTask('thisIsMyJobTypeId', {
  perform: function(params, callback) {
    console.info(params.theString.toUpperCase());
    callback();
  },
});

jobQueue.start();

jobQueue.submitJob('thisIsMyJobTypeId', 'resource_id', {theString: "derp"}, function(err) {
  if (err) throw err;
  console.info("job submitted");
});
```

## Documentation

### JobQueue([options])

`options`:

 * `namespace` - redis key namespace. Defaults to `"redis-dist-job-queue."`
 * `queueId` - the ID of the queue on redis that we are connecting to.
   Defaults to `"default"`.
 * `workerCount` - number of workers *for this JobQueue instance*. Defaults
   to the number of CPU cores on the computer.
 * `redisConfig` - defaults to an object with properties:
   * `host` - 127.0.0.1
   * `port` - 6379
   * `db` - 1
 * `flushStaleTimeout` - every this many milliseconds, scan for jobs that
   crashed while executing and moves them back into the queue.
   Defaults to 60000.

### jobQueue.start()

Nothing happens until you call this method. This connects to redis and starts
popping jobs off the queue.

### jobQueue.registerTask(taskId, options)

 * `taskId` - unique ID that identifies what function to call to perform the task
 * `options` - object with these properties
   * `perform(params, callback)` - function which actually does the task.
     * `params` - the same params you gave to `submitJob`, JSON encoded
        and then decoded.
     * `callback(err)` - call it when you're done processing
   * `timeout` - milliseconds since last heartbeat to wait before considering
     a job failed. defaults to `10000`.

