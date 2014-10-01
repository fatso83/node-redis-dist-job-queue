### 0.1.0

 * MIT LICENSE
 * update dependencies
 * use simpler and more robust uuid module
 * do not depend on hiredis
 * fix handling default config options

### 0.0.6

 * handle default config options better

### 0.0.5

 * add a `retries` parameter to `submitJob` which defaults to `0`.

### 0.0.4

 * when a job fails to acquire a lock, instead of dropping it, re-queue it

### 0.0.3

 * catch the case when the task returns a non error object
 * more consistent job.errorStack message
 * fix fake job failures from occuring
