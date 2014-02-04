// fail the first time it's invoked, then pass
var shared = require('./shared');

module.exports = {
  id: "failFirst",
  perform: function(params, callback) {
    shared.count += 1;
    if (shared.count < 2) {
      return callback(new Error("test failure"));
    }
    callback();
  },
};
