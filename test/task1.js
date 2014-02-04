var shared = require('./shared');

module.exports = {
  id: "thisIsMyTaskId",
  perform: function(params, callback) {
    shared.processedString = params.theString.toUpperCase();
    callback();
  },
};
