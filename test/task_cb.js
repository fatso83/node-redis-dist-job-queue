var shared = require('./shared');

module.exports = {
  id: "callback",
  perform: function(params, callback) {
    shared.callback(params);
    callback();
  },
};
