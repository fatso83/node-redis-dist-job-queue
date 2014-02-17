var first = true;

module.exports = {
  id: "twice",
  perform: function(params, callback) {
    if (first) {
      first = false;
      callback(new Error("it doesn't work the first time"));
      return;
    }
    callback();
  },
};
