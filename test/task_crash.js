module.exports = {
  id: "crashTask",
  timeout: 100,
  perform: function(params, callback) {
    process.nextTick(function() {
      throw new Error("intentionally thrown error");
    });
  },
};
