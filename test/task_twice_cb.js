var fs = require('fs');

module.exports = {
  id: "twice_cb",
  timeout: 100,
  perform: function(filePath, callback) {
    fs.stat(filePath, function(err, stat) {
      if (err) {
        fs.writeFile(filePath, "", function(err) {
          throw new Error("intentionally thrown error");
        });
        return;
      }

      // works second time
      callback();
    });
  },
};
