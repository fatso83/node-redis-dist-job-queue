var fs = require('fs');

module.exports = {
  id: "hitCount",
  perform: function(filePath, callback) {
    fs.readFile(filePath, function(err, data) {
      var x = parseInt(data, 10);
      if (err) return callback(err);
      x += 1;
      fs.writeFile(filePath, x.toString(), callback);
    });
  },
};

