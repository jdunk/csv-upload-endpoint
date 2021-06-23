const util = require('util');

function sqlitePromisify(db) {
  db.run[util.promisify.custom] = (sql, params) => {
    return new Promise((resolve, reject) => {
      db.run(sql, params, function(err, res) {
        if (err) {
          return reject(err);
        }
        
        resolve({ lastID: this.lastID, changes: this.changes });
      });
    });
  };
  const dbRun = util.promisify(db.run);

  db.all[util.promisify.custom] = (sql, params = []) => {
    return new Promise((resolve, reject) => {
      db.all(sql, params, function(err, rows) {
        if (err)
          return reject(err);
        
        resolve(rows);
      });
    });
  };
  const dbAll = util.promisify(db.all);

  return {
    dbRun,
    dbAll
  };
}

module.exports = sqlitePromisify;