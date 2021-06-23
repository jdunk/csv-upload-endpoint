const sqlite = require('sqlite3').verbose();
const sqlitePromisify = require('./sqlitePromisify.js');
const vehiclesTableName = process.env.VEHICLES_TABLE_NAME || 'vehicles';

const db = new sqlite.Database(':memory:', (err) => {
  if (err) {
    throw new Error('An error occurred while trying to create/connect to the database');
  }
});

const { dbRun, dbAll } = sqlitePromisify(db);

const createVehiclesTable = async () => {
  try {
    await dbRun(`CREATE TABLE IF NOT EXISTS \`${vehiclesTableName}\` (
      UUID TEXT PRIMARY KEY,
      VIN TEXT UNIQUE,
      Make,
      Model,
      Mileage,
      Year,
      Price,
      \`Zip Code\`,
      \`Create Date\`,
      \`Update Date\`
    )`);
  }
  catch (e) {
    throw new Error(`An error occurred while trying to create the vehicles table: ${e}`);
  }
};
createVehiclesTable();

const bulkInsertOrIgnore = async (tableName, fieldNames, valueSets) => {
  try {
    const fields = `(\`${fieldNames.join('`, `')}\`)`;

    const valueSetPlaceholder = `(?${',?'.repeat(fieldNames.length-1)})`;
    const valuesPlaceholder = `${valueSetPlaceholder}${`,${valueSetPlaceholder}`.repeat(valueSets.length-1)}`;

    const query = `INSERT OR IGNORE INTO \`${tableName}\`
      ${fields}
      VALUES ${valuesPlaceholder}`;
    
    const params = [
      ...valueSets.flat()
    ];

    // console.log({ query, params })
    return await dbRun(query, params);
  }
  catch(e) {
    throw new Error(`An error occurred while trying to insert records into \`${tableName}\`: ${e}`);
  }
};

const fetchAll = async (tableName) => {
  try {
    return await dbAll(`SELECT * FROM ${tableName}`);
  }
  catch(e) {
    throw new Error(`An error occurred while trying to fetch records: ${e}`);
  }
};

module.exports = {
  bulkInsertOrIgnore,
  closeDb: () => db.close(),
  maxVehicleRecordsPerInsert: Number(process.env.MAX_VEHICLE_RECORDS_PER_INSERT || 5),
  vehiclesTableName,
};