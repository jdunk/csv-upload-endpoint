const Stream = require('stream');
const sqlService = require('./sqlService.js');
const { vehicleColumnNames } = require('./config/vehicleColumns.js');
const uuid = require('uuid');

const writeVehiclesToDb = (onFinish = (numRecordsWritten) => {}) => {
  const writableStream = new Stream.Writable({ objectMode: true });

  let numRecordsWritten = 0;
  let recordBuffer = [];

  const lcVehicleColumnNames = vehicleColumnNames.map(x => x.toLowerCase());

  writableStream._write = async (rowObj, encoding, next) => {
    let atLeastOneNonNullishValue = false;

    const valuesToInsert = lcVehicleColumnNames.map((column) => {
      let value = rowObj[column];

      if (column === 'UUID' && !value) {
        value = uuid.v4();
      }

      if (value === undefined)
        value = null; // More db-value friendly

      if (value !== null)
        atLeastOneNonNullishValue = true;

      return value;
    });

    if (!atLeastOneNonNullishValue) {
      return next();
    }

    recordBuffer.push(valuesToInsert);

    if (recordBuffer.length < sqlService.maxVehicleRecordsPerInsert) {
      return next();
    }

    try {
      const { changes } = await sqlService.bulkInsertOrIgnore(sqlService.vehiclesTableName, vehicleColumnNames, recordBuffer);
      numRecordsWritten += changes;
      recordBuffer = [];
      next();
    }
    catch(e) {
      throw new Error(e);
    }
  }

  writableStream.on('finish', async () => {
    if (recordBuffer.length) {
      const { changes } = await sqlService.bulkInsertOrIgnore(sqlService.vehiclesTableName, vehicleColumnNames, recordBuffer);
      numRecordsWritten += changes;
    }

    onFinish(numRecordsWritten);
  })

  return writableStream;
}

module.exports = {
  writeVehiclesToDb,
};