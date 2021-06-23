'use strict'

const express = require('express');
const Busboy = require('busboy');
const Stream = require('stream');
const stripBomStream = require('strip-bom-stream');
const csv = require('csv-parser');
const { writeVehiclesToDb } = require('./saveVehicle.js');
const { closeDb } = require('./sqlService.js');

const app = express();

app.get('/', (req, res, next) => {
  res.sendFile(`${__dirname}/index.html`);
});

app.post('/upload', (req, res) => {
  const busboy = new Busboy({
    headers: req.headers,
  });

  busboy.on('file', (fieldname, file, filename, encoding, mimetype) => {
    console.log(`Upload of '${filename}' has started`);

    file.on('end', function() {
      console.log(`Upload of '${filename}' finished`);
    });

    const onFinished = (numRecordsWritten) => {
      res.status(201).json({ numRecordsWritten });
    };

    Stream.pipeline(
      file,
      stripBomStream(),
      csv({ mapHeaders: ({ header }) => header.toLowerCase() }),
      writeVehiclesToDb(onFinished),
      function done(err) {
        if (err) {
          console.error('Pipeline failed', err);
        } else {
          console.log('Pipeline succeeded');
        }
      }
    );
  });

  req.pipe(busboy);
});

process.on('exit', () => {
  closeDb();
});

const serverPort = process.env.SERVER_PORT || 3333;

app.listen(serverPort, function(){
  console.log(`Server is listening on port ${serverPort}`);
});
