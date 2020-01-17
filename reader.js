const csvParser = require('csv-parser');
const fs = require('fs');

let data = [];
fs.createReadStream('./reconnected.csv').pipe(csvParser())
    .on('data', (row) => {
        data.push(row);
    })
    .on('end', () => {
        console.log(data[1]);
    })