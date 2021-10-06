const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const fs = require('fs');

var filtered = [];

fs.createReadStream('test.csv')
    .pipe(csv())
    .on('data', (row) => {
        //console.log(row.raceId);
        var currentRow = null;
        var index = filtered.findIndex(current => current.raceId === row.raceId && current.driverId === row.driverId);

        if (index >= 0) {
            if (filtered[index].stop < row.stop) {
                filtered[index].stop = row.stop;
                filtered[index].Giro = row.Giro;
                filtered[index].Tempo = row.Tempo.toString();
                filtered[index].Durata = row.Durata.toString();
                filtered[index].milliseconds = row.milliseconds;
            }
        } else {
            filtered.push(row);
        }
    })
    .on('end', () => {
        console.log('CSV file successfully processed');
        const csvWriter = createCsvWriter({
            path: './test-filtered.csv',
            header: [
                { id: 'raceId', title: 'raceId' },
                { id: 'driverId', title: 'driverId' },
                { id: 'stop', title: 'stop' },
                { id: 'Giro', title: 'Giro' },
                { id: 'Tempo', title: 'Tempo' },
                { id: 'Durata', title: 'Durata' },
                { id: 'milliseconds', title: 'milliseconds' },
            ]
        });

        csvWriter
            .writeRecords(filtered)
            .then(() => console.log('The CSV file was written successfully'));
    });
