const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const fs = require('fs');

var affidabilita = [5, 6, 7, 8, 9, 10, 12, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 36, 37, 38, 39, 40, 41, 42, 42, 44, 45, 46, 47, 48, 51, 56, 70, 71, 72, 73, 74, 75, 76, 77, 78, 80, 83, 84, 85, 86, 87, 91, 92, 93, 94, 95, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 129, 131, 132, 135, 136, 137, 138, 139];
var incidente = [3, 4, 20, 29, 82, 104, 130];



var current = [];
var previous = [];

parse();

async function parse() {
    for (var i = 2015; i <= 2021; i++) {
        await parseYear(i);
    }
}

function parseYear(year) {
    return new Promise((resolve) => {
        fs.createReadStream('stagioni.csv')
            .pipe(csv())
            .on('data', (row) => {
                if (row['Posizione Partenza'] <= 0) {
                    //    return;
                }
                if (row.Anno == year) {
                    current.push(row);
                } else if (row.Anno == year - 1) {
                    previous.push(row);
                }
            })
            .on('end', () => {
                for (let i = 0; i < current.length; i++) {
                    var driverFound = previous.filter(driverOld => driverOld.Pilota == current[i].Pilota);
                    var scuderia = previous.filter(scurediaOld => scurediaOld.Scuderia == current[i].Scuderia);
                    //  console.log((driverFound.map(driver => driver['Posizione Partenza']).reduce((a, b) => a + b, 0) / driverFound.length), (driverFound.map(driver => driver['Posizione Partenza']).reduce((a, b) => a + b, 0) / driverFound.length).toFixed(2));
                    if (driverFound.length > 0) {
                        current[i].PosizionePartenzaMedia = toFixed(driverFound.map(driver => Number.parseInt(driver['Posizione Partenza'])).reduce((a, b) => a + b, 0) / driverFound.length);
                        current[i].PosizioneArrivoMedia = toFixed(driverFound.map(driver => Number.parseInt(driver['Posizione Finale'])).reduce((a, b) => a + b, 0) / driverFound.length);
                        current[i].GainLoss = current[i].PosizionePartenzaMedia - current[i].PosizioneArrivoMedia;
                        current[i].Affidabilita = toFixed(driverFound.map(driver => driver.statusId).filter(val => affidabile(val, affidabilita)).length / driverFound.length);
                        current[i].PuntiGP = toFixed(driverFound.map(driver => Number.parseInt(driver.Punti)).reduce((a, b) => a + b, 0) / driverFound.length);
                        current[i].Incidenti = toFixed(driverFound.map(driver => driver.statusId).filter(val => affidabile(val, incidente)).length / driverFound.length);
                        current[i].PolePosition = toFixed(driverFound.map(driver => driver['Posizione Partenza']).filter(val => val == 1).length / driverFound.length);
                        current[i].Podi = toFixed(driverFound.map(driver => driver['Posizione Finale']).filter(val => val == 2 || val == 3).length / driverFound.length);
                        current[i].Vittorie = toFixed(driverFound.map(driver => driver['Posizione Finale']).filter(val => val == 1).length / driverFound.length);
                        current[i].Top10 = toFixed(driverFound.map(driver => driver['Posizione Finale']).filter(val => val <= 10).length / driverFound.length);
                        current[i].FuoriPunti = toFixed(driverFound.map(driver => driver['Posizione Finale']).filter(val => val > 10 && val <= 20).length / driverFound.length);
                        current[i].GiriVeloci = toFixed(driverFound.map(driver => driver.rank).filter(val => val == 1).length / driverFound.length);
                        current[i].Velocita = toFixed(driverFound.filter(driver => !isNaN(driver.fastestLapSpeed)).map(driver => Number.parseFloat(driver.fastestLapSpeed)).reduce((a, b) => a + b, 0) / driverFound.length);
                        current[i].PuntiScuderia = toFixed(scuderia.map(driver => Number.parseInt(driver.Punti)).reduce((a, b) => a + b, 0) / scuderia.length);
                        current[i].Esordio = "NO";
                    } else {
                        current[i].Esordiente = true;
                        current[i].PosizionePartenzaMedia = mediana(current.filter(driver => driver.PosizionePartenzaMedia).map(driver => driver.PosizionePartenzaMedia));
                        current[i].PosizioneArrivoMedia = mediana(current.filter(driver => driver.PosizioneArrivoMedia).map(driver => driver.PosizioneArrivoMedia));
                        current[i].GainLoss = mediana(current.filter(driver => driver.GainLoss).map(driver => driver.GainLoss));
                        current[i].Affidabilita = mediana(current.filter(driver => driver.Affidabilita).map(driver => driver.Affidabilita));
                        current[i].PuntiGP = mediana(current.filter(driver => driver.PuntiGP).map(driver => driver.PuntiGP));
                        current[i].Incidenti = mediana(current.filter(driver => driver.Incidenti).map(driver => driver.Incidenti));
                        current[i].PolePosition = mediana(current.filter(driver => driver.PolePosition).map(driver => driver.PolePosition));
                        current[i].Podi = mediana(current.filter(driver => driver.Podi).map(driver => driver.Podi));
                        current[i].Vittorie = mediana(current.filter(driver => driver.Vittorie).map(driver => driver.Vittorie));
                        current[i].Top10 = mediana(current.filter(driver => driver.Top10).map(driver => driver.Top10));
                        current[i].FuoriPunti = mediana(current.filter(driver => driver.FuoriPunti).map(driver => driver.FuoriPunti));
                        current[i].GiriVeloci = mediana(current.filter(driver => driver.GiriVeloci).map(driver => driver.GiriVeloci));
                        current[i].Velocita = mediana(current.filter(driver => driver.Velocita).map(driver => driver.Velocita));
                        current[i].PuntiScuderia = toFixed(scuderia.filter(driver => driver.PuntiScuderia).map(driver => Number.parseInt(driver.Punti)).reduce((a, b) => a + b, 0) / scuderia.length);
                        current[i].Esordio = "SI";
                    }
                }

                var unique = [];

                current.forEach(item => {
                    if (!unique.find(uni => uni.Pilota == item.Pilota)) {
                        unique.push(item);
                    }
                });

                for (let i = 0; i < unique.length; i++) {
                    if (unique[i].Esordiente) {
                        unique[i].Pilota = "Esordiente";
                    }
                }

                var csvWriter = createCsvWriter({
                    path: `./stagione-${year}-annoprima.csv`,
                    header: [
                        { id: 'Pilota', title: 'Pilota' },
                        { id: 'PosizionePartenzaMedia', title: 'PosizionePartenzaMedia' },
                        { id: 'PosizioneArrivoMedia', title: 'PosizioneArrivoMedia' },
                        { id: 'GainLoss', title: 'GainLoss' },
                        { id: 'Affidabilita', title: 'Affidabilita' },
                        { id: 'PuntiGP', title: 'PuntiGP' },
                        { id: 'Incidenti', title: 'Incidenti' },
                        { id: 'PolePosition', title: 'PolePosition' },
                        { id: 'Podi', title: 'Podi' },
                        { id: 'Vittorie', title: 'Vittorie' },
                        { id: 'Top10', title: 'Top10' },
                        { id: 'FuoriPunti', title: 'FuoriPunti' },
                        { id: 'GiriVeloci', title: 'GiriVeloci' },
                        { id: 'Velocita', title: 'Velocita' },
                        { id: 'Scuderia', title: 'Scuderia' },
                        { id: 'PuntiScuderia', title: 'PuntiScuderia' },
                        { id: 'Esordio', title: 'Esordio' },
                    ]
                });


                //console.log(current);
                csvWriter
                    .writeRecords(unique)
                    .then(() => { console.log('The CSV file was written successfully'); resolve() });
            });
    });
}

function toFixed(x) {
    return x
}

function affidabile(item, filter) {
    return filter.find(value => value == item) >= 0;
}

function mediana(data) {
    var tryValue = (i) => {
        if (!data[i]) {
            return tryValue(++i);
        } else {
            return data[i];
        }
    };
    if (data.length % 2 == 0) {
        return (tryValue(Number.parseInt(data.length / 2)) + tryValue(Number.parseInt(data.length / 2) + 1)) / 2;
    } else {

        return tryValue(Number.parseInt(data.length / 2));
    }
}