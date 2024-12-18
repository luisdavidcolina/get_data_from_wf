const fs = require('fs');
const path = require('path');

const rutaActual = path.dirname(__filename);
const rutaJsonCOMPLETO = path.join(rutaActual, 'data', 'raw_data.json');

function cargarDatosDesdeJSON(ruta) {
  const contenido = fs.readFileSync(ruta, 'utf-8');
  return JSON.parse(contenido);
}

function convertDateTimeToEpoch(date_dd_mm_yy, time_hh_mm_ss, GMT_zone = "-04:00") {
  var dateParts = date_dd_mm_yy.split('/');
  var timeParts = time_hh_mm_ss.split(':');

  var dateStringToParse = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}T${timeParts[0]}:${timeParts[1]}:00.000${GMT_zone}`;

  var parsedDate = Date.parse(dateStringToParse);
  var dateObject = new Date(parsedDate);
  const epoc = (dateObject.getTime()) / 1000;

  return (epoc);

}


function convertEpochToDateTime(epoch, GMT_zone = "-04:00") {
  const [offsetSign, offsetHours, offsetMinutes] = GMT_zone.match(/([-+])(\d{2}):(\d{2})/).slice(1);
  const offsetSeconds = (parseInt(offsetHours, 10) * 60 + parseInt(offsetMinutes, 10)) * 60;
  const adjustedEpoch = epoch + (offsetSign === '-' ? -1 : 1) * offsetSeconds;

  const dateObject = new Date(adjustedEpoch * 1000);
  const year = dateObject.getUTCFullYear();
  const month = (dateObject.getUTCMonth() + 1).toString().padStart(2, '0');
  const day = dateObject.getUTCDate().toString().padStart(2, '0');
  const hours = dateObject.getUTCHours().toString().padStart(2, '0');
  const minutes = dateObject.getUTCMinutes().toString().padStart(2, '0');
  const seconds = dateObject.getUTCSeconds().toString().padStart(2, '0');

  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}${GMT_zone}`;
}

function run() {
  const rawData = {
    ...cargarDatosDesdeJSON(rutaJsonCOMPLETO)
  };
  console.log(rawData.actualTransactions.length);
  const storeStats = rawData.actualTransactions

  const range = {
    start: '2024-10-01',
    finish: '2024-10-01'
  }

  const filteredStats = storeStats.filter(stat => stat.location === "Plaza Las Americas").filter(stat => {
    const statDate = new Date(convertEpochToDateTime(stat.time).slice(0, 10));
    console.log(statDate)
    const startDate = new Date(range.start);
    const finDate = new Date(range.finish);
    return statDate >= startDate && statDate <= finDate;
  });




  console.log(convertDateTimeToEpoch('01/10/2024', "00:00:00"))
  console.log(convertDateTimeToEpoch('01/10/2024',"23:59:00"))
  console.log(filteredStats.length)

  const sumWithInitial = filteredStats.reduce(
    (accumulator, currentValue) => accumulator + currentValue.stat,
    0,
  );

  console.log(sumWithInitial)

}

run();