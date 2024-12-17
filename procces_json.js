const fs = require('fs');
const path = require('path');

const rutaActual = path.dirname(__filename);
const rutaJsonCOMPLETO = path.join(rutaActual, 'data', 'raw_data.json');

function cargarDatosDesdeJSON(ruta) {
  const contenido = fs.readFileSync(ruta, 'utf-8');
  return JSON.parse(contenido);
}

function run() {
    const rawData = {
        ...cargarDatosDesdeJSON(rutaJsonCOMPLETO)
      };
console.log(rawData.scheduled.length);   
console.log(rawData.minimumIdeal.length);   
 
}

run();