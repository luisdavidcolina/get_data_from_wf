const axios = require('axios');
const fs = require('fs');
const path = require('path');
const util = require('util');
const sql = require('mssql');

const writeFile = util.promisify(fs.writeFile);
const mkdir = util.promisify(fs.mkdir);

const API_TOKEN = '62f368d294e6e4ce9f897702e913f1345d723611ea001c10282833942fd13c2c';
const BASE_URL = 'https://my.tanda.co/api/v2';

const headers = {
  Authorization: `Bearer ${API_TOKEN}`
};

// Definir las fechas de inicio y fin
const fechaInicio = '2024-08-06';
const fechaFin = '2024-08-16';

// Configuración de la conexión a SQL Server
const sqlConfig = {
  user: 'WFPBI',
  password: 'Wk~%qz$pB8m',
  database: 'WFPBI',
  server: 'localhost',
  options: {
    encrypt: false,
    trustServerCertificate: false
  }
};

async function obtenerDatos(endpoint, params = {}) {
  try {
    const response = await axios.get(`${BASE_URL}${endpoint}`, { headers, params });
    return response.data;
  } catch (error) {
    console.error(`Error al obtener datos de ${endpoint}:`, error.response ? error.response.data : error.message);
    return null;
  }
}

function generarRangosSemanales(inicio, fin) {
  const rangos = [];
  let fechaActual = new Date(inicio);
  const fechaFinal = new Date(fin);

  while (fechaActual <= fechaFinal) {
    const inicioSemana = new Date(fechaActual);
    const finSemana = new Date(fechaActual.setDate(fechaActual.getDate() + 6));
    
    if (finSemana > fechaFinal) {
      rangos.push({
        inicio: inicioSemana.toISOString().split('T')[0],
        fin: fechaFinal.toISOString().split('T')[0]
      });
    } else {
      rangos.push({
        inicio: inicioSemana.toISOString().split('T')[0],
        fin: finSemana.toISOString().split('T')[0]
      });
    }

    fechaActual.setDate(fechaActual.getDate() + 1);
  }

  return rangos;
}

function obtenerLunes(fecha) {
  const date = new Date(fecha);
  const day = date.getDay();
  const diff = day === 0 ? -6 : 1 - day; // Ajuste si el día es domingo
  const lunes = new Date(date.setDate(date.getDate() + diff));
  return lunes.toISOString().split('T')[0];
}

async function obtenerDatosTandaApi(fechaInicio, fechaFin) {
  const datos = {
    minimumIdealCoverage: {
      Coverage: [],
      NonCoverage: [],
      Training: []
    },
    scheduledCoverage: {
      Coverage: [],
      NonCoverage: [],
      Training: []
    },
    transactionForecast: [],
    actualTransactions: [],
    Items: [],
    totalPunchesLaborHours: []
  };

  const rangosSemanales = generarRangosSemanales(fechaInicio, fechaFin);

  // Obtener lista de departamentos
  let departamentos = await obtenerDatos('/departments');
  
  // Filtrar por los departamentos relevantes
  const departamentosCoverage = departamentos.filter(departamento => 
    departamento.name === 'Baristas DT' ||
    departamento.name === 'Baristas' ||
    departamento.name === 'Supervisores' ||
    departamento.name === 'Supervisores DT'
  );
  const departamentosNonCoverage = departamentos.filter(departamento => 
    departamento.name === 'Non Coverage'
  );
  const departamentosTraining = departamentos.filter(departamento => 
    departamento.name === 'Training'
  );

  // Eliminar duplicados
  function eliminarDuplicadosPorId(array) {
    const idsUnicos = new Set();
    return array.filter((objeto) => {
      if (idsUnicos.has(objeto.id)) {
        return false;
      } else {
        idsUnicos.add(objeto.id);
        return true;
      }
    });
  }
  const departamentosCoverageUnicos = eliminarDuplicadosPorId(departamentosCoverage);
  const departamentosNonCoverageUnicos = eliminarDuplicadosPorId(departamentosNonCoverage);
  const departamentosTrainingUnicos = eliminarDuplicadosPorId(departamentosTraining);

  if ((!departamentosCoverageUnicos || departamentosCoverageUnicos.length === 0) &&
      (!departamentosNonCoverageUnicos || departamentosNonCoverageUnicos.length === 0) &&
      (!departamentosTrainingUnicos || departamentosTrainingUnicos.length === 0)) {
    console.error('No se pudieron obtener los departamentos o la lista está vacía.');
    return datos;
  }

  // Obtener lista de ubicaciones
  const ubicaciones = await obtenerDatos('/locations');
  if (!ubicaciones || ubicaciones.length === 0) {
    console.error('No se pudieron obtener las ubicaciones o la lista está vacía.');
    return datos;
  }


  // Obtener lista de datastreams
  const datastreams = await obtenerDatos('/datastreams');
  if (!datastreams || datastreams.length === 0) {
    console.error('No se pudieron obtener los datastreams o la lista está vacía.');
    return datos;
  }

  //Obtener lista de datastreams joins
  const datastreamsJoins = await obtenerDatos('/datastreamjoins');
  if (!datastreamsJoins || datastreamsJoins.length === 0) {
    console.error('No se pudieron obtener los datastreams joins o la lista está vacía.');
    return datos;
  }


  for (const rango of rangosSemanales) {
    const lunes = obtenerLunes(rango.inicio);
  
    // Obtener Horas Recomendadas (minimumIdealCoverage)
const totalRecommendedHours = {};

for (const department of departamentosCoverageUnicos) {
  const recommendedHours = await obtenerDatos('/recommended_hours', {
    from_date: rango.inicio,
    to_date: rango.fin,
    department_id: department.id
  });

  if (recommendedHours) {
    const location = ubicaciones.find(loc => loc.id === department.location_id);
    if (location) {
      if (!totalRecommendedHours[location.id]) {
        totalRecommendedHours[location.id] = {
          week: lunes,
          location_name: location.name,
          location_id: location.id,
          Total: 0
        };
      }
      totalRecommendedHours[location.id].Total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);
    } else {
      console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
    }
  } else {
    console.error(`No se pudieron obtener las horas recomendadas para el departamento con ID: ${department.id}`);
  }
}

for (const department of departamentosNonCoverageUnicos) {
  const recommendedHours = await obtenerDatos('/recommended_hours', {
    from_date: rango.inicio,
    to_date: rango.fin,
    department_id: department.id
  });

  if (recommendedHours) {
    const location = ubicaciones.find(loc => loc.id === department.location_id);
    if (location) {
      if (!totalRecommendedHours[location.id]) {
        totalRecommendedHours[location.id] = {
          week: lunes,
          location_name: location.name,
          location_id: location.id,
          Total: 0
        };
      }
      totalRecommendedHours[location.id].Total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);
    } else {
      console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
    }
  } else {
    console.error(`No se pudieron obtener las horas recomendadas para el departamento con ID: ${department.id}`);
  }
}

for (const department of departamentosTrainingUnicos) {
  const recommendedHours = await obtenerDatos('/recommended_hours', {
    from_date: rango.inicio,
    to_date: rango.fin,
    department_id: department.id
  });

  if (recommendedHours) {
    const location = ubicaciones.find(loc => loc.id === department.location_id);
    if (location) {
      if (!totalRecommendedHours[location.id]) {
        totalRecommendedHours[location.id] = {
          week: lunes,
          location_name: location.name,
          location_id: location.id,
          Total: 0
        };
      }
      totalRecommendedHours[location.id].Total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);
    } else {
      console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
    }
  } else {
    console.error(`No se pudieron obtener las horas recomendadas para el departamento con ID: ${department.id}`);
  }
}

// Convertir el objeto totalRecommendedHours a un array y agregarlo a datos.minimumIdealCoverage
datos.minimumIdealCoverage.Coverage = Object.values(totalRecommendedHours);

   // Obtener Cobertura Programada (scheduledCoverage)
for (const department of departamentosCoverageUnicos) {
  const scheduledCoverage = await obtenerDatos(`/rosters/on/${rango.inicio}`, {
    show_costs: false,
    department_id: department.id
  });

  if (scheduledCoverage) {
    let totalHoras = 0;

    for (const schedule of scheduledCoverage.schedules) {
      for (const shift of schedule.schedules) {
        // Convertir las fechas Unix a objetos Date
        const startDate = new Date(shift.start * 1000);
        const finishDate = new Date(shift.finish * 1000);

        // Calcular la diferencia en milisegundos
        const diffMilliseconds = finishDate - startDate;

        // Calcular el total de break en milisegundos
        const breakMilliseconds = shift.breaks.reduce((total, b) => total + (b.length * 60 * 1000), 0);

        // Restar el break de la diferencia
        const totalMilliseconds = diffMilliseconds - breakMilliseconds;

        // Convertir el total de milisegundos a horas y sumar al total
        totalHoras += totalMilliseconds / (1000 * 60 * 60);
      }
    }

    const location = ubicaciones.find(loc => loc.id === department.location_id);
    if (location) {
      datos.scheduledCoverage.Coverage.push({
        week: lunes,
        location: location.name,
        location_id: location.id,
        total: totalHoras
      });
    } else {
      console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
    }
  } else {
    console.error(`No se pudieron obtener las coberturas programadas para el departamento con ID: ${department.id}`);
  }
}

for (const department of departamentosNonCoverageUnicos) {
  const scheduledCoverage = await obtenerDatos(`/rosters/on/${rango.inicio}`, {
    show_costs: false,
    department_id: department.id
  });

  if (scheduledCoverage) {
    let totalHoras = 0;

    for (const schedule of scheduledCoverage.schedules) {
      for (const shift of schedule.schedules) {
        // Convertir las fechas Unix a objetos Date
        const startDate = new Date(shift.start * 1000);
        const finishDate = new Date(shift.finish * 1000);

        // Calcular la diferencia en milisegundos
        const diffMilliseconds = finishDate - startDate;

        // Calcular el total de break en milisegundos
        const breakMilliseconds = shift.breaks.reduce((total, b) => total + (b.length * 60 * 1000), 0);

        // Restar el break de la diferencia
        const totalMilliseconds = diffMilliseconds - breakMilliseconds;

        // Convertir el total de milisegundos a horas y sumar al total
        totalHoras += totalMilliseconds / (1000 * 60 * 60);
      }
    }

    const location = ubicaciones.find(loc => loc.id === department.location_id);
    if (location) {
      datos.scheduledCoverage.NonCoverage.push({
        week: lunes,
        location: location.name,
        location_id: location.id,
        total: totalHoras
      });
    } else {
      console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
    }
  } else {
    console.error(`No se pudieron obtener las coberturas programadas para el departamento con ID: ${department.id}`);
  }
}

for (const department of departamentosTrainingUnicos) {
  const scheduledCoverage = await obtenerDatos(`/rosters/on/${rango.inicio}`, {
    show_costs: false,
    department_id: department.id
  });

  if (scheduledCoverage) {
    let totalHoras = 0;

    for (const schedule of scheduledCoverage.schedules) {
      for (const shift of schedule.schedules) {
        // Convertir las fechas Unix a objetos Date
        const startDate = new Date(shift.start * 1000);
        const finishDate = new Date(shift.finish * 1000);

        // Calcular la diferencia en milisegundos
        const diffMilliseconds = finishDate - startDate;

        // Calcular el total de break en milisegundos
        const breakMilliseconds = shift.breaks.reduce((total, b) => total + (b.length * 60 * 1000), 0);

        // Restar el break de la diferencia
        const totalMilliseconds = diffMilliseconds - breakMilliseconds;

        // Convertir el total de milisegundos a horas y sumar al total
        totalHoras += totalMilliseconds / (1000 * 60 * 60);
      }
    }

    const location = ubicaciones.find(loc => loc.id === department.location_id);
    if (location) {
      datos.scheduledCoverage.Training.push({
        week: lunes,
        location: location.name,
        location_id: location.id,
        total: totalHoras
      });
    } else {
      console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
    }
  } else {
    console.error(`No se pudieron obtener las coberturas programadas para el departamento con ID: ${department.id}`);
  }
}

if (ubicaciones) {
  for (const location of ubicaciones) {
    // Obtener Pronóstico de Transacciones
    const predictedTransactions = await obtenerDatos(`/predicted_storestats/for_location/${location.id}`, {
      from: rango.inicio,
      to: rango.fin
    });
    if (predictedTransactions && Array.isArray(predictedTransactions.stats)) {
      // Sumar el valor de "stat"
      const totalStat = predictedTransactions.stats.reduce((sum, stat) => sum + stat.stat, 0);

      datos.transactionForecast.push({
        week: lunes,
        location: location.name,
        location_id: location.id,
        total: totalStat
      });
    } else {
      console.error(`No se pudieron obtener las transacciones pronosticadas para la ubicación con ID: ${location.id}`);
    }
  }
}

if (datastreams) {
  for (const datastream of datastreams) {
    // Encontrar el datastream join correspondiente
    const datastreamJoin = datastreamsJoins.find(join => join.data_stream_id === datastream.id && join.data_streamable_type === 'Location');
    if (datastreamJoin) {
      const location = ubicaciones.find(loc => loc.id === datastreamJoin.data_streamable_id);
      if (location) {
        // Obtener Transacciones Reales
        const actualTransactions = await obtenerDatos(`/storestats/for_datastream/${datastream.id}`, {
          from: rango.inicio,
          to: rango.fin,
          type: 'checks'
        });
        if (Array.isArray(actualTransactions)) {
          // Sumar el valor de "stat"
          const totalChecks = actualTransactions.reduce((sum, transaction) => sum + transaction.stat, 0);

          datos.actualTransactions.push({
            week: lunes,
            datastream: datastream.name,
            location: location.name,
            location_id: location.id,
            total: totalChecks
          });
        } else {
          console.error(`No se pudieron obtener las transacciones reales para el datastream con ID: ${datastream.id}`);
        }

        // Obtener Ventas Reales
        const actualSales = await obtenerDatos(`/storestats/for_datastream/${datastream.id}`, {
          from: rango.inicio,
          to: rango.fin,
          type: 'sales count'
        });
        if (Array.isArray(actualSales)) {
          // Sumar el valor de "stat"
          const totalSales = actualSales.reduce((sum, sale) => sum + sale.stat, 0);

          datos.Items.push({
            week: lunes,
            datastream: datastream.name,
            location: location.name,
            location_id: location.id,
            total: totalSales
          });
        } else {
          console.error(`No se pudieron obtener las ventas reales para el datastream con ID: ${datastream.id}`);
        }
      } else {
        console.error(`No se pudo encontrar la ubicación para el datastream join con ID: ${datastreamJoin.id}`);
      }
    } else {
      console.error(`No se pudo encontrar el datastream join para el datastream con ID: ${datastream.id}`);
    }
  }
}

    // Obtener Total de Horas Laborales Semanales Registradas
const totalWeeklyWorkedHours = await obtenerDatos(`/timesheets/on/${rango.inicio}`, {
  show_costs: false,
  show_award_interpretation: false
});
if (Array.isArray(totalWeeklyWorkedHours)) {
  for (const hours of totalWeeklyWorkedHours) {
    if (hours.status === 'approved') {
      let totalHoras = 0;

      for (const shift of hours.shifts) {
        // Convertir las fechas Unix a objetos Date
        const startDate = new Date(shift.start * 1000);
        const finishDate = new Date(shift.finish * 1000);

        // Calcular la diferencia en milisegundos
        const diffMilliseconds = finishDate - startDate;

        // Convertir el break de minutos a milisegundos
        const breakMilliseconds = shift.break_length * 60 * 1000;

        // Restar el break de la diferencia
        const totalMilliseconds = diffMilliseconds - breakMilliseconds;

        // Convertir el total de milisegundos a horas y sumar al total
        totalHoras += totalMilliseconds / (1000 * 60 * 60);
      }

      const department = departamentos.find(dep => dep.id === hours.shifts[0].department_id);
      if (department) {
        const location = ubicaciones.find(loc => loc.id === department.location_id);
        if (location) {
          datos.totalPunchesLaborHours.push({
            week: lunes,
            location: location.name,
            location_id: location.id,
            totalHoras: totalHoras
          });
        } else {
          console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
        }
      } else {
        console.error(`No se pudo encontrar el departamento con ID: ${hours.shifts[0].department_id}`);
      }
    }
  }
} else {
  console.error(`No se pudieron obtener las horas laborales semanales registradas para el rango: ${rango.inicio} - ${rango.fin}`);
}
  
}

  return datos;

}

//DATOS COMPLETOS NO TOTALIZADOS 
async function obtenerDatosCOMPLETOSTandaApi(fechaInicio, fechaFin) {
  const datos = {
    minimumIdealCoverage: [],
    scheduledCoverage: [],
    transactionForecast: [],
    actualTransactions: [],
    Items: [],
    totalPunchesLaborHours: []
  };

  const rangosSemanales = generarRangosSemanales(fechaInicio, fechaFin);

  // Obtener lista de departamentos
  let departamentos = await obtenerDatos('/departments');
  
  // Filtrar por los departamentos relevantes
  const departamentosRelevantes = departamentos.filter(departamento => 
    departamento.name === 'Baristas DT' ||
    departamento.name === 'Baristas' ||
    departamento.name === 'Supervisores' ||
    departamento.name === 'Supervisores DT' ||
    departamento.name === 'Non Coverage' ||
    departamento.name === 'Training'
  );

  // Eliminar duplicados
  function eliminarDuplicadosPorId(array) {
    const idsUnicos = new Set();
    return array.filter((objeto) => {
      if (idsUnicos.has(objeto.id)) {
        return false;
      } else {
        idsUnicos.add(objeto.id);
        return true;
      }
    });
  }
  const departamentosUnicos = eliminarDuplicadosPorId(departamentosRelevantes);

  if (!departamentosUnicos || departamentosUnicos.length === 0) {
    console.error('No se pudieron obtener los departamentos o la lista está vacía.');
    return datos;
  }

  // Obtener lista de ubicaciones
  const ubicaciones = await obtenerDatos('/locations');
  if (!ubicaciones || ubicaciones.length === 0) {
    console.error('No se pudieron obtener las ubicaciones o la lista está vacía.');
    return datos;
  }

  // Obtener lista de datastreams
  const datastreams = await obtenerDatos('/datastreams');
  if (!datastreams || datastreams.length === 0) {
    console.error('No se pudieron obtener los datastreams o la lista está vacía.');
    return datos;
  }

  //Obtener lista de datastreams joins
  const datastreamsJoins = await obtenerDatos('/datastreamjoins');
  if (!datastreamsJoins || datastreamsJoins.length === 0) {
    console.error('No se pudieron obtener los datastreams joins o la lista está vacía.');
    return datos;
  }


  for (const rango of rangosSemanales) {
  
    // Obtener Horas Recomendadas (minimumIdealCoverage)
for (const department of departamentosUnicos) {
  const recommendedHoursComplete = await obtenerDatos('/recommended_hours', {
    from_date: rango.inicio,
    to_date: rango.fin,
    department_id: department.id
  });

  if (recommendedHoursComplete) {
    const location = ubicaciones.find(loc => loc.id === department.location_id);
    if (location) {
      const recommendedHoursFlattened = recommendedHoursComplete.map((item) => ({
        ...item,
        department_name: department.name,
        department_id: department.id,
        location_name: location.name,
        location_id: location.id
      }));
      datos.minimumIdealCoverage.push(...recommendedHoursFlattened);
    } else {
      console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
    }
  } else {
    console.error(`No se pudieron obtener las horas recomendadas para el departamento con ID: ${department.id}`);
  }
}

// Obtener Cobertura Programada (scheduledCoverage)
for (const department of departamentosUnicos) {
  const scheduledCoverageComplete = await obtenerDatos(`/rosters/on/${rango.inicio}`, {
    show_costs: false,
    department_id: department.id
  });

  if (scheduledCoverageComplete) {
    const location = ubicaciones.find(loc => loc.id === department.location_id);
    if (location) {
      const scheduledCoverageFlattened = scheduledCoverageComplete.schedules.flatMap(schedule => 
        schedule.schedules.map(shift => {
          // Calcular el break_length en minutos
          const breakLength = shift.breaks.reduce((total, b) => {
            const breakStart = new Date(b.start * 1000);
            const breakFinish = new Date(b.finish * 1000);
            return total + (breakFinish - breakStart) / (1000 * 60);
          }, 0);

          return {
            ...shift,
            department_name: department.name,
            department_id: department.id,
            location_name: location.name,
            location_id: location.id,
            break_length: breakLength
          };
        })
      );
      datos.scheduledCoverage.push(...scheduledCoverageFlattened);
    } else {
      console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
    }
  } else {
    console.error(`No se pudieron obtener las coberturas programadas para el departamento con ID: ${department.id}`);
  }
}

if (ubicaciones) {
  for (const location of ubicaciones) {
    // Obtener Pronóstico de Transacciones
    const predictedTransactionsComplete = await obtenerDatos(`/predicted_storestats/for_location/${location.id}`, {
      from: rango.inicio,
      to: rango.fin
    });
    if (predictedTransactionsComplete && Array.isArray(predictedTransactionsComplete.stats)) {
      const predictedTransactionsFlattened = predictedTransactionsComplete.stats.map((item) => ({
        ...item,
        location_name: location.name,
        location_id: location.id
      }));

      datos.transactionForecast.push(...predictedTransactionsFlattened);
    } else {
      console.error(`No se pudieron obtener las transacciones pronosticadas para la ubicación con ID: ${location.id}`);
    }
  }
}

if (datastreams) {
  for (const datastream of datastreams) {
    // Encontrar el datastream join correspondiente
    const datastreamJoin = datastreamsJoins.find(join => join.data_stream_id === datastream.id && join.data_streamable_type === 'Location');
    if (datastreamJoin) {
      const location = ubicaciones.find(loc => loc.id === datastreamJoin.data_streamable_id);
      if (location) {
        // Obtener Transacciones Reales
        const actualTransactionsComplete = await obtenerDatos(`/storestats/for_datastream/${datastream.id}`, {
          from: rango.inicio,
          to: rango.fin,
          type: 'checks'
        });
        if (Array.isArray(actualTransactionsComplete)) {
          const actualTransactionsFlattened = actualTransactionsComplete.map((item) => ({
            ...item,
            location_name: location.name,
            location_id: location.id
          }));

          datos.actualTransactions.push(...actualTransactionsFlattened);
        } else {
          console.error(`No se pudieron obtener las transacciones reales para el datastream con ID: ${datastream.id}`);
        }

        // Obtener Ventas Reales
        const actualSalesComplete = await obtenerDatos(`/storestats/for_datastream/${datastream.id}`, {
          from: rango.inicio,
          to: rango.fin,
          type: 'sales count'
        });
        if (Array.isArray(actualSalesComplete)) {
          const actualSalesFlattened = actualSalesComplete.map((item) => ({
            ...item,
            location_name: location.name,
            location_id: location.id
          }));

          datos.Items.push(...actualSalesFlattened);
        } else {
          console.error(`No se pudieron obtener las ventas reales para el datastream con ID: ${datastream.id}`);
        }
      } else {
        console.error(`No se pudo encontrar la ubicación para el datastream join con ID: ${datastreamJoin.id}`);
      }
    } else {
      console.error(`No se pudo encontrar el datastream join para el datastream con ID: ${datastream.id}`);
    }
  }
}

        // Obtener Total de Horas Laborales Semanales Registradas
const totalWeeklyWorkedHoursComplete = await obtenerDatos(`/timesheets/on/${rango.inicio}`, {
  show_costs: false,
  show_award_interpretation: false
});
if (Array.isArray(totalWeeklyWorkedHoursComplete)) {
  for (const hours of totalWeeklyWorkedHoursComplete) {
    if (hours.status === 'approved') {
      const department = departamentos.find(dep => dep.id === hours.shifts[0].department_id);
      if (department) {
        const location = ubicaciones.find(loc => loc.id === department.location_id);
        if (location) {
          const shiftsFlattened = hours.shifts.map(shift => {
            const { breaks, tag, tag_id, metadata, leave_request_id, allowances, approved_by, approved_at, award_interpretation, ...rest } = shift;
            return {
              ...rest,
              break_length: breaks.reduce((total, b) => total + b.length, 0)
            };
          });

          datos.totalPunchesLaborHours.push({
            location: location.name,
            location_id: location.id,
            datos: {
              ...hours,
              shifts: shiftsFlattened
            }
          });
        } else {
          console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
        }
      } else {
        console.error(`No se pudo encontrar el departamento con ID: ${hours.shifts[0].department_id}`);
      }
    }
  }
} else {
  console.error(`No se pudieron obtener las horas laborales semanales registradas para el rango: ${rango.inicio} - ${rango.fin}`);
}

}
  return datos;
}

// async function createTables(pool) {
//   const tableQueries = [
//     `IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='MinimumIdealCoverage' AND xtype='U')
//     CREATE TABLE MinimumIdealCoverage (
//       id INT IDENTITY(1,1) PRIMARY KEY,
//       week DATE,
//       datos NVARCHAR(MAX),
//       department NVARCHAR(255)
//     )`,
//     `IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ScheduledCoverage' AND xtype='U')
//     CREATE TABLE ScheduledCoverage (
//       id INT IDENTITY(1,1) PRIMARY KEY,
//       week DATE,
//       datos NVARCHAR(MAX),
//       totalHoras FLOAT
//     )`,
//     `IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TransactionForecast' AND xtype='U')
//     CREATE TABLE TransactionForecast (
//       id INT IDENTITY(1,1) PRIMARY KEY,
//       week DATE,
//       datos NVARCHAR(MAX),
//       location NVARCHAR(255)
//     )`,
//     `IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ActualTransactions' AND xtype='U')
//     CREATE TABLE ActualTransactions (
//       id INT IDENTITY(1,1) PRIMARY KEY,
//       week DATE,
//       datos NVARCHAR(MAX),
//       datastream NVARCHAR(255)
//     )`,
//     `IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Items' AND xtype='U')
//     CREATE TABLE Items (
//       id INT IDENTITY(1,1) PRIMARY KEY,
//       week DATE,
//       datos NVARCHAR(MAX),
//       datastream NVARCHAR(255)
//     )`,
//     `IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TotalPunchesLaborHours' AND xtype='U')
//     CREATE TABLE TotalPunchesLaborHours (
//       id INT IDENTITY(1,1) PRIMARY KEY,
//       week DATE,
//       datos NVARCHAR(MAX),
//       totalHoras FLOAT
//     )`
//   ];

//   for (const query of tableQueries) {
//     await pool.request().query(query);
//   }
// }

// async function insertData(pool, tableName, week, datos, extraField = null) {
//   let query = `INSERT INTO ${tableName} (week, datos`;
//   if (extraField) {
//     query += `, ${extraField.name}`;
//   }
//   query += `) VALUES (@week, @datos`;
//   if (extraField) {
//     query += `, @${extraField.name}`;
//   }
//   query += `)`;

//   const request = pool.request()
//     .input('week', sql.Date, week)
//     .input('datos', sql.NVarChar(sql.MAX), JSON.stringify(datos));

//   if (extraField) {
//     request.input(extraField.name, sql.NVarChar(255), extraField.value);
//   }

//   await request.query(query);
// }

const rutaActual = path.dirname(__filename);
const rutaJsonTOTALIZADO = path.join(rutaActual, 'data', 'datos_tanda.json');
const rutaJsonCOMPLETO = path.join(rutaActual, 'data', 'datos_tanda_COMPLETOS.json');

async function main() {
  try {
    // Crear el directorio 'data' si no existe
    await mkdir(path.dirname(rutaJson), { recursive: true });

    console.log(`Obteniendo datos desde ${fechaInicio} hasta ${fechaFin}...`);

    const datosTOTALIZADOS = await obtenerDatosTandaApi(fechaInicio, fechaFin);
    const jsonDatosTOTALIZADOS = JSON.stringify(datos, null, 2);
    await writeFile(rutaJsonTOTALIZADO, jsonDatosTOTALIZADOS);

    const datosCOMPLETOS = await obtenerDatosCOMPLETOSTandaApi(fechaInicio, fechaFin);
    const jsonDatosCOMPLETOS = JSON.stringify(datos, null, 2);
    await writeFile(rutaJsonCOMPLETO, jsonDatosCOMPLETOS);

    console.log(`Datos guardados exitosamente en ${rutaJsonTOTALIZADO}`);

    // // Conectar a SQL Server
    // const pool = await sql.connect(sqlConfig);

    // // Crear tablas si no existen
    // await createTables(pool);

    // // Insertar datos en las tablas
    // for (const item of datos.minimumIdealCoverage) {
    //   await insertData(pool, 'MinimumIdealCoverage', item.week, item.datos, { name: 'department', value: item.department });
    // }
    // for (const item of datos.scheduledCoverage) {
    //   await insertData(pool, 'ScheduledCoverage', item.week, item.datos, { name: 'totalHoras', value: item.totalHoras });
    // }
    // for (const item of datos.transactionForecast) {
    //   await insertData(pool, 'TransactionForecast', item.week, item.datos, { name: 'location', value: item.location });
    // }
    // for (const item of datos.actualTransactions) {
    //   await insertData(pool, 'ActualTransactions', item.week, item.datos, { name: 'datastream', value: item.datastream });
    // }
    // for (const item of datos.Items) {
    //   await insertData(pool, 'Items', item.week, item.datos, { name: 'datastream', value: item.datastream });
    // }
    // for (const item of datos.totalPunchesLaborHours) {
    //   await insertData(pool, 'TotalPunchesLaborHours', item.week, item.datos, { name: 'totalHoras', value: item.totalHoras });
    // }

    console.log('Datos subidos exitosamente a SQL Server');
  } catch (error) {
    console.error('Error en la ejecución principal:', error);
  } finally {
    sql.close();
  }
}

main();