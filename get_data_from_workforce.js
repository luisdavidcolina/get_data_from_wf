const axios = require('axios');
const fs = require('fs');
const path = require('path');
const util = require('util');

const writeFile = util.promisify(fs.writeFile);
const mkdir = util.promisify(fs.mkdir);

const API_TOKEN = '62f368d294e6e4ce9f897702e913f1345d723611ea001c10282833942fd13c2c';
const BASE_URL = 'https://my.tanda.co/api/v2';

const headers = {
  Authorization: `Bearer ${API_TOKEN}`
};

// Definir las fechas de inicio y fihn
const fechaInicio = '2024-09-02';
const fechaFin = '2024-09-08';

async function obtenerDatos(endpoint, params = {}) {
  try {
    const response = await axios.get(`${BASE_URL}${endpoint}`, { headers, params });
    return response.data;
  } catch (error) {
    console.error(`Error al obtener datos de ${endpoint}:`, error.response ? error.response.data : error.message);
    return null;
  }
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

function extractMonthInSpanish(date) {
  const months = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
  ];
  const monthNumber = parseInt(date.split('-')[1], 10);
  return months[monthNumber - 1];
}

function generateWeeklyRanges(inicio, fin) {
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

  // Obtener la fecha actual en UTC para evitar problemas con zonas horarias
  date.setUTCHours(0, 0, 0, 0);

  const dayOfWeek = date.getUTCDay();

  // Retroceder hasta el lunes
  // Si es domingo (0), retrocedemos 6 días
  // Si es lunes (1), retrocedemos 0 días
  // Si es martes (2), retrocedemos 1 día
  // etc...
  const diff = dayOfWeek === 0 ? -6 : -dayOfWeek + 1;

  // Ajustar la fecha
  date.setUTCDate(date.getUTCDate() + diff);

  return date.toISOString().split('T')[0];
}

async function fetchMultipleWorkforceRequests(fechaInicio, fechaFin) {

  const rawData = {
    departments: [],
    locations: [],
    datastreams: [],
    minimumIdeal: [],
    scheduled: [],
    transactionForecast: [],
    actualTransactions: [],
    items: [],
    totalPunchesLaborHours: []
  };

  const kpisByWeek = {
    minimumIdealCoverage: [],
    minimumIdealNonCoverage: [],
    minimumIdealTraining: [],
    scheduledCoverage: [],
    scheduledNonCoverage: [],
    scheduledTraining: [],
    transactionForecast: [],
    actualTransactions: [],
    items: [],
    totalPunchesLaborHours: []
  };

  const rangosSemanales = generateWeeklyRanges(fechaInicio, fechaFin);

  let departamentos = await obtenerDatos('/departments');


  const departamentosRelevantesCompletosUnicos = departamentos.filter(departamento =>
    departamento.name === 'Baristas DT' ||
    departamento.name === 'Baristas' ||
    departamento.name === 'Supervisores' ||
    departamento.name === 'Supervisores DT' ||
    departamento.name === 'Non Coverage' ||
    departamento.name === 'Training'
  );

  const ubicaciones = await obtenerDatos('/locations');
  const datastreams = await obtenerDatos('/datastreams');
  const datastreamsJoins = await obtenerDatos('/datastreamjoins');

  rawData.departments = departamentosRelevantesCompletosUnicos.map((department) => {
    return { department_id: department.id, name: department.name }
  });

  rawData.locations= ubicaciones.map((location)=> {
    return { location_id: location.id, name: location.name}
  })

  rawData.datastreams = datastreams.map((datastream)=> {
    return {datastream_id: datastream.id, name: datastream.name}
  })


  let totalRecommendedHoursCoverage = {};
  let totalRecommendedHoursNonCoverage = {};
  let totalRecommendedHoursTraning = {}

  const locationHoursCoverage = {};
  const locationHoursNonCoverage = {};
  const locationHoursTraining = {};


  for (const rango of rangosSemanales) {

    const lunes = obtenerLunes(rango.inicio);


    for (const department of departamentosRelevantesCompletosUnicos) {
      const recommendedHours = await obtenerDatos('/recommended_hours', {
        from_date: rango.inicio,
        to_date: rango.fin,
        department_id: department.id
      });

      function transformarHorasPorFecha(recommended_hours_by_date) {
        return Object.entries(recommended_hours_by_date).map(([date, total]) => ({
          date,
          total
        }));
      }




      if (recommendedHours) {
        const location = ubicaciones.find(loc => loc.id === department.location_id);
        if (location) {

          const recommendedHoursFlattened = transformarHorasPorFecha(recommendedHours.recommended_hours_by_date).map((item) => ({
            ...item,
            department_name: department.name,
            department_id: department.id,
            location_id: location.id
          }));
          rawData.minimumIdeal.push(...recommendedHoursFlattened);


          if (department.name === 'Baristas DT' ||
            department.name === 'Baristas' ||
            department.name === 'Supervisores' ||
            department.name === 'Supervisores DT') {

            if (!totalRecommendedHoursCoverage[location.id]) {
              totalRecommendedHoursCoverage[location.id] = {
                week: lunes,
                location: location.name,
                location_id: location.id,
                total: 0
              };
            }
            totalRecommendedHoursCoverage[location.id].Total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);


          } else if (department.name === 'Non Coverage') {
            if (!totalRecommendedHoursNonCoverage[location.id]) {
              totalRecommendedHoursNonCoverage[location.id] = {
                week: lunes,
                location: location.name,
                location_id: location.id,
                total: 0
              };
            }
            totalRecommendedHoursNonCoverage[location.id].Total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);


          } else if (department.name === 'Training') {
            if (!totalRecommendedHoursTraning[location.id]) {
              totalRecommendedHoursTraning[location.id] = {
                week: lunes,
                location: location.name,
                location_id: location.id,
                total: 0
              };
            }
            totalRecommendedHoursTraning[location.id].Total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);

          }


          console.log(`Obtenidas las horas recomendadas para el departamento con ID: ${department.id}`)

        } else {
          console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
        }
      } else {
        console.error(`No se pudieron obtener las horas recomendadas para el departamento con ID: ${department.id} o el resultado no es un array`);
      }
    }


    const scheduled = await obtenerDatos(`/rosters/on/${rango.inicio}`, {
      show_costs: false
    });


    for (const department of departamentosRelevantesCompletosUnicos) {

      if (scheduled) {
        const location = ubicaciones.find(loc => loc.id === department.location_id);
        if (location) {
          const scheduledFlattened = scheduled.schedules.flatMap(schedule =>
            schedule.schedules.map(shift => {
              const breakLength = shift.breaks.reduce((total, b) => {
                const breakStart = new Date(b.start * 1000);
                const breakFinish = new Date(b.finish * 1000);
                return total + (breakFinish - breakStart) / (1000 * 60);
              }, 0);

              const total = ((shift.finish - shift.start) / 60 - breakLength) / 60

              const newShift = {
                ...shift,
                department_id: department.id,
                location_id: location.id,
                break_length: breakLength,
                roster_id: shift.id,
                date: convertEpochToDateTime(shift.start).slice(0, 10),
                time: convertEpochToDateTime(shift.start).slice(11, 16),
                week: lunes,
                month: extractMonthInSpanish(convertEpochToDateTime(shift.time).slice(0, 10)),
                total
              };

              delete newShift.breaks
              delete newShift.time_zone
              delete newShift.id
              delete newShift.automatic_break_length
              delete newShift.creation_method
              delete newShift.creation_platform
              delete newShift.acceptance_status
              delete newShift.last_acknowledged_at
              delete newShift.needs_acceptance
              delete newShift.utc_offset
              return newShift
            })
          );
          rawData.scheduled.push(...scheduledFlattened);

          if (department.name === 'Baristas DT' ||
            department.name === 'Baristas' ||
            department.name === 'Supervisores' ||
            department.name === 'Supervisores DT') {

            let totalHoras = 0;
            for (const schedule of scheduled.schedules) {
              for (const shift of schedule.schedules.filter(shift => shift.department_id === department.id)) {
                const startDate = new Date(shift.start * 1000);
                const finishDate = new Date(shift.finish * 1000);
                const diffMilliseconds = finishDate - startDate;
                const breakMilliseconds = shift.breaks.reduce((total, b) => total + (b.length * 60 * 1000), 0);
                const totalMilliseconds = diffMilliseconds - breakMilliseconds;
                totalHoras += totalMilliseconds / (1000 * 60 * 60);
              }
            }
            totalHoras = Math.round(totalHoras);

            if (!locationHoursCoverage[location.id]) {
              locationHoursCoverage[location.id] = {
                week: lunes,
                location: location.name,
                location_id: location.id,
                total: 0
              };
            }
            locationHoursCoverage[location.id].total += totalHoras;
            console.log(`Total horas Coverage para ${location.name} (${department.name}): ${totalHoras}`);

          } else if (department.name === 'Non Coverage') {
            let totalHoras = 0;
            for (const schedule of scheduled.schedules) {
              for (const shift of schedule.schedules.filter(shift => shift.department_id === department.id)) {
                const startDate = new Date(shift.start * 1000);
                const finishDate = new Date(shift.finish * 1000);
                const diffMilliseconds = finishDate - startDate;
                const breakMilliseconds = shift.breaks.reduce((total, b) => total + (b.length * 60 * 1000), 0);
                const totalMilliseconds = diffMilliseconds - breakMilliseconds;
                totalHoras += totalMilliseconds / (1000 * 60 * 60);
              }
            }
            totalHoras = Math.round(totalHoras);

            if (!locationHoursNonCoverage[location.id]) {
              locationHoursNonCoverage[location.id] = {
                week: lunes,
                location: location.name,
                location_id: location.id,
                total: 0
              };
            }
            locationHoursNonCoverage[location.id].total += totalHoras;
            console.log(`Total horas Non Coverage para ${location.name} (${department.name}): ${totalHoras}`);

          } else if (department.name === 'Training') {
            let totalHoras = 0;
            for (const schedule of scheduled.schedules) {
              for (const shift of schedule.schedules.filter(shift => shift.department_id === department.id)) {
                const startDate = new Date(shift.start * 1000);
                const finishDate = new Date(shift.finish * 1000);
                const diffMilliseconds = finishDate - startDate;
                const breakMilliseconds = shift.breaks.reduce((total, b) => total + (b.length * 60 * 1000), 0);
                const totalMilliseconds = diffMilliseconds - breakMilliseconds;
                totalHoras += totalMilliseconds / (1000 * 60 * 60);
              }
            }
            totalHoras = Math.round(totalHoras);

            if (!locationHoursTraining[location.id]) {
              locationHoursTraining[location.id] = {
                week: lunes,
                location: location.name,
                location_id: location.id,
                total: 0
              };
            }
            locationHoursTraining[location.id].total += totalHoras;
            console.log(`Total horas Training para ${location.name} (${department.name}): ${totalHoras}`);
          }

          console.log(`Se obtuvieron las coberturas programadas para el departamento con ID: ${department.id}`);
        }
      }
    }


    for (const location of ubicaciones) {
      try {
        const predictedTransactions = (await obtenerDatos(`/predicted_storestats/for_location/${location.id}`, {
          from: rango.inicio,
          to: rango.fin
        }));


        if (Array.isArray(predictedTransactions)) {
          let totalStats = 0;
          const allStats = [];

          // Procesar cada array de stats
          for (const transaction of predictedTransactions) {
            if (Array.isArray(transaction.stats)) {
              // Agregar a raw data
              const flattened = transaction.stats.filter(storeStat => storeStat.type === 'checks').map(item => {
                let newItem = {
                  ...item,
                  location_id: location.id,
                  predicted_storestats_id: item.id,
                  date: convertEpochToDateTime(item.time).slice(0, 10),
                  time: convertEpochToDateTime(item.time).slice(11, 16),
                  week: lunes,
                  month: extractMonthInSpanish(convertEpochToDateTime(item.time).slice(0, 10))
                }
                delete newItem.id
                return newItem
              });
              allStats.push(...flattened);

              // Sumar al total
              totalStats += transaction.stats.reduce((sum, stat) => sum + (stat.stat || 0), 0);
            }
          }

          // Agregar al raw data
          rawData.transactionForecast.push(...allStats);

          // Agregar a kpisByWeek
          kpisByWeek.transactionForecast.push({
            week: lunes,
            location_id: location.id,
            total: totalStats
          });
          console.log(`Obtenidas las transacciones pronosticadas para la ubicación con ID: ${location.id}`)
        } else {
          console.error(`No se pudieron obtener las transacciones pronosticadas para la ubicación con ID: ${location.id}`);
        }
      } catch (error) {
        console.error(`Error al obtener las transacciones pronosticadas para la ubicación con ID: ${location.id}`);
        console.error(error);
      }
    }



    for (const datastream of datastreams) {
      const datastreamJoin = datastreamsJoins.find(join => join.data_stream_id === datastream.id && join.data_streamable_type === 'Location');
      if (datastreamJoin) {
        const location = ubicaciones.find(loc => loc.id === datastreamJoin.data_streamable_id);
        const storeStats = await obtenerDatos(`/storestats/for_datastream/${datastream.id}`, {
          from: rango.inicio,
          to: rango.fin
        });

        if (storeStats) {

          if ((storeStats[0]) && (storeStats[0].type) && (storeStats[0].type === 'checks')) {

            const actualTransactionsFlattened = storeStats.map((item) => {
              let newItem = {
                ...item,
                location_id: location.id,
                storestats_id: item.id,
                date: convertEpochToDateTime(item.time).slice(0, 10),
                time: convertEpochToDateTime(item.time).slice(11, 16),
                week: lunes,
                month: extractMonthInSpanish(convertEpochToDateTime(item.time).slice(0, 10))
              }
              delete newItem.id
              return newItem
            });

            rawData.actualTransactions.push(...actualTransactionsFlattened);

            const totalChecks = storeStats.reduce((sum, transaction) => sum + transaction.stat, 0);

            kpisByWeek.actualTransactions.push({
              week: lunes,
              location_id: location.id,
              total: totalChecks
            });
          }

          else if ((storeStats[0]) && (storeStats[0].type) && (storeStats[0].type === 'sales count')) {
            const actualSalesFlattened = storeStats.map((item) => {
              let newItem = {
                ...item,
                location_id: location.id,
                storestats_id: item.id,
                date: convertEpochToDateTime(item.time).slice(0, 10),
                time: convertEpochToDateTime(shift.start).slice(11, 16),
                week: lunes,
                month: extractMonthInSpanish(convertEpochToDateTime(item.time).slice(0, 10))
              }
              delete newItem.id
              return newItem
            });
            rawData.items.push(...actualSalesFlattened);


            const totalSales = storeStats.reduce((sum, sale) => sum + sale.stat, 0);
            kpisByWeek.items.push({
              week: lunes,
              location_id: location.id,
              total: totalSales
            });
          }


        }
        console.log(`Se obtuvieron los stats para el datastream con ID: ${datastream.id}`);
      }
      
    }



    const totalWeeklyWorkedHours = await obtenerDatos(`/timesheets/on/${rango.inicio}`, {
      show_costs: false,
      show_award_interpretation: false
    });

    if (Array.isArray(totalWeeklyWorkedHours)) {
      for (const hours of totalWeeklyWorkedHours) {
        if (hours.status === 'approved' && hours && hours.shifts[0] && hours.shifts[0].department_id) {
          const department = departamentos.find(dep => dep.id === hours.shifts[0].department_id);
          if (department) {
            const location = ubicaciones.find(loc => loc.id === department.location_id);
            if (location) {
              const shiftsFlattened = hours.shifts.filter(shift => {
                const shiftDate = new Date(shift.date);
                const inicio = new Date(rango.inicio);
                const fin = new Date(rango.fin);

                return shiftDate >= inicio && shiftDate <= fin;
              }).map(shift => {
                let shift_id = shift.id
                const { id, break_finish, break_start, updated_at, breaks, tag, sub_cost_centre, tag_id, metadata, leave_request_id, allowances, approved_by, approved_at, award_interpretation, ...rest } = shift;

                const total = ((shift.finish - shift.start) / 60 - shift.break_length) / 60

                return {
                  ...rest,
                  location_id: location.id,
                  shift_id,
                  break_length: breaks.reduce((total, b) => total + b.length, 0),
                  week: lunes,
                  time: convertEpochToDateTime(shift.start).slice(11, 16),
                  month: extractMonthInSpanish(convertEpochToDateTime(shift.time).slice(0, 10)),
                  total
                };
              });

              rawData.totalPunchesLaborHours.push(...shiftsFlattened);
            }
          }
        }

        if (Array.isArray(totalWeeklyWorkedHours)) {
          let totalHoras = 0;
          for (const hours of totalWeeklyWorkedHours) {
            if (hours.status === 'approved' && hours && hours.shifts[0] && hours.shifts[0].department_id) {


              for (const shift of hours.shifts.filter(shift => {
                const shiftDate = new Date(shift.date);
                const inicio = new Date(rango.inicio);
                const fin = new Date(rango.fin);

                return shiftDate >= inicio && shiftDate <= fin;
              })) {
                const startDate = new Date(shift.start * 1000);
                const finishDate = new Date(shift.finish * 1000);
                const diffMilliseconds = finishDate - startDate;
                const breakMilliseconds = shift.break_length * 60 * 1000;
                const totalMilliseconds = diffMilliseconds - breakMilliseconds;
                totalHoras += totalMilliseconds / (1000 * 60 * 60);
              }


              const department = departamentos.find(dep => dep.id === hours.shifts[0].department_id);
              if (department) {
                const location = ubicaciones.find(loc => loc.id === department.location_id);
                if (location) {
                  kpisByWeek.totalPunchesLaborHours.push({
                    week: lunes,
                    location: location.name,
                    location_id: location.id,
                    total: totalHoras
                  });
                } else {
                  console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
                }
              } else {
                console.error(`No se pudo encontrar el departamento con ID: ${hours.shifts[0].department_id}`);
              }
            }
          }
          console.log(`Se obtuvieron las horas laborales semanales registradas para el rango: ${rango.inicio} - ${rango.fin}`);
        } else {
          console.error(`No se pudieron obtener las horas laborales semanales registradas para el rango: ${rango.inicio} - ${rango.fin}`);
        }


      }

    }



    kpisByWeek.minimumIdealCoverage.push(...Object.values(totalRecommendedHoursCoverage));
    kpisByWeek.minimumIdealNonCoverage.push(...Object.values(totalRecommendedHoursNonCoverage));
    kpisByWeek.minimumIdealTraining.push(...Object.values(totalRecommendedHoursTraning));

    // Convertir los objetos locationHours en arrays y agregarlos a kpisByWeek
    kpisByWeek.scheduledCoverage = Object.values(locationHoursCoverage);
    kpisByWeek.scheduledNonCoverage = Object.values(locationHoursNonCoverage);
    kpisByWeek.scheduledTraining = Object.values(locationHoursTraining);

  }

  function aggregateByWeekAndLocation(data) {
    const result = [];

    data.forEach(item => {
      // Busca si ya existe un objeto en `result` con la misma week y location_id
      const existingEntry = result.find(entry =>
        entry.week === item.week && entry.location_id === item.location_id
      );

      if (existingEntry) {
        // Si ya existe, suma el total
        existingEntry.total += item.total;
      } else {
        // Si no existe, agrega una nueva entrada con los datos del objeto actual
        result.push({
          week: item.week,
          location: item.location,
          location_id: item.location_id,
          total: item.total
        });
      }
    });

    return result;
  }

  kpisByWeek.items = aggregateByWeekAndLocation(kpisByWeek.items)
  kpisByWeek.actualTransactions = aggregateByWeekAndLocation(kpisByWeek.actualTransactions)

  return { kpisByWeek, rawData };
}


async function main() {

  console.log(`Obteniendo datos desde ${fechaInicio} hasta ${fechaFin}...`);

  const rutaActual = path.dirname(__filename);
  const rutaJsonTOTALIZADO = path.join(rutaActual, 'data', 'kpis_by_week.json');
  const rutaJsonTOTALIZADOPorDia = path.join(rutaActual, 'data', 'kpis_by_date.json');
  const rutaJsonCOMPLETO = path.join(rutaActual, 'data', 'raw_data.json');

  try {
    await mkdir(path.dirname(rutaJsonTOTALIZADO), { recursive: true });
    await mkdir(path.dirname(rutaJsonCOMPLETO), { recursive: true });

    const datosObtenidos = await fetchMultipleWorkforceRequests(fechaInicio, fechaFin);

    const jsonDatosTOTALIZADOS = JSON.stringify(datosObtenidos.kpisByWeek || {}, null, 2);
    const jsonDatosCOMPLETOS = JSON.stringify(datosObtenidos.rawData || [], null, 2);

    await writeFile(rutaJsonTOTALIZADO, jsonDatosTOTALIZADOS);
    await writeFile(rutaJsonCOMPLETO, jsonDatosCOMPLETOS);

    console.log(`Datos guardados exitosamente en ${rutaJsonTOTALIZADO} y ${rutaJsonCOMPLETO}`);


  } catch (error) {
    console.error('Error al obtener o guardar los datos:', error);
  }
}

main();