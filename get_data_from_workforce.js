const axios = require('axios');
const fs = require('fs');
const path = require('path');
const util = require('util');
const { loadJsonToSql } = require('./load_json_to_sql');

const writeFile = util.promisify(fs.writeFile);
const mkdir = util.promisify(fs.mkdir);

const API_TOKEN = '62f368d294e6e4ce9f897702e913f1345d723611ea001c10282833942fd13c2c';
const BASE_URL = 'https://my.tanda.co/api/v2';

const headers = {
  Authorization: `Bearer ${API_TOKEN}`
};

const datestart = '2024-09-09';
const datefinish = '2024-10-27';

async function getDatos(endpoint, params = {}) {
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


function generateWeeklyRanges(start, finish) {
  const ranges = [];
  let dateActual = new Date(start);
  const dateFinal = new Date(finish);

  while (dateActual <= dateFinal) {
    const startweek = new Date(dateActual);
    const finweek = new Date(dateActual.setDate(dateActual.getDate() + 6));

    if (finweek > dateFinal) {
      ranges.push({
        start: startweek.toISOString().split('T')[0],
        finish: dateFinal.toISOString().split('T')[0]
      });
    } else {
      ranges.push({
        start: startweek.toISOString().split('T')[0],
        finish: finweek.toISOString().split('T')[0]
      });
    }

    dateActual.setDate(dateActual.getDate() + 1);
  }

  return ranges;
}



async function fetchMultipleWorkforceRequests(datestart, dateFinish) {

  const rawData = {
    locations: [],
    departments: [],
    datastreams: [],
    minimumIdeal: [],
    scheduled: [],
    transactionForecast: [],
    actualTransactions: [],
    items: [],
    totalPunchesLaborHours: []
  };


  const WeeklyRanges = generateWeeklyRanges(datestart, dateFinish);

  let departamentos = await getDatos('/departments');


  const departamentosRelevantesCompletosUnicos = departamentos.filter(departamento =>
    departamento.name === 'Baristas DT' ||
    departamento.name === 'Baristas' ||
    departamento.name === 'Supervisores' ||
    departamento.name === 'Supervisores DT' ||
    departamento.name === 'Non Coverage' ||
    departamento.name === 'Training'
  );

  const ubicaciones = await getDatos('/locations');
  const datastreams = await getDatos('/datastreams');
  const datastreamsJoins = await getDatos('/datastreamjoins');
/*
  
  rawData.departments = departamentosRelevantesCompletosUnicos.map((department) => {
    return { department_id: department.id, name: department.name }
  });

  rawData.locations= ubicaciones.map((location)=> {
    return { location_id: location.id, name: location.name}
  })

  rawData.datastreams = datastreams.map((datastream)=> {
    return {datastream_id: datastream.id, name: datastream.name}
  })
*/

  for (const range of WeeklyRanges) {

    console.log(`Recorriendo semana ${range.start} hasta ${range.finish}...`);

    const rutaJsonCOMPLETO = path.join(__dirname, 'data', 'raw_data.json');


    for (const department of departamentosRelevantesCompletosUnicos) {
      const recommendedHours = await getDatos('/recommended_hours', {
        from_date: range.start,
        to_date: range.finish,
        department_id: department.id
      });

      function TransformHoursFordate(recommended_hours_by_date) {
        return Object.entries(recommended_hours_by_date).map(([date, total]) => ({
          date,
          total
        }));
      }


      if (recommendedHours) {
        const location = ubicaciones.find(loc => loc.id === department.location_id);
        if (location) {

          const recommendedHoursFlattened = TransformHoursFordate(recommendedHours.recommended_hours_by_date).map((item) => ({
            ...item,
            department_id: department.id,
            location_id: location.id
          }));
          rawData.minimumIdeal.push(...recommendedHoursFlattened);




          console.log(`Obtenidas las horas recomendadas para el departamento con ID: ${department.id}`)

        } else {
          console.error(`No se pudo encontrar la ubicación para el departamento con ID: ${department.id}`);
        }
      } else {
        console.error(`No se pudieron obtener las horas recomendadas para el departamento con ID: ${department.id} o el resultado no es un array`);
      }
    }


    const scheduled = await getDatos(`/rosters/on/${range.start}`, {
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
                total
              };

              delete newShift.breaks
              delete newShift.time_zone
              delete newShift.id
              delete newShift.automatic_break_length
              delete newShift.shift_detail_id
              delete newShift.creation_method
              delete newShift.creation_platform
              delete newShift.acceptance_status
              delete newShift.last_acknowledged_at
              delete newShift.needs_acceptance
              delete newShift.utc_offset
              return newShift
            })
          ).filter(scheduled => scheduled.last_published_at !== null);
          rawData.scheduled.push(...scheduledFlattened);




          console.log(`Se obtuvieron las coberturas programadas para el departamento con ID: ${department.id}`);
        }
      }
    }


    for (const location of ubicaciones) {
      try {
        const toDate = new Date(range.finish);
        toDate.setDate(toDate.getDate() + 1);
        const predictedTransactions = await getDatos(`/predicted_storestats/for_location/${location.id}`, {
          from: range.start,
          to: toDate.toISOString().split('T')[0]
        });

        if (Array.isArray(predictedTransactions)) {
          const allStats = [];

          for (const transaction of predictedTransactions) {
            if (Array.isArray(transaction.stats)) {
              const filteredStats = transaction.stats.filter(stat => {
                const statDate = new Date(convertEpochToDateTime(stat.time).slice(0, 10));
                const startDate = new Date(range.start);
                const finDate = new Date(range.finish);
                return statDate >= startDate && statDate <= finDate;
              });

              const flattened = filteredStats.filter(storeStat => storeStat.type === 'checks' && Number(storeStat.stat) > 0).map(item => {
                let newItem = {
                  ...item,
                  location_id: location.id,
                }
                delete newItem.type
                delete newItem.id;
                return newItem;
              })
              allStats.push(...flattened);

            }
          }

          rawData.transactionForecast.push(...allStats);


          console.log(`Obtenidas las transacciones pronosticadas para la ubicación con ID: ${location.id}`);
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

        const toDate = new Date(range.finish);
        toDate.setDate(toDate.getDate() + 1);
        const storeStats = await getDatos(`/storestats/for_datastream/${datastream.id}`, {
          from: range.start,
          to: toDate.toISOString().split('T')[0]
        });

        if (storeStats) {

          const filteredStats = storeStats.filter(stat => {
            const statDate = new Date(convertEpochToDateTime(stat.time).slice(0, 10));
            const startDate = new Date(range.start);
            const finDate = new Date(range.finish);
            return statDate >= startDate && statDate <= finDate;
          });

          if ((filteredStats[0]) && (filteredStats[0].type) && (filteredStats[0].type === 'checks')) {

            const actualTransactionsFlattened = filteredStats.map((item) => {
              let newItem = {
                ...item,
                location_id: location.id,
                storestats_id: item.id,
              }
              delete newItem.type
              delete newItem.id
              return newItem
            }).filter(storeStat => Number(storeStat.stat) > 0);

            rawData.actualTransactions.push(...actualTransactionsFlattened);

          }

          else if ((filteredStats[0]) && (filteredStats[0].type) && (filteredStats[0].type === 'sales count')) {
            const actualSalesFlattened = filteredStats.map((item) => {
              let newItem = {
                ...item,
                location_id: location.id,
                storestats_id: item.id,
              }
              delete newItem.id
              delete newItem.type
              return newItem
            }).filter(storeStat => Number(storeStat.stat) > 0);
            rawData.items.push(...actualSalesFlattened);

          }


        }
        console.log(`Se obtuvieron los stats para el datastream con ID: ${datastream.id}`);
      }

    }



    const totalWeeklyWorkedHours = await getDatos(`/timesheets/on/${range.start}`, {
      show_costs: false,
      show_award_interpretation: false
    });

    if (Array.isArray(totalWeeklyWorkedHours)) {
      const totalHorasPorTienda = {};

      for (const hours of totalWeeklyWorkedHours) {
        if (hours.status === 'approved' && hours && hours.shifts[0] && hours.shifts[0].department_id) {
          const department = departamentos.find(dep => dep.id === hours.shifts[0].department_id);
          if (department) {
            const location = ubicaciones.find(loc => loc.id === department.location_id);
            if (location) {
              if (!totalHorasPorTienda[location.id]) {
                totalHorasPorTienda[location.id] = 0;
              }

              const shiftsFlattened = hours.shifts.filter(shift => {
                const shiftDate = new Date(shift.date);
                const start = new Date(range.start);
                const finish = new Date(range.finish);

                return shiftDate >= start && shiftDate <= finish;
              }).map(shift => {
                let shift_id = shift.id;
                const { id, break_finish, timesheet_id, record_id, break_start, updated_at, breaks, tag, sub_cost_centre, tag_id, metadata, leave_request_id, allowances, approved_by, approved_at, award_interpretation, ...rest } = shift;

                const total = ((shift.finish - shift.start) / 60 - shift.break_length) / 60;
                totalHorasPorTienda[location.id] += total;

                return {
                  ...rest,
                  location_id: location.id,
                  shift_id,
                };
              });

              rawData.totalPunchesLaborHours.push(...shiftsFlattened);
            }
          }
        }
      }


      console.log(`Se obtuvieron las horas laborales semanales registradas para el range: ${range.start} - ${range.finish}`);
    } else {
      console.error(`No se pudieron obtener las horas laborales semanales registradas para el range: ${range.start} - ${range.finish}`);
    }




    try {

      await mkdir(path.dirname(rutaJsonCOMPLETO), { recursive: true });

      await writeFile(rutaJsonCOMPLETO, JSON.stringify(rawData, null, 2));

      console.log(`Datos guardados para el range ${range.start} en ${rutaJsonCOMPLETO}`);
    } catch (error) {
      console.error('Error al guardar los JSON:', error);
    }

    try {
      await loadJsonToSql();
      console.log(`Datos cargados a SQL exitosamente para el range ${range.start}`);
    } catch (error) {
      console.error(`Error en la carga de datos a SQL para el range ${range.start}:`, error);
    }

    console.log('Fin del proceso para el range:', range);

  }





}


async function main() {

  console.log(`Obteniendo datos desde ${datestart} hasta ${datefinish}...`);



  try {

    await fetchMultipleWorkforceRequests(datestart, datefinish);


  } catch (error) {
    console.error('Error al obtener o guardar los datos:', error);
  }
}

main();

