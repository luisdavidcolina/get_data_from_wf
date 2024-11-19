const axios = require('axios');
const fs = require('fs');
const path = require('path');
const util = require('util');
const { loadJsonToSql} = require('./load_json_to_sql');

const writeFile = util.promisify(fs.writeFile);
const mkdir = util.promisify(fs.mkdir);

const API_TOKEN = '62f368d294e6e4ce9f897702e913f1345d723611ea001c10282833942fd13c2c';
const BASE_URL = 'https://my.tanda.co/api/v2';

const headers = {
  Authorization: `Bearer ${API_TOKEN}`
};

const datestart = '2024-09-02';
const datefinish= '2024-09-08';

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

function extractMonthInSpanish(date) {
  const months = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
  ];
  const monthNumber = parseInt(date.split('-')[1], 10);
  return months[monthNumber - 1];
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

function getMonday(dateStartOfRange) {
  const date = new Date(dateStartOfRange);

  date.setUTCHours(0, 0, 0, 0);

  const dayOfWeek = date.getUTCDay();

  const diff = dayOfWeek === 0 ? -6 : -dayOfWeek + 1;

  date.setUTCDate(date.getUTCDate() + diff);

  return date.toISOString().split('T')[0];
}

function aggregateByWeekAndLocation(data) {
  const result = [];

  data.forEach(item => {
    const existingEntry = result.find(entry =>
      entry.week === item.week && entry.location_id === item.location_id
    );

    if (existingEntry) {
      existingEntry.total += item.total;
    } else {
      result.push({
        week: item.week,
        location_id: item.location_id,
        total: item.total
      });
    }
  });

  return result;
}

async function fetchMultipleWorkforceRequests(datestart, dateFinish) {

  const rawData = {
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
    varianceToIdealCoverage: [],
    varianceToIdealNonCoverage : [],
    varianceToIdealTraining: [],
    totalIdealWeeklyLaborHours: [],
    totalScheduledWeeklyLaborHours: [],
    totalVariancesToIdealSummations: [],
    nonCoveragePorcentage: [],
    transactionForecast: [],
    actualTransactions: [],
    forecastAcuraccy: [],
    items: [],
    TPLH: [],
    IPLH: [],
    totalPunchesLaborHours: [],
    totalVarianceToIdealLaborHours: []
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

  let totalRecommendedHoursCoverage = {};
  let totalRecommendedHoursNonCoverage = {};
  let totalRecommendedHoursTraning = {}

  const locationHoursCoverage = {};
  const locationHoursNonCoverage = {};
  const locationHoursTraining = {};


  for (const range of WeeklyRanges) {

    const monday = getMonday(range.start);

    const rutaJsonTOTALIZADO = path.join(__dirname, 'data', 'kpis_by_week.json');
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
                week: monday,
                location_id: location.id,
                total: 0
              };
            }
            totalRecommendedHoursCoverage[location.id].total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);



          } else if (department.name === 'Non Coverage') {
            if (!totalRecommendedHoursNonCoverage[location.id]) {
              totalRecommendedHoursNonCoverage[location.id] = {
                week: monday,
                location_id: location.id,
                total: 0
              };
            }
            totalRecommendedHoursNonCoverage[location.id].total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);


          } else if (department.name === 'Training') {
            if (!totalRecommendedHoursTraning[location.id]) {
              totalRecommendedHoursTraning[location.id] = {
                week: monday,
                location_id: location.id,
                total: 0
              };
            }
            totalRecommendedHoursTraning[location.id].total += parseFloat(recommendedHours.total_recommended_hours_for_date_range);

          }


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
                date: convertEpochToDateTime(shift.start).slice(0, 10),
                time_start: convertEpochToDateTime(shift.start).slice(11, 16),
                time_finish: convertEpochToDateTime(shift.finish).slice(11, 16),
                week: monday,
                month: extractMonthInSpanish(convertEpochToDateTime(shift.start).slice(0, 10)),
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
                week: monday,
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
                week: monday,
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
                week: monday,
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
        const toDate = new Date(range.finish);
        toDate.setDate(toDate.getDate() + 1);
        const predictedTransactions = await getDatos(`/predicted_storestats/for_location/${location.id}`, {
          from: range.start,
          to: toDate.toISOString().split('T')[0]
        });
    
        if (Array.isArray(predictedTransactions)) {
          let totalStats = 0;
          const allStats = [];
    
          for (const transaction of predictedTransactions) {
            if (Array.isArray(transaction.stats)) {
              const filteredStats = transaction.stats.filter(stat => {
                const statDate = new Date(convertEpochToDateTime(stat.time).slice(0, 10));
                const startDate = new Date(range.start);
                const finDate = new Date(range.finish);
                return statDate >= startDate && statDate <= finDate;
              });
    
              const flattened = filteredStats.filter(storeStat => storeStat.type === 'checks').map(item => {
                let newItem = {
                  ...item,
                  location_id: location.id,
                  predicted_storestats_id: item.id,
                  date: convertEpochToDateTime(item.time).slice(0, 10),
                  time: convertEpochToDateTime(item.time).slice(11, 16),
                  week: monday,
                  month: extractMonthInSpanish(convertEpochToDateTime(item.time).slice(0, 10))
                }
                delete newItem.id;
                return newItem;
              });
              allStats.push(...flattened);
    
              totalStats += filteredStats.reduce((sum, stat) => sum + (stat.stat || 0), 0);
            }
          }
    
          rawData.transactionForecast.push(...allStats);
    
          kpisByWeek.transactionForecast.push({
            week: monday,
            location_id: location.id,
            total: totalStats
          });
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
                date: convertEpochToDateTime(item.time).slice(0, 10),
                time: convertEpochToDateTime(item.time).slice(11, 16),
                week: monday,
                month: extractMonthInSpanish(convertEpochToDateTime(item.time).slice(0, 10))
              }
              delete newItem.id
              return newItem
            });

            rawData.actualTransactions.push(...actualTransactionsFlattened);

            const totalChecks = filteredStats.reduce((sum, transaction) => sum + transaction.stat, 0);

            kpisByWeek.actualTransactions.push({
              week: monday,
              location_id: location.id,
              total: totalChecks
            });
          }

          else if ((filteredStats[0]) && (filteredStats[0].type) && (filteredStats[0].type === 'sales count')) {
            const actualSalesFlattened = filteredStats.map((item) => {
              let newItem = {
                ...item,
                location_id: location.id,
                storestats_id: item.id,
                date: convertEpochToDateTime(item.time).slice(0, 10),
                time: convertEpochToDateTime(item.time).slice(11, 16),
                week: monday,
                month: extractMonthInSpanish(convertEpochToDateTime(item.time).slice(0, 10))
              }
              delete newItem.id
              return newItem
            });
            rawData.items.push(...actualSalesFlattened);


            const totalSales = filteredStats.reduce((sum, sale) => sum + sale.stat, 0);
            kpisByWeek.items.push({
              week: monday,
              location_id: location.id,
              total: totalSales
            });
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
      for (const hours of totalWeeklyWorkedHours) {
        if (hours.status === 'approved' && hours && hours.shifts[0] && hours.shifts[0].department_id) {
          const department = departamentos.find(dep => dep.id === hours.shifts[0].department_id);
          if (department) {
            const location = ubicaciones.find(loc => loc.id === department.location_id);
            if (location) {
              const shiftsFlattened = hours.shifts.filter(shift => {
                const shiftDate = new Date(shift.date);
                const start = new Date(range.start);
                const finish= new Date(range.finish);

                return shiftDate >= start && shiftDate <= finish;
              }).map(shift => {
                let shift_id = shift.id
                const { id, break_finish, break_start, updated_at, breaks, tag, sub_cost_centre, tag_id, metadata, leave_request_id, allowances, approved_by, approved_at, award_interpretation, ...rest } = shift;

                const total = ((shift.finish - shift.start) / 60 - shift.break_length) / 60

                return {
                  ...rest,
                  location_id: location.id,
                  shift_id,
                  break_length: breaks.reduce((total, b) => total + b.length, 0),
                  week: monday,
                  time_start: convertEpochToDateTime(shift.start).slice(11, 16),
                  time_finish: convertEpochToDateTime(shift.finish).slice(11, 16),
                  month: extractMonthInSpanish(convertEpochToDateTime(shift.start).slice(0, 10)),
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
                const start = new Date(range.start);
                const finish= new Date(range.finish);

                return shiftDate >= start && shiftDate <= finish;
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
                    week: monday,
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
          console.log(`Se obtuvieron las horas laborales semanales registradas para el range: ${range.start} - ${range.finish}`);
        } else {
          console.error(`No se pudieron obtener las horas laborales semanales registradas para el range: ${range.start} - ${range.finish}`);
        }


      }

    }



    kpisByWeek.minimumIdealCoverage.push(...Object.values(totalRecommendedHoursCoverage));
    kpisByWeek.minimumIdealNonCoverage.push(...Object.values(totalRecommendedHoursNonCoverage));
    kpisByWeek.minimumIdealTraining.push(...Object.values(totalRecommendedHoursTraning));

    kpisByWeek.scheduledCoverage = Object.values(locationHoursCoverage);
    kpisByWeek.scheduledNonCoverage = Object.values(locationHoursNonCoverage);
    kpisByWeek.scheduledTraining = Object.values(locationHoursTraining);  

     
    kpisByWeek.items = aggregateByWeekAndLocation(kpisByWeek.items)
    kpisByWeek.actualTransactions = aggregateByWeekAndLocation(kpisByWeek.actualTransactions)
    

    // Agrupar los datos por tienda para Coverage
    const groupedScheduledCoverage = kpisByWeek.scheduledCoverage.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});

    const groupedMinimumIdealCoverage = kpisByWeek.minimumIdealCoverage.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});


    // Calcular la variación para cada tienda en Coverage
    Object.keys(groupedScheduledCoverage).forEach(location_id => {
      const scheduled = groupedScheduledCoverage[location_id];
      const ideal = groupedMinimumIdealCoverage[location_id];

      if (ideal) {
        const variance = (scheduled.total / ideal.total) - 1;
        kpisByWeek.varianceToIdealCoverage.push({
          week: monday,
          location_id: scheduled.location_id,
          total: variance ?? 0
        });
      }
    });

    // Agrupar los datos por tienda para Non Coverage
    const groupedScheduledNonCoverage = kpisByWeek.scheduledNonCoverage.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});

    const groupedMinimumIdealNonCoverage = kpisByWeek.minimumIdealNonCoverage.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});

    // Calcular la variación para cada tienda en Non Coverage
    Object.keys(groupedScheduledNonCoverage).forEach(location_id => {
      const scheduled = groupedScheduledNonCoverage[location_id];
      const ideal = groupedMinimumIdealNonCoverage[location_id];

      if (ideal) {
        const variance = (scheduled.total / ideal.total) - 1;
        kpisByWeek.varianceToIdealNonCoverage.push({
          week: monday,
          location_id: scheduled.location_id,
          total: variance ?? 0
        });
      }
    });

    // Agrupar los datos por tienda para Training
    const groupedScheduledTraining = kpisByWeek.scheduledTraining.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});

    const groupedMinimumIdealTraining = kpisByWeek.minimumIdealTraining.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});

    // Calcular la variación para cada tienda en Training
    Object.keys(groupedScheduledTraining).forEach(location_id => {
      const scheduled = groupedScheduledTraining[location_id];
      const ideal = groupedMinimumIdealTraining[location_id];

      if (ideal) {
        const variance = (scheduled.total / ideal.total) - 1;
        kpisByWeek.varianceToIdealTraining.push({
          week: monday,
          location_id: scheduled.location_id,
          total: variance   ?? 0
        });
      }
    });

    console.log(`Variance to Ideal Coverage %: ${JSON.stringify(kpisByWeek.varianceToIdealCoverage, null, 2)}`);
    console.log(`Variance to Ideal Non Coverage %: ${JSON.stringify(kpisByWeek.varianceToIdealNonCoverage, null, 2)}`);
    console.log(`Variance to Ideal Training %: ${JSON.stringify(kpisByWeek.varianceToIdealTraining, null, 2)}`);

    // Calcular Total Ideal Weekly Labor Hours y Total Scheduled Weekly Labor Hours por tienda
    Object.keys(groupedScheduledCoverage).forEach(location_id => {
      const scheduledCoverage = groupedScheduledCoverage[location_id] || { total: 0 };
      const scheduledNonCoverage = groupedScheduledNonCoverage[location_id] || { total: 0 };
      const scheduledTraining = groupedScheduledTraining[location_id] || { total: 0 };

      const minimumIdealCoverage = groupedMinimumIdealCoverage[location_id] || { total: 0 };
      const minimumIdealNonCoverage = groupedMinimumIdealNonCoverage[location_id] || { total: 0 };
      const minimumIdealTraining = groupedMinimumIdealTraining[location_id] || { total: 0 };

      const totalIdealWeeklyLaborHours = minimumIdealCoverage.total + minimumIdealNonCoverage.total + minimumIdealTraining.total;
      const totalScheduledWeeklyLaborHours = scheduledCoverage.total + scheduledNonCoverage.total + scheduledTraining.total;

      const totalVarianceToIdeal = (totalScheduledWeeklyLaborHours / totalIdealWeeklyLaborHours) - 1;

      kpisByWeek.totalIdealWeeklyLaborHours.push({
        week: monday,
        location_id: location_id,
        total: totalIdealWeeklyLaborHours
      });

      kpisByWeek.totalScheduledWeeklyLaborHours.push({
        week: monday,
        location_id: location_id,
        total: totalScheduledWeeklyLaborHours
      });

      kpisByWeek.totalVariancesToIdealSummations.push({
        week: monday,
        location_id: location_id,
        total: totalVarianceToIdeal
      });

      // Calcular nonCoveragePorcentage
      const nonCoveragePorcentage = (minimumIdealNonCoverage.total + minimumIdealTraining.total) / totalIdealWeeklyLaborHours;
      kpisByWeek.nonCoveragePorcentage.push({
        week: monday,
        location_id: location_id,
        total: nonCoveragePorcentage ?? 0
      });
    });

    // Agrupar los datos por tienda para transactionForecast y actualTransactions
    const groupedTransactionForecast = kpisByWeek.transactionForecast.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});

    const groupedActualTransactions = kpisByWeek.actualTransactions.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});

    // Calcular forecastAcuraccy para cada tienda
    Object.keys(groupedTransactionForecast).forEach(location_id => {
      const forecast = groupedTransactionForecast[location_id];
      const actual = groupedActualTransactions[location_id];

      if (actual) {
        const accuracy = forecast.total / actual.total;
        kpisByWeek.forecastAcuraccy.push({
          week: monday,
          location_id: forecast.location_id,
          total: accuracy ?? 0
        });
      }
    });

    // Agrupar los datos por tienda para totalPunchesLaborHours
    const groupedTotalPunchesLaborHours = kpisByWeek.totalPunchesLaborHours.reduce((acc, item) => {
      if (!acc[item.location_id]) {
        acc[item.location_id] = { ...item, total: 0 };
      }
      acc[item.location_id].total += item.total;
      return acc;
    }, {});

    // Calcular totalVarianceToIdealLaborHours para cada tienda
    Object.keys(groupedTotalPunchesLaborHours).forEach(location_id => {
      const punches = groupedTotalPunchesLaborHours[location_id];
      const ideal = kpisByWeek.totalIdealWeeklyLaborHours.find(item => item.location_id === location_id);

      if (ideal) {
        const variance = (punches.total / ideal.total) - 1;
        kpisByWeek.totalVarianceToIdealLaborHours.push({
          week: monday,
          location_id: punches.location_id,
          total: variance ?? 0
        });
      }
    });

    // Calcular TPLH y IPLH para cada tienda
    Object.keys(groupedActualTransactions).forEach(location_id => {
      const actualTransactions = groupedActualTransactions[location_id];
      const totalPunches = groupedTotalPunchesLaborHours[location_id] || { total: 0 };
      const scheduledTraining = groupedScheduledTraining[location_id] || { total: 0 };
      const scheduledNonCoverage = groupedScheduledNonCoverage[location_id] || { total: 0 };
      const items = kpisByWeek.items.find(item => item.location_id === location_id) || { total: 0 };

      const denominator = totalPunches.total - scheduledTraining.total - scheduledNonCoverage.total;

      if (denominator > 0) {
        const tplh = actualTransactions.total / denominator;
        const iplh = (items.total * actualTransactions.total) / denominator;

        kpisByWeek.TPLH.push({
          week: monday,
          location_id: location_id,
          total: tplh ?? 0
        });

        kpisByWeek.IPLH.push({
          week: monday,
          location_id: location_id,
          total: iplh  ?? 0
        });
      }
    });

    console.log(`Total Ideal Weekly Labor Hours: ${JSON.stringify(kpisByWeek.totalIdealWeeklyLaborHours, null, 2)}`);
    console.log(`Total Scheduled Weekly Labor Hours: ${JSON.stringify(kpisByWeek.totalScheduledWeeklyLaborHours, null, 2)}`);
    console.log(`Total Variance to Ideal: ${JSON.stringify(kpisByWeek.totalVarianceToIdeal, null, 2)}`);
    console.log(`Non Coverage Percentage: ${JSON.stringify(kpisByWeek.nonCoveragePorcentage, null, 2)}`);
    console.log(`Forecast Accuracy: ${JSON.stringify(kpisByWeek.forecastAcuraccy, null, 2)}`);
    console.log(`Total Variance to Ideal Labor Hours: ${JSON.stringify(kpisByWeek.totalVarianceToIdealLaborHours, null, 2)}`);
    console.log(`TPLH: ${JSON.stringify(kpisByWeek.TPLH, null, 2)}`);
    console.log(`IPLH: ${JSON.stringify(kpisByWeek.IPLH, null, 2)}`);


    try {

      await mkdir(path.dirname(rutaJsonTOTALIZADO), { recursive: true });
      await mkdir(path.dirname(rutaJsonCOMPLETO), { recursive: true });

      await writeFile(rutaJsonTOTALIZADO, JSON.stringify(kpisByWeek, null, 2));
      await writeFile(rutaJsonCOMPLETO, JSON.stringify(rawData, null, 2));

      console.log(`Datos guardados para el range ${range.start} en ${rutaJsonTOTALIZADO} y ${rutaJsonCOMPLETO}`);
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

