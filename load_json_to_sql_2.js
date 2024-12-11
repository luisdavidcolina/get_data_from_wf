const sql = require('mssql');
const fs = require('fs');
const path = require('path');

const sqlConfig = {
  user: 'WFPBI',
  password: 'Wk~%qz$pB8m',
  database: 'WFPBI',
  server: 'localhost',
  options: {
    encrypt: false,
    trustServerCertificate: false,
  }
};

const rutaActual = path.dirname(__filename);
const rutaJsonCOMPLETO = path.join(rutaActual, 'data', 'raw_data.json');

function toSnakeCase(str) {
  return str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
}

function getSqlType(value) {
  if (typeof value === 'string') return 'NVARCHAR(MAX)';
  if (typeof value === 'number') return 'FLOAT';
  if (value instanceof Date) return 'NVARCHAR(MAX)';
  if (typeof value === 'boolean') return 'BIT'; 
  return 'NVARCHAR(MAX)';
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

async function createTableFromObject(pool, tableName, object) {
  const filteredObject = {};
  for (let [key, value] of Object.entries(object)) {
    if (key && typeof key !== 'boolean') {
      filteredObject[key] = value;
    }
  }
  
  const columns = Object.entries(filteredObject)
    .map(([key, value]) => `[${toSnakeCase(key)}] ${getSqlType(value)}`)
    .join(', ');

  const query = `
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='${tableName}' AND xtype='U')
    BEGIN
      CREATE TABLE [${tableName}] (
        id INT IDENTITY(1,1) PRIMARY KEY,
        ${columns}
      )
    END
  `;

  await pool.request().query(query);
}

async function insertData(pool, tableName, dataArray) {
  for (const data of dataArray) {
    const columns = Object.keys(data).map(key => `[${toSnakeCase(key)}]`).join(', ');
    const values = Object.values(data).map(value => {
      if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`;
      if (value instanceof Date) return `'${value.toISOString().split('T')[0]}'`;
      if (typeof value === 'boolean') return value ? 1 : 0; 
      return value !== null && value !== undefined ? value : "NULL";
    }).join(', ');

    if (!columns || !values) continue;

    const query = `INSERT INTO [${tableName}] (${columns}) VALUES (${values})`;
    await pool.request().query(query);
  }
}

function cargarDatosDesdeJSON(ruta) {
  const contenido = fs.readFileSync(ruta, 'utf-8');
  return JSON.parse(contenido);
}

async function deleteExistingData(pool, tableName, startDate, endDate) {
  let query = '';

  switch (tableName) {
    case 'minimum_ideal':
      query = `
        DELETE FROM [${tableName}]
        WHERE [date] BETWEEN '${startDate}' AND '${endDate}'
      `;
      break;
    case 'scheduled':
    case 'total_punches_labor_hours':
      query = `
        DELETE FROM [${tableName}]
        WHERE [start] BETWEEN ${new Date(startDate).getTime() / 1000} AND ${new Date(endDate).getTime() / 1000}
      `;
      break;
    case 'transaction_forecast':
    case 'actual_transactions':
    case 'items':
      query = `
        DELETE FROM [${tableName}]
        WHERE [time] BETWEEN ${new Date(startDate).getTime() / 1000} AND ${new Date(endDate).getTime() / 1000}
      `;
      break;
    default:
      console.error(`Tabla desconocida: ${tableName}`);
      return;
  }

  await pool.request().query(query);
}

async function createTablesAndInsertData(pool, startDate, endDate) {
  const rawData = cargarDatosDesdeJSON(rutaJsonCOMPLETO);

  for (const [key, dataArray] of Object.entries(rawData)) {
    if (Array.isArray(dataArray) && dataArray.length > 0) {
      const tableName = toSnakeCase(key);
      const sampleObject = dataArray[0];

      await createTableFromObject(pool, tableName, sampleObject);
      await deleteExistingData(pool, tableName, startDate, endDate); 
      await insertData(pool, tableName, dataArray);

      console.log(`Datos insertados en la tabla ${tableName} exitosamente`);
    }
  }
}

async function loadJsonToSql2(startDate, endDate) {
  try {
    const pool = await sql.connect(sqlConfig);

    await createTablesAndInsertData(pool, startDate, endDate);

    console.log('Tablas creadas y datos insertados exitosamente en SQL Server');
  } catch (error) {
    console.error('Error en la ejecución principal:', error);
  } finally {
    sql.close();
  }
}

if (require.loadJsonToSql2 === module) {
  loadJsonToSql2();
}

module.exports = { loadJsonToSql2 };

