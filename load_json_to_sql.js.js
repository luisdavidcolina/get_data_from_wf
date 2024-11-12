const sql = require('mssql');
const fs = require('fs');
const path = require('path');

// Configuración de la conexión a SQL Server
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

// Rutas de los archivos JSON
const rutaActual = path.dirname(__filename);
const rutaJsonTOTALIZADO = path.join(rutaActual, 'data', 'kpis_by_week.json');
const rutaJsonCOMPLETO = path.join(rutaActual, 'data', 'raw_data.json');

// Función para convertir nombres de camelCase a snake_case
function toSnakeCase(str) {
  return str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`);
}

// Función para detectar el tipo de dato SQL desde un valor de JavaScript
function getSqlType(value) {
  if (typeof value === 'string') return 'NVARCHAR(MAX)';
  if (typeof value === 'number') return 'FLOAT';
  if (value instanceof Date) return 'NVARCHAR(MAX)';
  return 'NVARCHAR(MAX)'; // tipo por defecto para cualquier otro valor
}

// Función para crear una tabla SQL a partir de la estructura de un objeto
async function createTableFromObject(pool, tableName, object) {
  const columns = Object.entries(object)
    .map(([key, value]) => `${toSnakeCase(key)} ${getSqlType(value)}`)
    .join(', ');

  const query = `
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='${tableName}' AND xtype='U')
    CREATE TABLE ${tableName} (
      id INT IDENTITY(1,1) PRIMARY KEY,
      ${columns}
    )`;

  await pool.request().query(query);
}

// Función para insertar datos en la tabla correspondiente
async function insertData(pool, tableName, dataArray) {
  for (const data of dataArray) {
    const columns = Object.keys(data).map(key => toSnakeCase(key)).join(', ');
    const values = Object.values(data).map(value => {
      if (typeof value === 'string') return `'${value.replace(/'/g, "''")}'`; // Escapa comillas en strings
      if (value instanceof Date) return `'${value.toISOString().split('T')[0]}'`; // Formato de fecha 'YYYY-MM-DD'
      return value;
    }).join(', ');

    const query = `INSERT INTO ${tableName} (${columns}) VALUES (${values})`;
    await pool.request().query(query);
  }
}

// Función para cargar y parsear los archivos JSON
function cargarDatosDesdeJSON(ruta) {
  const contenido = fs.readFileSync(ruta, 'utf-8');
  return JSON.parse(contenido);
}

// Función principal que crea tablas e inserta datos en base a la estructura de los archivos JSON
async function createTablesAndInsertData(pool) {
  
  const datosTotalizado = cargarDatosDesdeJSON(rutaJsonTOTALIZADO) || {};


    const transformedData = {
      kpisByWeek: Object.entries(datosTotalizado).flatMap(([key, values]) =>
        Array.isArray(values) 
          ? values.map(item => ({ ...item, kpi: key })) 
          : []
      )
    };

  const rawData = {
    ...transformedData,
    ...cargarDatosDesdeJSON(rutaJsonCOMPLETO)
  };

  for (const [key, dataArray] of Object.entries(rawData)) {
    if (Array.isArray(dataArray) && dataArray.length > 0) {
      const tableName = toSnakeCase(key);
      const sampleObject = dataArray[0]; // Usar el primer objeto para detectar las columnas

      // Crear la tabla si no existe
      await createTableFromObject(pool, tableName, sampleObject);

      // Insertar datos en la tabla
      await insertData(pool, tableName, dataArray);

      console.log(`Datos insertados en la tabla ${tableName} exitosamente`);
    }
  }
}

async function main() {
  try {
    const pool = await sql.connect(sqlConfig);

    // Crear tablas e insertar datos desde los JSON
    await createTablesAndInsertData(pool);

    console.log('Tablas creadas y datos insertados exitosamente en SQL Server');
  } catch (error) {
    console.error('Error en la ejecución principal:', error);
  } finally {
    sql.close();
  }
}

main();
