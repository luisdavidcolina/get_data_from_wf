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

const rawDataPath = path.join(__dirname, 'data', 'raw_data.json');
const rawData = JSON.parse(fs.readFileSync(rawDataPath, 'utf8'));

// Mapeo de keys en rawData a nombres de tablas en SQL
const tableMappings = {
  locations: 'locations',
  departments: 'departments',
  datastreams: 'datastreams',
  minimumIdeal: 'minimum_ideal',
  scheduled: 'scheduled',
  transactionForecast: 'transaction_forecast',
  actualTransactions: 'actual_transactions',
  items: 'items',
  totalPunchesLaborHours: 'total_punches_labor_hours',
  updateDates: 'update_dates'
};

async function deleteLastRecords(pool, tableName, count) {
  if (count === 0) {
    console.log(`No hay registros para eliminar en la tabla ${tableName}.`);
    return;
  }

  const query = `
    DELETE FROM [${tableName}]
    WHERE id IN (
      SELECT id FROM [${tableName}]
      ORDER BY id DESC
      LIMIT ${count}
    )
  `;

  try {
    const result = await pool.request().query(query);
    console.log(`Eliminados ${result.rowsAffected[0]} registros de ${tableName} (esperados: ${count}).`);
  } catch (err) {
    console.error(`Error al eliminar de ${tableName}:`, err.message);
  }
}

async function main() {
  try {
    const pool = await sql.connect(sqlConfig);

    const keys = Object.keys(tableMappings);
    for (const key of keys) {
      const count = rawData[key] ? rawData[key].length : 0;
      if (count > 0) {
        console.log(`Tabla ${tableMappings[key]}: eliminando los últimos ${count} registros.`);
        await deleteLastRecords(pool, tableMappings[key], count);
      }
    }

    console.log('Proceso de eliminación completado.');
  } catch (error) {
    console.error('Error en la ejecución principal:', error);
  } finally {
    sql.close();
  }
}

main();