const sql = require('mssql');

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

async function clearAllTables() {
  try {
    const pool = await sql.connect(sqlConfig);

    // Obtener todas las tablas de la base de datos
    const result = await pool.request().query(`
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_TYPE = 'BASE TABLE'
    `);

    const tables = result.recordset.map(row => row.TABLE_NAME);

    // Vaciar cada tabla
    for (const table of tables) {
      try {
        await pool.request().query(`TRUNCATE TABLE [${table}]`);
        console.log(`Tabla vaciada exitosamente: ${table}`);
      } catch (error) {
        if (error.message.includes('TRUNCATE TABLE') && error.message.includes('referential integrity constraint')) {
          console.warn(`No se puede truncar la tabla ${table} debido a restricciones. Intentando DELETE...`);
          await pool.request().query(`DELETE FROM [${table}]`);
          console.log(`Datos eliminados de la tabla: ${table}`);
        } else {
          console.error(`Error al intentar vaciar la tabla ${table}:`, error.message);
        }
      }
    }

    console.log('Todas las tablas han sido vaciadas exitosamente');
  } catch (error) {
    console.error('Error al vaciar tablas:', error.message);
  } finally {
    sql.close();
  }
}

// Ejecutar el script
clearAllTables();
