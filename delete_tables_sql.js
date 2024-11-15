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

async function dropAllTables() {
  try {
    const pool = await sql.connect(sqlConfig);

    // Obtener todas las tablas en la base de datos
    const result = await pool.request().query(`
      SELECT TABLE_NAME 
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_TYPE = 'BASE TABLE'
    `);

    const tables = result.recordset.map(row => row.TABLE_NAME);

    // Deshabilitar restricciones de claves foráneas temporalmente
    await pool.request().query('EXEC sp_msforeachtable "ALTER TABLE ? NOCHECK CONSTRAINT ALL"');

    // Eliminar cada tabla
    for (const table of tables) {
      try {
        await pool.request().query(`DROP TABLE [${table}]`);
        console.log(`Tabla eliminada exitosamente: ${table}`);
      } catch (error) {
        console.error(`Error al intentar eliminar la tabla ${table}:`, error.message);
      }
    }

    console.log('Todas las tablas han sido eliminadas exitosamente');
  } catch (error) {
    console.error('Error al eliminar tablas:', error.message);
  } finally {
    sql.close();
  }
}

// Ejecutar el script
dropAllTables();
