require("dotenv").config();
const knex = require("knex");
const dataTenantSpace = require("continuation-local-storage");
const commonDBConnection = require("./commonDBConnection");

let dataTenantMap;

async function connectAllDb() {
  let tenants;

  try {
    let instancias = await commonDBConnection.raw(`SELECT
        ins.id AS id_instancia,
        ter.id AS id_tercero,
        LOWER(ter.subdomain) AS subdomain,
        LOWER(ins.database) AS 'database',
        LOWER(app.codigo) AS app
    FROM admin_instancias ins
    INNER JOIN admin_versiones ver ON ver.id = ins.id_version
    INNER JOIN admin_aplicaciones app ON app.id = ver.id_aplicacion
    INNER JOIN gen_terceros ter ON ter.id = ins.id_tercero 
    WHERE ins.estado=1;`);
    tenants = instancias[0];
    if (tenants.length === 0) {
      console.log("tenants empty");
    }
  } catch (error) {
    console.log("error conexion DB", error);
    return;
  }
  dataTenantMap = {};
  tenants.forEach((tenant) => {
    let key = tenant.subdomain.toLowerCase() + tenant.app.toLowerCase();
    dataTenantMap[key] = knex({
      client: process.env.DB_CLIENT,
      connection: {
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        port: process.env.DB_PORT,
        database: tenant.database,
        host: process.env.DB_HOST,
        connectTimeout: 60000,
        keepAlive: true,
      },
      pool: {
        min: 2,
        max: 50,
        idleTimeoutMillis: 10000, // 10 seconds
        acquireTimeoutMillis: 60000,
        createTimeoutMillis: 30000,
        destroyTimeoutMillis: 5000,
      },
      connectionTimeout: 60000, // 60 segundos
      requestTimeout: 60000, // 60 segundos
    });
  });
}

function getConnectionBySubdominio(subdominio, app) {
  try {
    let key = subdominio.toLowerCase() + app.toLowerCase();
    if (dataTenantMap && dataTenantMap[key]) {
      return dataTenantMap[key];
    } else {
      throw new Error(
        "No existe conexión para el subdominio y app especificados"
      );
    }
  } catch (error) {
    console.error("Error en getConnectionBySubdominio:", error);
    return null;
  }
}

function getConnection() {
  try {
    const nameSpace = dataTenantSpace.getNamespace("unique context");
    const conn = nameSpace.get("connection");
    if (!conn) {
      throw new Error("Connection is not set for any tenant database");
    }
    return conn;
  } catch (error) {
    console.error("Error en getConnection:", error);
    return null;
  }
}

function closeonexion(conex) {
  try {
    conex.destroy();
    console.log("Conexión cerrada correctamente");
  } catch (error) {
    console.error("Error al cerrar la conexión:", error);
  }
}

module.exports = {
  connectAllDb,
  getConnectionBySubdominio,
  getConnection,
  closeonexion,
};
