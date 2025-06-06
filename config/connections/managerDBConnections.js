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

  dataTenantMap = tenants
    .map((tenant) => {
      let key = tenant.subdomain.toLowerCase() + tenant.app.toLowerCase();
      return {
        [key]: knex({
          client: process.env.DB_CLIENT,
          connection: {
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            port: process.env.DB_PORT,
            database: tenant.database,
            host: process.env.DB_HOST,
          },
          pool: {
            min: 2,
            max: 20,
          },
          connectionTimeout: 30000, // 30 segundos
          requestTimeout: 30000, // 30 segundos
        }),
      };
    })
    .reduce((prev, next) => {
      return Object.assign({}, prev, next);
    }, {});
}

function getConnectionBySubdominio(subdominio, app) {
  // DESCOMENTAR PARA ACTIVAR EL MULTITENACY
  // +++++++++++++++++++++++++++++++++++++++++++++++++++
  try {
    let key = subdominio.toLowerCase() + app.toLowerCase();
    if (dataTenantMap) {
      return dataTenantMap[key];
    }
  } catch (error) {}
  // +++++++++++++++++++++++++++++++++++++++++++++++++++

  // return {
  //     host: process.env.DB_HOST,
  //     user: process.env.DB_USER,
  //     database: process.env.DB_DATABASE,
  //     //database: "alimentos_develop_2",
  //     password: process.env.DB_PASSWORD
  // }
}

function getConnection() {
  try {
    const nameSpace = dataTenantSpace.getNamespace("unique context");
    const conn = nameSpace.get("connection");
    if (!conn) {
      throw "Connection i not set for any tenant database";
    }
    return conn;
  } catch (error) {
    console.log(error);
  }
}

function closeonexion(conex) {
  conex.destroy();
}

module.exports = {
  connectAllDb,
  getConnectionBySubdominio,
  getConnection,
  closeonexion,
};
