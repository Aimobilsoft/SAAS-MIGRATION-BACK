require("dotenv").config();
console.log("KNEX", process.env.DB_CLIENT);

/**
 * server configuration where the main project data will be saved
 */
const configInit = {
  client: process.env.DB_CLIENT,
  connection: {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    port: process.env.DB_PORT,
    database: process.env.DB_DATABASE_SAAS,
    password: process.env.DB_PASSWORD,
    connectTimeout: 60000, // 60 seconds
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
};

module.exports = require("knex")(configInit);
