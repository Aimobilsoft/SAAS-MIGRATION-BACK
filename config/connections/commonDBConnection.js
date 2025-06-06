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
  },
  pool: {
    min: 2,
    max: 20,
  },
  connectionTimeout: 30000, // 30 segundos
  requestTimeout: 30000, // 30 segundos
};

module.exports = require("knex")(configInit);
