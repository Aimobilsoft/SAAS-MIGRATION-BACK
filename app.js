const express = require('express');
const path = require('path');
const cookieParser = require('cookie-parser');
const logger = require('morgan');
const cors = require('cors');
const app = express();

const connectionAllDB = require('./config/connections/managerDBConnections');
const connectionResolver = require('./config/middleware/connectionResolver');
const { DATABASE_ENGINE } = require('./constants/connection.constants');

ConfiguracionDB = {
    "user": "desarrollo",
    "password": "d3v3l0p3r",
    // "database": "db_saas",
    "database": "db_inmobiliaria",
    "server": "192.168.1.200",
    "port": 1433,
    // "driver": "tedious",

};
ConfiguracionMysql = {
    "user": "developer",
    "password": "D3vMySql2021*",
    "database": "db_saas_demo",
    "host": "152.44.32.146",
}
motor = DATABASE_ENGINE.MYSQL;

const formularios = require('./routes/formularios');
const tablas = require('./routes/tablas');
const db = require('./routes/db');
const productos = require('./routes/Productos');
const almacen = require('./routes/almacen');
const tercero = require('./routes/tercero');
const ServiciosForm = require('./routes/ServiciosForm');
const consecutivos = require('./routes/consecutivos');
const login = require('./routes/login');
const indexRouter = require('./routes/index');
const archivosRoute = require('./routes/archivos');
const sendMail = require('./routes/sendMail');
const notifications = require('./routes/notifications');
// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');
app.use(cors());

// SE DESCOMENTA LAS SIGUIENTES DOS LINEAS PARA ACTIVAR EL MULTITENACY
// +++++++++++++++++++++++++++++++++++++++++++++++++++
connectionAllDB.connectAllDb();
app.use(connectionResolver.resolve);
// +++++++++++++++++++++++++++++++++++++++++++++++++++

app.use(logger('dev'));
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true, parameterLimit: 50000 }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/db', db);
app.use('/api/login', login);
app.use('/api/forms', formularios);
app.use('/api/tablas', tablas);
app.use('/api/Productos', productos);
app.use('/api/almacen', almacen);
app.use('/api/tercero', tercero);
app.use('/api/consecutivo', consecutivos);
app.use('/api/archivos', archivosRoute)
app.use('/api/mail', sendMail)
app.use('/api', ServiciosForm);
app.use('/api/notifications', notifications);

// catch 404 and forward to error handler
app.use(function (req, res, next) {
    res.status(404).json({
        message: "Error al procesar esta consulta",
        error: { status: 404, message: "Lo siento, no encuentro esta ruta" }
    });
    next();
});

/**
 * Manejo de errores 
 * Importante:
 * Debe agregar un try/catch a la funcion asincrona y enviar el error con next(err) 
 * o implementar la util "handleErrorAsync"
 * esto con el fin que nodejs pueda registrar el error y entrar al middleware
*/

app.use(async (err, req, res, next) => {
    if (!err) return next();
    const error = (err instanceof Error) ? new Error(err) : err;
    console.error(error.message);
    return res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: error.message,
        status: false
    });
});
module.exports = app;