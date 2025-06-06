var sql = require('mssql');
var q = require('q');
var deferred = q.defer();

module.exports = {
    async configurar(req, res) {
        motor = req.body.motor;
        if (req.body.motor == 'sql') {

            ConfiguracionDB.user = req.body.user;
            ConfiguracionDB.password = req.body.password;
            ConfiguracionDB.database = req.body.database;
            ConfiguracionDB.server = req.body.server;
            await sql.connect(ConfiguracionDB, async errorConnect => {
                if (errorConnect) {
                    deferred.reject({
                        menssage: "Error al conectar con la base de datos",
                        description: "Error al establecer conexion con la base de datos",
                        error: errorConnect,
                        config: ''
                    });
                    res.status(500).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        err: errorConnect,
                        // datos: result.returnValue,
                        status: false
                    })
                    sql.close();
                } else {
                    var querytabla = require('./iniciarDB').queryTablaFormularios();
                    (await new sql.Request().query(querytabla).then(log => {}).then(con => {}))

                    var queryGenerador = require('./iniciarDB').querySpGeneradorProcedure();
                    (await new sql.Request().query(queryGenerador).then(con => {}))

                    var querySpTablas = require('./iniciarDB').querySpTablas();
                    (await new sql.Request().query(querySpTablas).then(con => {}))

                    var ExistSpFormularios = require('./iniciarDB').ExistSpFormularios();
                    var exist = false;
                    (await new sql.Request().query(ExistSpFormularios).then(con => {
                        console.log(con.recordset.length);
                        if (con.recordset.length !== 0) { exist = true; }
                    }))
                    if (!exist) {
                        var querySpFormularios = require('./iniciarDB').querySpFormularios();
                        (await new sql.Request().query(querySpFormularios).then(con => {
                            console.log('TODO CREADO');
                            sql.close();
                        }))
                    } else {
                        sql.close()
                    }

                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        // filas: result,
                        // datos: result.returnValue,
                        status: true
                    })
                }
            });
        } else {
            var mysql = require('mysql');
            ConfiguracionMysql.user = req.body.user;
            ConfiguracionMysql.password = req.body.password;
            ConfiguracionMysql.database = req.body.database;
            ConfiguracionMysql.host = req.body.server;
            var connection = (await mysql.createConnection(ConfiguracionMysql));
            connection.connect(async function(err) {
                if (err) {
                    deferred.reject({
                        menssage: "Error al conectar con la base de datos",
                        description: "Error al establecer conexion con la base de datos",
                        error: err,
                        config: ''
                    });
                    res.status(500).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        err: err,
                        // datos: result.returnValue,
                        status: false
                    });
                    connection.end();

                } else {
                    // var querytabla = require('../controllerMysql/iniciarDB').queryTablaFormularios();
                    // (await connection.query(querytabla, function(error, results, fields) {
                    //     if (error) throw error;
                    //     console.log('TABLA CREADA');
                    // }));

                    // (await connection.query('DROP PROCEDURE IF EXISTS sp_tablas;', function(error, results, fields) {
                    //     if (error) throw error;
                    //     console.log('DROP PROCEDURE IF EXISTS sp_tablas;');
                    // }));
                    // var querySpTablas = require('../controllerMysql/iniciarDB').querySpTablas();
                    // (await connection.query(querySpTablas, function(error, results, fields) {
                    //     if (error) throw error;
                    //     console.log('SP TABLAS CREADO');
                    // }));
                    // var a = [1, 2];
                    // (await connection.query(`CALL sp_tablas (${a.map(e=>{return '?'})})`, ['formularios', 'L'], function(error, results, fields) {
                    //     if (error) throw error;
                    // }));
                    connection.end();
                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        // filas: result,
                        // datos: result.returnValue,
                        status: true
                    })
                }
            });
        }
    },

    ver(req, res) {
        ConfiguracionDB.motor = 'sql';
        ConfiguracionMysql.motor = 'mysql';
        res.status(200).json({
            mensaje: "Procedimiento ejecutado correctamente",
            // datos: ConfiguracionDB,
            datos: ConfiguracionMysql,
            status: true
        })

    }

}