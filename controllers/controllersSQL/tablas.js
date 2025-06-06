var sql = require('mssql');
var q = require('q');
var deferred = q.defer();

module.exports = {
    listar(req, res) {
        console.log("listando tablas", req.body);
        sql.connect(ConfiguracionDB, errorConnect => {
            if (errorConnect) {
                deferred.reject({
                    menssage: "Error al conectar con la base de datos",
                    description: "Error al establecer conexion con la base de datos",
                    error: errorConnect,
                    config: ''
                });
            } else {
                var request = new sql.Request();
                request.input('name', sql.VarChar, req.body.tabla);
                request.input('accion', sql.VarChar, 'L');
                request.execute('[sp_tablas]').then(result => {
                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        filas: result,
                        datos: result.returnValue,
                        status: true
                    });
                    sql.close();
                }).catch(err => {
                    res.status(500).json({
                        mensaje: "No se pudo ejecutar el procedimiento",
                        error: err,
                        status: false
                    });
                    sql.close();
                })
            }
        });
    },

    registro(req, res) {
        sql.connect(ConfiguracionDB, errorConnect => {
            if (errorConnect) {
                deferred.reject({
                    menssage: "Error al conectar con la base de datos",
                    description: "Error al establecer conexion con la base de datos",
                    error: errorConnect,
                    config: ''
                });
            } else {
                var request = new sql.Request();
                request.input('id', sql.Int, req.body.id);
                request.input('name', sql.VarChar, req.body.tabla);
                request.input('accion', sql.VarChar, 'Registro');
                request.execute('[sp_tablas]').then(result => {
                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        filas: result,
                        datos: result.returnValue,
                        status: true
                    });
                    sql.close();
                }).catch(err => {
                    res.status(500).json({
                        mensaje: "No se pudo ejecutar el procedimiento",
                        error: err,
                        status: false
                    });
                    sql.close();
                })
            }
        });
    },


}