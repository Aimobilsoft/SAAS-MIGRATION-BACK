var sql = require('mssql');
var q = require('q');
var deferred = q.defer();

module.exports = {
    listar(req, res) {
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
                request.input('id', sql.VarChar, null);
                request.input('id_sublinea', sql.VarChar, req.body.id_sublinea);
                request.input('accion', sql.VarChar, req.body.accion);

                // request.input('accion', sql.VarChar, 'listar');
                request.execute('Sp_productos_formio').then(result => {
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


    listar2(req, res) {
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
                request.input('id', sql.VarChar, null);
                request.input('id_sublinea', sql.VarChar, null);
                request.input('accion', sql.VarChar, 'listar');

                // request.input('accion', sql.VarChar, 'listar');
                request.execute('Sp_productos_formio').then(result => {
                    res.status(200).json(result.recordset);
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