var sql = require('mssql');
var q = require('q');
var deferred = q.defer();

module.exports = {
    consecutivos(req, res) {
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
                request.input('columna', sql.VarChar, req.body.columna);
                request.input('table', sql.VarChar, req.body.table);
                request.input('longitud', sql.VarChar, req.body.longitud);
                request.input('json', sql.VarChar, JSON.stringify(req.body.json));

                request.execute('Sp_Consecutivos').then(result => {
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