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
                const table = req.params.table;
                const json = req.params.json;
                request.input('name', sql.VarChar, table);
                request.input('json', sql.VarChar, JSON.stringify(json));
                request.execute('sp_views').then(result => {
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