var q = require('q');
var path = require("path");
// 
var env = process.env.NODE_ENV || "desarrollo_home";
var config = require(path.join(__dirname, 'database.json'))[env];
var mssql = require('mssql');
module.exports = {
    getConnection: function() {
        // var deferred = q.defer();
        // var pool = new mssql.ConnectionPool(ConfiguracionDB, errorConnect => {
        //     if (errorConnect) {
        //         deferred.reject({
        //             menssage: "Error al conectar con la base de datos",
        //             description: "Error al establecer conexion con la base de datos",
        //             error: errorConnect,
        //             config: ''
        //         });
        //     } else {
        //         deferred.resolve(pool);
        //     }
        // })
        // return deferred.promise;
    }
}