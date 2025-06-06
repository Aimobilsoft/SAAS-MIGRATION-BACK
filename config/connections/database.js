const mysql = require('mysql');
const getConnectionconfig = require('./managerDBConnections');


var connection;

function getConnection(peticion) {
    const sub = peticion.headers.subdominio;
    const app = peticion.headers.app;

    connection = mysql.createConnection(getConnectionconfig.getConnectionBySubdominio(sub, app));

    connection.connect(function(err) {
        if (err) {
            console.log('error when connecting to db:', err);
            connection = getConnection({ headers: { subdominio: sub, app } })
        }
    });

    connection.on('error', function(err) {
        console.log('db error-------', err);
        if (err.code === 'PROTOCOL_CONNECTION_LOST') {
            connection = getConnection({ headers: { subdominio: sub, app } });
        } else {
            console.log('-------     ', err);
            setTimeout(() => {
                connection = getConnection({ headers: { subdominio: sub, app } });
            })

        }
    });
    return connection;
}

module.exports = {
    getConnection
}