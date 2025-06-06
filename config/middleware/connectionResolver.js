const createNamesSpace = require('continuation-local-storage');
const msg = require('../../lib/message');
const result = new msg.MessageBuilder().setOrigen('API').build();

const managerDBConnections = require('../connections/managerDBConnections');

// Create a namespace for the application.
let nameSpace = createNamesSpace.createNamespace('unique context');


async function resolve(req, res, next) {
    let Subdominio = req.headers.subdominio;
    let aplicacion = req.headers.app;

    let refresh = await managerDBConnections.connectAllDb();

    // if (!Subdominio) {
    //     result.message = `Por favor comuniquese con administracion.`;
    //     result.description = `La peticion que quiere hacer no manda el Subdominio.`;
    //     return res.status(500).json(result);
    // }
    // if (!aplicacion) {
    //     result.message = `Por favor comuniquese con administracion.`;
    //     result.description = `La peticion que quiere hacer no manda la aplicacion.`;
    //     return res.status(500).json(result);
    // }

    // Run the application in the defined namespace. It will contextualize every underlying function calls.
    nameSpace.run(() => {
        nameSpace.set('connection', managerDBConnections.getConnectionBySubdominio(Subdominio, aplicacion)); // This will set the knex instance to the 'connection'
        next();
    });
}

module.exports = {
    resolve
}