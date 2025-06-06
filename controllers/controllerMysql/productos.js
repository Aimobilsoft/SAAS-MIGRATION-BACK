const db = require('../../config/connections/database');
const managerDBConnections = require('../../config/connections/managerDBConnections');


module.exports = {
    async listar(req, res) {
        const conn = managerDBConnections.getConnection();

        const array = [
            null,
            req.body.id_categoria == undefined ? null : req.body.id_categoria,
            req.body.id_sublinea == undefined ? null : req.body.id_sublinea,
            req.body.accion == undefined ? null : req.body.accion
        ];
        try {
            let data = await conn.raw(`call Sp_productos_formio(?,?,?,?)`, array);
            managerDBConnections.closeonexion(conn);
            let results = data[0];
            res.status(200).json({
                mensaje: "Procedimiento ejecutado correctamente",
                filas: { recordset: results[0], recordsets: [results[0]] },
                // datos: result.returnValue,
                status: true
            })
        } catch (err) {
            res.status(500).json({
                mensaje: "No se pudo ejecutar el procedimiento",
                error: new Error(err).message,
                status: false
            })
        }
    },
    async listar2(req, res) {
        const conn = managerDBConnections.getConnection();

        const array = [
            null,
            null,
            null,
            'listar'
        ];
        try {
            let data = await conn.raw(`call Sp_productos_formio(?,?,?,?)`, array);
            managerDBConnections.closeonexion(conn);
            let results = data[0];
            res.status(200).json(results[0][0]);
        } catch (err) {
            res.status(500).json({
                mensaje: "No se pudo ejecutar el procedimiento",
                error: new Error(err).message,
                status: false
            })
        }
    },
}