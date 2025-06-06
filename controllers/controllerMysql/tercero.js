const db = require('../../config/connections/database');
const managerDBConnections = require('../../config/connections/managerDBConnections');


module.exports = {
    async listar(req, res) {
        const conn = managerDBConnections.getConnection();

        const array = [
            req.body.id_tercero,
            req.params.id_tercero,
            req.body.accion
        ];
        try {
            console.log(`call Sp_tercero_formio(?,?,?)`, array);
            let data = await conn.raw(`call Sp_tercero_formio(?,?,?)`, array);
            managerDBConnections.closeonexion(conn);

            let results = data[0];
            res.status(200).json({
                mensaje: "Procedimiento ejecutado correctamente",
                filas: { recordset: results[0] },
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

    async listarDigito(req, res) {
        const conn = managerDBConnections.getConnection();

        const array = [
            req.params.digito
        ];
        try {
            let data = await conn.raw(`select DigitoDian(?) as digito;`, array);
            managerDBConnections.closeonexion(conn);
            let results = data[0];

            res.status(200).json({
                mensaje: "Procedimiento ejecutado correctamente",
                digito: results[0]['digito'],
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
}