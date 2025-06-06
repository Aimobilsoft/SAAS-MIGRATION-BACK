const db = require('../../config/connections/database');
const managerDBConnections = require('../../config/connections/managerDBConnections');


module.exports = {
    async consecutivos(req, res) {
        const conn = managerDBConnections.getConnection();

        const array = [
            req.params.table == undefined ? null : req.params.table,
            req.params.columna == undefined ? null : req.params.columna,
            req.params.longitud == undefined ? null : req.params.longitud,
            req.params.json == undefined ? null : req.params.json,
        ];
        try {
            let respuesta = await conn.raw(`call sp_Consecutivos(?,?,?,?)`, array);
            managerDBConnections.closeonexion(conn);
            let data = respuesta[0];
            res.status(200).json(data[0][0]);

        } catch (err) {
            res.status(500).json({
                mensaje: "No se pudo ejecutar el procedimiento",
                error: new Error(err).message,
                status: false
            })
        }
    },
    async consecutivos_fuentes(req, res) {
        const conn = managerDBConnections.getConnection();

        try {
            const id_sucursal = (req.params.id_sucursal === undefined) ? 1 : req.params.id_sucursal;
            const fecha = (req.params.fecha === undefined) ? 'now()' : req.params.fecha;

            let respuesta = await conn.raw(`call sp_consecutivo_fuentes(${id_sucursal}, '${req.params.fuente}', ${fecha},'${req.params.accion}',true, @consec_)`);
            managerDBConnections.closeonexion(conn);
            let results = respuesta[0];
            res.status(200).json(results[0][0]);
        } catch (err) {
            console.log(err);
            res.status(500).json({
                mensaje: "No se pudo ejecutar el procedimiento",
                error: new Error(err).message,
                status: false
            })
        }
    },
}