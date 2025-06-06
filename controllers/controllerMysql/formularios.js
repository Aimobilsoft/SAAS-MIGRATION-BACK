const managerDBConnections = require('../../config/connections/managerDBConnections');
const { adapterParamaterProcedures, adapterBody } = require('../../utils/adapterParametersProcedures.utils');
const { verifyResponse } = require('../../utils/handleErrorFuncAsync');
const controllerArchivos = require('../controllerMysql/archivos');
const { DATABASE_ENGINE } = require('../../constants/connection.constants');


module.exports = {
    async save(req, res) {
        const conn = managerDBConnections.getConnection();

        const array = [
            req.body.id == undefined ? null : Number(req.body.id),
            req.body.nombre || 'por defecto',
            req.body.tabla || null,
            req.body.form == undefined ? null : JSON.stringify(req.body.form),
            req.body.titulo || null,
            req.body.icono || null,
            'guardar'
        ];

        const data = await conn.raw(`call Sp_formularios(?,?,?,?,?,?,?)`, array);
        const results = data[0];
        const resultDynamicSQL = { drop: results[0][0].SQl_dinamic_drop, create: results[0][0].SQL_dinamic };

        if (req.body.creaProc) {
            await conn.raw(resultDynamicSQL.drop);
            const responseCreate = (await conn.raw(resultDynamicSQL.create))[0];
            managerDBConnections.closeonexion(conn);

            return res.status(200).json({
                mensaje: "Procedimiento ejecutado correctamente",
                filas: { recordset: responseCreate },
                status: true
            });
        }
        return res.status(200).json({
            mensaje: "Procedimiento ejecutado correctamente",
            filas: { recordset: [] },
            status: true
        });
    },
    async listar(req, res) {
        const conn = managerDBConnections.getConnection();
        const array = [
            req.body.id_formulario == null ? null : Number(req.body.id_formulario),
            null,
            null,
            null,
            null,
            null,
            'listar'
        ];
        const data = await conn.raw(`call Sp_formularios(?,?,?,?,?,?,?)`, array);
        managerDBConnections.closeonexion(conn);

        res.status(200).json({
            mensaje: "Procedimiento ejecutado correctamente",
            filas: { recordset: data[0][0], recordsets: data[0] },
            status: true
        });
    },
    async Guardar(req, res) {
        const conn = managerDBConnections.getConnection();
        //console.info("Data received: ", req.body);
        req.body = adapterBody(req.body);
        const values = await adapterParamaterProcedures(DATABASE_ENGINE.MYSQL, req.body.sp.trim(), req.body, conn);
        values.push(req.body.id == '' || req.body.id == null ? 'guardar' : 'editar');
        if (values.id == '') values.id = null;

        const parametersAssignment = values.map(e => { return '?' });
        const data = await conn.raw(`CALL ${req.body.sp}(${parametersAssignment})`, values);
        const result = data[0];
        const operationSaved = (result[0] && result[0][0]) ? result[0][0] : null;
        const [isValid, message] = verifyResponse(result);
        if (!isValid) throw new Error(message);

        if (req.body.files && operationSaved) {
            req.body['id_operacion'] = operationSaved.id;
            req.body['tabla'] = operationSaved.tabla;
            req.body['operacion'] = operationSaved.operacion;

            return await controllerArchivos.grabarArchivos(req, res, conn);
        }
        managerDBConnections.closeonexion(conn);

        return res.status(200).json({
            mensaje: "Procedimiento ejecutado correctamente",
            filas: { recordset: result },
            status: true
        });
    },
    async eliminar(req, res) {
        // TODO sacar esto a un util para no tener que repetir la validacion
        const conn = await managerDBConnections.getConnection();
        if (!conn) throw new Error('Se ha presentado un error con la conexion');
        const array = [
            req.body.id_formulario == null ? null : Number(req.body.id_formulario),
            null,
            null,
            null,
            null,
            null,
            'eliminar'
        ];
        const data = conn.raw(`call Sp_formularios(?,?,?,?,?,?,?)`, array);
        managerDBConnections.closeonexion(conn);
        const results = data[0];
        res.status(200).json({
            mensaje: "Procedimiento ejecutado correctamente",
            filas: { recordset: results[0], recordsets: results },
            status: true
        });
    },
    async eliminarReg(req, res) {
        const conn = managerDBConnections.getConnection();
        console.info('Data received (delete reg): ', req.body);
        if (!req.body.id) throw new Error('Este formulario No tiene El parametro Id');

        const values = await adapterParamaterProcedures(DATABASE_ENGINE.MYSQL, req.body.sp.trim(), req.body, conn);
        values.push('eliminar');

        const parametersAssignment = values.map(e => { return '?' });
        const data = await conn.raw(`CALL ${req.body.sp}(${parametersAssignment})`, values);
        managerDBConnections.closeonexion(conn);

        const result = data[0];

        return res.status(200).json({
            mensaje: "Procedimiento ejecutado correctamente",
            filas: { recordset: result },
            status: true
        });
    },
}