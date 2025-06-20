const managerDBConnections = require("../../config/connections/managerDBConnections");
const {
  adapterParamaterProcedures,
  adapterBody,
} = require("../../utils/adapterParametersProcedures.utils");
const { verifyResponse } = require("../../utils/handleErrorFuncAsync");
const controllerArchivos = require("../controllerMysql/archivos");
const { DATABASE_ENGINE } = require("../../constants/connection.constants");

module.exports = {
  async save(req, res) {
    try {
      const conn = managerDBConnections.getConnection();
      const array = [
        req.body.id === undefined ? null : Number(req.body.id),
        req.body.nombre || "por defecto",
        req.body.tabla || null,
        req.body.form == undefined ? null : JSON.stringify(req.body.form),
        req.body.titulo || null,
        req.body.icono || null,
        "guardar",
      ];

      const data = await conn.raw(`CALL Sp_formularios(?,?,?,?,?,?,?)`, array);
      const results = data[0];
      const resultDynamicSQL = {
        drop: results[0][0].SQl_dinamic_drop,
        create: results[0][0].SQL_dinamic,
      };

      if (req.body.creaProc) {
        await conn.raw(resultDynamicSQL.drop);
        const responseCreate = (await conn.raw(resultDynamicSQL.create))[0];

        return res.status(200).json({
          mensaje: "Procedimiento ejecutado correctamente",
          filas: { recordset: responseCreate },
          status: true,
        });
      }

      return res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: { recordset: [] },
        status: true,
      });
    } catch (error) {
      console.error("❌ Error en save:", error);
      return res.status(500).json({
        mensaje: "Error al guardar el formulario",
        error: error.message || error,
        status: false,
      });
    }
  },

  async listar(req, res) {
    try {
      const conn = managerDBConnections.getConnection();
      const array = [
        req.body.id_formulario == null ? null : Number(req.body.id_formulario),
        null,
        null,
        null,
        null,
        null,
        "listar",
      ];
      const data = await conn.raw(`CALL Sp_formularios(?,?,?,?,?,?,?)`, array);

      return res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: {
          recordset: data[0][0],
          recordsets: data[0],
        },
        status: true,
      });
    } catch (error) {
      console.error("❌ Error en listar:", error);
      return res.status(500).json({
        mensaje: "Error al listar los formularios",
        error: error.message || error,
        status: false,
      });
    }
  },

  async Guardar(req, res) {
    try {
      const conn = managerDBConnections.getConnection();
      req.body = adapterBody(req.body);

      const values = await adapterParamaterProcedures(
        DATABASE_ENGINE.MYSQL,
        req.body.sp.trim(),
        req.body,
        conn
      );

      values.push(
        req.body.id == "" || req.body.id == null ? "guardar" : "editar"
      );

      const parametersAssignment = values.map(() => "?");
      const data = await conn.raw(
        `CALL ${req.body.sp}(${parametersAssignment.join(",")})`,
        values
      );
      const result = data[0];
      const operationSaved = result[0]?.[0] || null;
      const [isValid, message] = verifyResponse(result);

      if (!isValid) throw new Error(message);

      if (req.body.files && operationSaved) {
        req.body["id_operacion"] = operationSaved.id;
        req.body["tabla"] = operationSaved.tabla;
        req.body["operacion"] = operationSaved.operacion;

        return await controllerArchivos.grabarArchivos(req, res, conn);
      }

      return res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: { recordset: result },
        status: true,
      });
    } catch (error) {
      console.error("❌ Error en Guardar:", error);
      return res.status(500).json({
        mensaje: "Error al guardar los datos",
        error: error.message || error,
        status: false,
      });
    }
  },

  async eliminar(req, res) {
    try {
      const conn = managerDBConnections.getConnection();

      if (!conn) throw new Error("Se ha presentado un error con la conexión");

      const array = [
        req.body.id_formulario == null ? null : Number(req.body.id_formulario),
        null,
        null,
        null,
        null,
        null,
        "eliminar",
      ];
      const data = await conn.raw(`CALL Sp_formularios(?,?,?,?,?,?,?)`, array);

      return res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: {
          recordset: data[0][0],
          recordsets: data[0],
        },
        status: true,
      });
    } catch (error) {
      console.error("❌ Error en eliminar:", error);
      return res.status(500).json({
        mensaje: "Error al eliminar el formulario",
        error: error.message || error,
        status: false,
      });
    }
  },

  async eliminarReg(req, res) {
    try {
      const conn = managerDBConnections.getConnection();

      if (!req.body.id)
        throw new Error("Este formulario no tiene el parámetro id");

      const values = await adapterParamaterProcedures(
        DATABASE_ENGINE.MYSQL,
        req.body.sp.trim(),
        req.body,
        conn
      );
      values.push("eliminar");

      const parametersAssignment = values.map(() => "?");
      const data = await conn.raw(
        `CALL ${req.body.sp}(${parametersAssignment.join(",")})`,
        values
      );

      return res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: { recordset: data[0] },
        status: true,
      });
    } catch (error) {
      console.error("❌ Error en eliminarReg:", error);
      return res.status(500).json({
        mensaje: "Error al eliminar el registro",
        error: error.message || error,
        status: false,
      });
    }
  },
};
