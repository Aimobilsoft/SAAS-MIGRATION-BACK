const db = require("../../config/connections/database");
const managerDBConnections = require("../../config/connections/managerDBConnections");

module.exports = {
  async listar(req, res) {
    const conn = managerDBConnections.getConnection();

    const params = [null, req.body.tabla ?? null, req.body.json ?? null, "L"];

    try {
      const data = await conn.raw(`CALL sp_tablas(?,?,?,?)`, params);
      const results = data[0];

      // Crear lista de nombres
      const names = results[0].map((elem, i) => ({
        name: ["", elem.name],
        orden: i,
      }));

      let respuesta = JSON.parse(JSON.stringify(results[1]));

      // Si la tabla es view_entrada_almacen, procesar el detalle
      if (params[1] === "view_entrada_almacen") {
        respuesta = respuesta.filter((elemento) => {
          for (const [key, value] of Object.entries(elemento)) {
            if (key === "detalle:hide") {
              const productos = JSON.parse(value);
              const productosConDiferencia = productos.filter((producto) => {
                const recibidas = producto["Recibidas:$:colspan:[Cantidades]"];
                const facturadas = producto["Facturada:$:colspan:[Cantidades]"];
                return recibidas - facturadas > 0;
              });
              return productosConDiferencia.length > 0;
            }
          }
          return true; // Si no tiene "detalle:hide", conservar
        });
      }

      res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: { recordset: results[0], recordsets: [names, respuesta] },
        status: true,
      });
    } catch (err) {
      console.error("Error en listar:", err);
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: err.message || "Error desconocido",
        status: false,
      });
    } finally {
      managerDBConnections.closeonexion(conn);
    }
  },

  async listar_vistas(req, res) {
    const conn = managerDBConnections.getConnection();
    const params = [null, null, null, "vistas"];

    try {
      const data = await conn.raw(`CALL sp_tablas(?,?,?,?)`, params);
      const results = data[0];

      // Generar nombres de las columnas
      const names = results[0].map((elem, i) => ({
        name: ["", elem.name],
        orden: i,
      }));

      res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: {
          recordset: results[0],
          recordsets: [names, results[1]],
        },
        status: true,
      });
    } catch (err) {
      console.error("Error en listar_vistas:", err);
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: err.message || "Error desconocido",
        status: false,
      });
    } finally {
      managerDBConnections.closeonexion(conn);
    }
  },

  async registro(req, res) {
    const conn = managerDBConnections.getConnection();

    try {
      if (req.body.from === "tb") {
        // Procedimiento basado en tabla
        const params = [
          req.body.id ?? null,
          req.body.tabla ?? null,
          req.body.json ?? null,
          "Registro",
        ];

        const data = await conn.raw(`CALL sp_tablas(?,?,?,?)`, params);
        const results = data[0];

        return res.status(200).json({
          mensaje: "Procedimiento ejecutado correctamente",
          filas: { recordset: results[0], recordsets: [results[0]] },
          status: true,
        });
      } else {
        // Procedimiento basado en SP
        const spName = req.body.sp?.trim();
        if (!spName) {
          return res.status(400).json({
            mensaje: "El nombre del procedimiento almacenado (sp) es requerido",
            status: false,
          });
        }

        // Obtener campos del SP
        const dataCampos = await conn.raw(`CALL sp_listar_campos_proc(?, ?)`, [
          spName,
          "listar",
        ]);
        const campos = dataCampos[0][0]; // Primer recordset

        if (!campos || campos.length === 0) {
          return res.status(400).json({
            mensaje: "No se encontraron campos para el procedimiento",
            status: false,
          });
        }

        let bodyData = { ...req.body };
        if (Object.keys(bodyData).length === 3) {
          Object.keys(bodyData).forEach((key) => {
            if (typeof bodyData[key] === "object") {
              bodyData = { ...bodyData[key], sp: spName };
            }
          });
        }

        const values = campos
          .map((e) => {
            if (e.ORDINAL_POSITION !== campos.length) {
              const val = bodyData[e.nombre_parametro];
              return val === "" && val !== 0 ? null : val;
            }
          })
          .filter((v) => v !== undefined);

        values.push("LISTAR"); // Agregar modo LISTAR

        const placeholders = campos.map(() => "?").join(", ");
        const resultData = await conn.raw(
          `CALL ${spName}(${placeholders})`,
          values
        );

        return res.status(200).json({
          mensaje: "Procedimiento ejecutado correctamente",
          filas: { recordset: resultData[0][0] },
          status: true,
        });
      }
    } catch (error) {
      console.error("Error en registro:", error);

      return res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: error.message || "Error desconocido",
        status: false,
      });
    } finally {
      managerDBConnections.closeonexion(conn);
    }
  },
};
