const managerDBConnections = require("../../config/connections/managerDBConnections");

module.exports = {
  async listar(req, res) {
    const conn = managerDBConnections.getConnection();
    const params = [null, req.body.tabla ?? null, req.body.json ?? null, "L"];

    try {
      const data = await conn.raw(`CALL sp_tablas(?,?,?,?)`, params);
      const [cabeceras, contenido] = data[0];

      const names = cabeceras.map((col, i) => ({
        name: ["", col.name],
        orden: i,
      }));

      let respuesta = JSON.parse(JSON.stringify(contenido));

      // Filtrar si es una vista específica
      if (params[1] === "view_entrada_almacen") {
        respuesta = respuesta.filter((item) => {
          const detalle = item["detalle:hide"];
          if (detalle) {
            const productos = JSON.parse(detalle);
            return productos.some((prod) => {
              const r = prod["Recibidas:$:colspan:[Cantidades]"];
              const f = prod["Facturada:$:colspan:[Cantidades]"];
              return r - f > 0;
            });
          }
          return true;
        });
      }

      res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: { recordset: cabeceras, recordsets: [names, respuesta] },
        status: true,
      });
    } catch (err) {
      console.error("❌ Error en listar:", err);
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
      const [cabeceras, contenido] = data[0];

      const names = cabeceras.map((col, i) => ({
        name: ["", col.name],
        orden: i,
      }));

      res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: {
          recordset: cabeceras,
          recordsets: [names, contenido],
        },
        status: true,
      });
    } catch (err) {
      console.error("❌ Error en listar_vistas:", err);
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
      const { from, tabla, id, json, sp } = req.body;

      if (from === "tb") {
        // Lógica para tabla directa
        const params = [id ?? null, tabla ?? null, json ?? null, "Registro"];
        const data = await conn.raw(`CALL sp_tablas(?,?,?,?)`, params);
        return res.status(200).json({
          mensaje: "Procedimiento ejecutado correctamente",
          filas: { recordset: data[0][0], recordsets: [data[0][0]] },
          status: true,
        });
      }

      // Lógica basada en SP
      const spName = sp?.trim();
      if (!spName) {
        return res.status(400).json({
          mensaje: "El nombre del procedimiento almacenado (sp) es requerido",
          status: false,
        });
      }

      // Obtener campos del SP
      const dataCampos = await conn.raw(`CALL sp_listar_campos_proc(?, ?)`, [spName, "listar"]);
      const campos = dataCampos[0][0];

      if (!campos?.length) {
        return res.status(400).json({
          mensaje: "No se encontraron campos para el procedimiento",
          status: false,
        });
      }

      // Ajuste si viene un objeto anidado
      let bodyData = { ...req.body };
      if (Object.keys(bodyData).length === 3) {
        const nestedKey = Object.keys(bodyData).find((k) => typeof bodyData[k] === "object");
        if (nestedKey) bodyData = { ...bodyData[nestedKey], sp: spName };
      }

      // Armar valores dinámicos
      const values = campos
        .filter((f) => f.ORDINAL_POSITION !== campos.length)
        .map((f) => {
          const val = bodyData[f.nombre_parametro];
          return val === "" && val !== 0 ? null : val;
        });

      values.push("LISTAR");
      const placeholders = campos.map(() => "?").join(", ");

      const resultData = await conn.raw(`CALL ${spName}(${placeholders})`, values);

      return res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: { recordset: resultData[0][0] },
        status: true,
      });
    } catch (error) {
      console.error("❌ Error en registro:", error);
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: error.message || "Error desconocido",
        status: false,
      });
    } finally {
      managerDBConnections.closeonexion(conn);
    }
  },
};