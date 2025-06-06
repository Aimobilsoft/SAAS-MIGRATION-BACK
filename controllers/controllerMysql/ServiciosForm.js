const managerDBConnections = require("../../config/connections/managerDBConnections");

module.exports = {
  async listar(req, res) {
    const conn = managerDBConnections.getConnection();

    const array = [
      req.params.table == undefined ? null : req.params.table,
      req.params.json == undefined ? null : req.params.json,
    ];
    try {
      let data = await conn.raw(`call sp_views(?,?)`, array);
      managerDBConnections.closeonexion(conn);
      let results = data[0];
      res.status(200).json(results[0]);
    } catch (err) {
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: new Error(err).message,
        status: false,
      });
    }
  },

  async call_procedure(req, res, tipo) {
    const conn = managerDBConnections.getConnection();

    try {
      let req_procedure =
        tipo == "2" ? req.body.procedure : req.params.procedure;
      let req_params = tipo == "2" ? req.body.params : req.params.params;
      let req_is_string =
        tipo == "2" ? req.body.is_string : req.params.is_string;

      let is_string = req_is_string == undefined ? 0 : req_is_string;
      let name = req_procedure == undefined ? null : req_procedure;
      let params =
        req_params == undefined
          ? []
          : is_string == 0
          ? typeof req_params === "object"
            ? req_params
            : JSON.parse(req_params)
          : req_params;
      let sigbols = "";
      if (is_string == 0) {
        params.map(
          (item, i) => (sigbols += i < params.length - 1 ? "?," : "?")
        );
      }
      try {
        let data = await conn.raw(
          `call ${name}(${
            is_string == 0 ? sigbols : params.slice(0, params.length)
          })`,
          is_string == 0 ? params : []
        );
        managerDBConnections.closeonexion(conn);
        let results = data[0];

        res.status(200).json(
          results.length > 0
            ? results[0]
            : {
                mensaje: "Procedimiento ejecutado correctamente",
                status: true,
                result: results,
              }
        );
      } catch (err) {
        // res.status(500).json({
        //   mensaje: "No se pudo ejecutar el procedimiento",
        //   error: {
        //     originalError: {
        //       info: {
        //         message:
        //           err.sqlState == "45000"
        //             ? results
        //               ? results[0][0].sqlMessage
        //               : err.sqlMessage
        //             : err.sqlMessage,
        //       },
        //     },
        //   },
        //   status: false,
        // });
        res.status(500).json({
          mensaje: "No se pudo ejecutar el procedimiento",
          error: new Error(err).message,
          status: false,
        });
      }
    } catch (err) {
      // res.status(500).json({
      //   mensaje: "Error al ejecutar el procedimiento",
      //   error: {
      //     originalError: {
      //       info: { message: err ? err.message : "Error iterno" },
      //     },
      //   },
      //   status: false,
      // });
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: new Error(err).message,
        status: false,
      });
    }
  },

  async consecutivo(req, res) {
    const conn = managerDBConnections.getConnection();

    const array = [
      req.params.table == undefined ? null : req.params.table,
      req.params.columna == undefined ? null : req.params.columna,
      req.params.json == undefined ? null : req.params.json,
    ];
    try {
      let data = await conn.raw(`call sp_views(?,?)`, array);
      managerDBConnections.closeonexion(conn);
      let results = data[0];
      res.status(200).json(results[0]);
    } catch (err) {
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: new Error(err).message,
        status: false,
      });
    }
  },

  async cuentas(req, res) {
    const conn = managerDBConnections.getConnection();

    const array = [
      req.params.clase == undefined ? null : Number(req.params.clase),
      req.params.filtro == undefined ? 1 : Number(req.params.filtro),
      req.params.codigocuenta == undefined
        ? null
        : req.params.codigocuenta == "null"
        ? null
        : Number(req.params.codigocuenta),
    ];

    try {
      let data = await conn.raw(
        `call sp_listar_cuentas_interfaz(?,?,?);`,
        array
      );
      managerDBConnections.closeonexion(conn);
      let results = data[0];
      res.status(200).json(results[0]);
    } catch (err) {
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: new Error(err).message,
        status: false,
      });
    }
  },
  async cuentas_generico(req, res) {
    const conn = managerDBConnections.getConnection();

    const array = [
      req.params.clase == undefined ? null : Number(req.params.clase),
      req.params.filtro == undefined ? 1 : Number(req.params.filtro),
      req.params.codigocuenta == undefined
        ? null
        : req.params.codigocuenta == "null"
        ? null
        : Number(req.params.codigocuenta),
    ];
    try {
      let data = await conn.raw(
        `call sp_listar_cuentas_generico(?,?,?);`,
        array
      );
      managerDBConnections.closeonexion(conn);
      let results = data[0];
      res.status(200).json(results[0]);
    } catch (err) {
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: new Error(err).message,
        status: false,
      });
    }
  },

  async Tercero_formio(req, res) {
    const conn = managerDBConnections.getConnection();

    const array = [
      null,
      req.params.id_tercero === undefined ? 1 : Number(req.params.id_tercero),
      "impuestos",
    ];
    try {
      let data = await conn.raw(`call Sp_tercero_formio(?,?,?);`, array);
      managerDBConnections.closeonexion(conn);
      let results = data[0];
      res.status(200).json(results[0]);
    } catch (err) {
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: new Error(err).message,
        status: false,
      });
    }
  },

  async resumendash(req, res) {
    const conn = managerDBConnections.getConnection();

    const array = [req.body.fuentes, req.body.json];
    //

    try {
      let data = await conn.raw(`call sp_consultas_resumen(?,?);`, array);
      managerDBConnections.closeonexion(conn);
      let results = data[0];
      res.status(200).json(results[0]);
    } catch (err) {
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: new Error(err).message,
        status: false,
      });
    }
  },
  async call_procedure_v2(req, res) {
    const conn = managerDBConnections.getConnection();

    try {
      // 1. Listar los parámetros que el SP espera
      const dataCampos = await conn.raw(`CALL sp_listar_campos_proc (?, ?)`, [
        req.body.sp.trim(),
        "listar",
      ]);
      const campos = dataCampos[0][0]; // Assumiendo que tu driver devuelve [ [rows], metadata ]

      if (!campos || campos.length === 0) {
        throw new Error("No se encontraron parámetros del procedimiento.");
      }

      // 2. Preprocesar body si trae objeto dentro
      if (Object.keys(req.body).length === 3) {
        const spName = req.body.sp;
        for (const key in req.body) {
          if (typeof req.body[key] === "object" && req.body[key] !== null) {
            req.body = { ...req.body[key], sp: spName };
            break;
          }
        }
      }

      // 3. Preparar valores
      const values = campos.map(
        (campo) => req.body[campo.nombre_parametro] ?? null
      );
      const placeholders = campos.map(() => "?").join(", ");

      // 4. Ejecutar el procedimiento real
      const dataResult = await conn.raw(
        `CALL ${req.body.sp}(${placeholders})`,
        values
      );
      const result = dataResult[0];

      // 5. Cerrar conexión
      managerDBConnections.closeonexion(conn);

      // 6. Responder exitosamente
      res.status(200).json({
        mensaje: "Procedimiento ejecutado correctamente",
        filas: { recordset: result },
        status: true,
      });
    } catch (error) {
      console.error("Error en call_procedure_v2:", error);
      managerDBConnections.closeonexion(conn); // Asegura cerrar aunque falle
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: error.message || "Error desconocido",
        status: false,
      });
    }
  },

  async GetNumMenuToday(req, res) {
    const conn = managerDBConnections.getConnection();

    try {
      let data = await conn.raw(
        `SELECT calcula_ciclo_actual(${req.body.id_contrato},'${req.body.fecha}') AS NumMenu;`
      );
      managerDBConnections.closeonexion(conn);
      let results = data[0];
      res.status(200).json(results[0]);
    } catch (err) {
      res.status(500).json({
        mensaje: "No se pudo ejecutar el procedimiento",
        error: new Error(err).message,
        status: false,
      });
    }
  },
  
};
