var sql = require('mssql');
var q = require('q');
var deferred = q.defer();

module.exports = {
    save(req, res) {
        sql.connect(ConfiguracionDB, errorConnect => {
            if (errorConnect) {
                deferred.reject({
                    menssage: "Error al conectar con la base de datos",
                    description: "Error al establecer conexion con la base de datos",
                    error: errorConnect,
                    config: ''
                });
            } else {
                var request = new sql.Request();
                request.input('id', sql.Int, req.body.id);
                request.input('nombre', sql.VarChar, req.body.nombre);
                request.input('form', sql.VarChar, JSON.stringify(req.body.form));
                request.input('titulo', sql.VarChar, req.body.titulo);
                request.input('tabla', sql.VarChar, req.body.tabla);
                request.input('accion', sql.VarChar, `${req.body.id==null || req.body.id==''?'guardar':'guardar'}`);
                request.execute('[sp_formularios]').then(result => {
                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        filas: result,
                        datos: result.returnValue,
                        status: true
                    });
                    sql.close();
                }).catch(err => {
                    res.status(500).json({
                        mensaje: "No se pudo ejecutar el procedimiento",
                        error: err,
                        status: false
                    });
                    sql.close();
                });
            }
        });
    },
    listar(req, res) {
        sql.connect(ConfiguracionDB, errorConnect => {
            if (errorConnect) {
                deferred.reject({
                    menssage: "Error al conectar con la base de datos",
                    description: "Error al establecer conexion con la base de datos",
                    error: errorConnect,
                    config: ''
                });
            } else {
                var request = new sql.Request();
                request.input('id', sql.Int, Number(req.body.id_formulario));
                request.input('accion', sql.VarChar, 'listar');
                request.execute('[sp_formularios]').then(result => {
                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        filas: result,
                        datos: result.returnValue,
                        status: true
                    });
                    sql.close();
                }).catch(err => {
                    res.status(500).json({
                        mensaje: "No se pudo ejecutar el procedimiento",
                        error: err,
                        status: false
                    })
                    sql.close();
                })
            }
        });

    },
    eliminar(req, res) {
        // config for your database

        sql.connect(ConfiguracionDB, errorConnect => {
            if (errorConnect) {
                deferred.reject({
                    menssage: "Error al conectar con la base de datos",
                    description: "Error al establecer conexion con la base de datos",
                    error: errorConnect,
                    config: ''
                });
            } else {
                var request = new sql.Request();
                request.input('id', sql.Int, Number(req.body.id_formulario));
                request.input('accion', sql.VarChar, 'eliminar');
                request.execute('[sp_formularios]').then(result => {
                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        filas: result,
                        datos: result.returnValue,
                        status: true
                    });
                    sql.close();
                }).catch(err => {
                    res.status(500).json({
                        mensaje: "No se pudo ejecutar el procedimiento",
                        error: err,
                        status: false
                    })
                    sql.close();
                })
            }
        });

    },
    formularios(req, res) {
        sql.connect(ConfiguracionDB, errorConnect => {
            if (errorConnect) {
                deferred.reject({
                    menssage: "Error al conectar con la base de datos",
                    description: "Error al establecer conexion con la base de datos",
                    error: errorConnect,
                    config: ''
                });
            } else {
                var request = new sql.Request();
                request.input('id', sql.Int, Number(req.body.id_formulario));
                request.input('accion', sql.VarChar, 'L');
                request.execute('[sp_formularios]').then(result => {
                    res.status(200).json(result.recordsets[0]);
                    sql.close();

                }).catch(err => {
                    res.status(500).json({
                        mensaje: "No se pudo ejecutar el procedimiento",
                        error: err,
                        status: false
                    })
                    sql.close();

                })
            }
        })
    },
    Guardar(req, res) {
        sql.connect(ConfiguracionDB, errorConnect => {
            if (errorConnect) {
                deferred.reject({
                    menssage: "Error al conectar con la base de datos",
                    description: "Error al establecer conexion con la base de datos",
                    error: errorConnect,
                    config: ''
                });
            } else {
                var request = new sql.Request();
                var keys = Object.keys(req.body);
                keys.forEach(elem => {
                    if (elem !== 'submit' && elem !== 'sp') {
                        request.input(`${elem}`, sql.VarChar, req.body[elem]);
                    }
                });
                request.input('accion', sql.VarChar, `${req.body.id==null || req.body.id==''?'guardar':'editar'}`);
                request.execute(`${req.body.sp}`).then(result => {
                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        filas: result,
                        datos: result.returnValue,
                        status: true
                    });
                    sql.close();
                }).catch(err => {
                    res.status(500).json({
                        mensaje: "No se pudo ejecutar el procedimiento",
                        error: err,
                        status: false
                    });
                    sql.close();
                })
            }
        });
    },
    eliminarReg(req, res) {
        sql.connect(ConfiguracionDB, errorConnect => {
            if (errorConnect) {
                deferred.reject({
                    menssage: "Error al conectar con la base de datos",
                    description: "Error al establecer conexion con la base de datos",
                    error: errorConnect,
                    config: ''
                });
            } else {
                var request = new sql.Request();
                request.input(`id`, sql.VarChar, req.body.id);
                request.input('accion', sql.VarChar, `eliminar`);
                request.execute(`${req.body.sp}`).then(result => {
                    res.status(200).json({
                        mensaje: "Procedimiento ejecutado correctamente",
                        filas: result,
                        datos: result.returnValue,
                        status: true
                    });
                    sql.close();
                }).catch(err => {
                    res.status(500).json({
                        mensaje: "No se pudo ejecutar el procedimiento",
                        error: err,
                        status: false
                    });
                    sql.close();
                })
            }
        });
    }

}