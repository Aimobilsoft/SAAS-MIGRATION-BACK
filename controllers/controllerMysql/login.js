const managerDBConnections = require('../../config/connections/managerDBConnections');
var bcrypt = require('bcryptjs');

module.exports = {
    async logear(req, res) {
        const conn = managerDBConnections.getConnection();

        new Promise(async(resolve, reject) => {
            try {
                let data = await conn.raw(`call proc_usuarios(null,null,null,null,'${req.body.user}','${req.body.pass}',null,null,null,null,'Q')`);
                if (data[0][0].length == 0) {
                    res.status(200).json({
                        mensaje: "Procedimiento Ejecutado Correctamente",
                        filas: { recordset: [] },
                        // datos: result.returnValue,
                        status: true
                    });
                } else {
                    resolve(data[0][0]);
                }
            } catch (err) {
                res.status(500).json({
                    mensaje: "No se pudo ejecutar el procedimiento",
                    // error: { originalError: { info: { message: error.sqlMessage } } },
                    error: err,
                    status: false
                })
                reject();
            }
        }).then(async(data) => {
            if (data.length) {
                const valido = (await this.comparePasswords(req.body.pass, data[0].password));
                if (valido) {
                    try {
                        let formMenu = await conn.raw(`call proc_usuarios(${data[0].id},null,null,null,null,null,null,null,null,null,'L')`);
                        let results = formMenu[0];

                        let Menu = [];
                        let datamenu = JSON.parse(results[0][0].menu);
                        let formularios = results[1];
                        let itemMenu = {};
                        let ListForms = [];
                        let ListSubMenu = [];
                        datamenu?.forEach(modulo => {
                            itemMenu = {};
                            ListForms = [];
                            ListSubMenu = [];
                            itemMenu.id = modulo.id;
                            itemMenu.menu = modulo.menu;
                            modulo.submenu.forEach(submenu => {
                                formularios.forEach(form => {
                                    if (form.id_modulo_detalle == submenu.id) {
                                        ListForms.push(form);
                                    }
                                });
                                submenu.items_submenu = ListForms;
                                ListSubMenu.push(submenu);
                                ListForms = [];
                            })
                            itemMenu.submenu = ListSubMenu;
                            Menu.push(itemMenu);
                        });
                        data[0]['menu'] = JSON.stringify(Menu);
                        managerDBConnections.closeonexion(conn);

                        res.status(200).json({
                            mensaje: "Procedimiento Ejecutado Correctamente",
                            filas: { recordset: data },
                            // datos: result.returnValue,
                            status: true
                        });

                    } catch (err) {
                        res.status(500).json({
                            mensaje: "No se pudo ejecutar el procedimiento",
                            // error: { originalError: { info: { message: error.sqlMessage } } },
                            error: err,
                            status: false
                        })

                    }
                }
            }

        }).catch(function(error) {
            console.log(error);
        });

    },
    async comparePasswords(newPassword, password) {
        return await bcrypt.compare(newPassword, password).then((result) => {
            return result
        });
    }
}