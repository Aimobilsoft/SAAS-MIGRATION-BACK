const db = require('../../config/connections/database');
const managerDBConnections = require('../../config/connections/managerDBConnections');
const webpush = require('web-push');

module.exports = {
    async saveBrowser(req, res) {
        const conn = managerDBConnections.getConnection();

        try {
            let create = await conn.raw(`CREATE TABLE IF NOT EXISTS Browsers  (
                id INT UNSIGNED NOT NULL AUTO_INCREMENT,
                Browser TEXT NOT NULL,
                PRIMARY KEY (id));`);

        let consult = await conn.raw(`select count(*) as count from Browsers where Browser='${req.body.token}';`);
        let insert={};
        if(consult[0][0].count==0){
            insert = await conn.raw(`INSERT INTO Browsers(Browser) VALUES ('${req.body.token}')`);
        }

        managerDBConnections.closeonexion(conn);

        let results = insert;
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

    async sendNotification(req, res) {
        const conn = managerDBConnections.getConnection();

        const vapidKeys = {
            "publicKey":"BJpSw7ME_n0ivRcMcr0MG67CKh_nuyyGJTFh1NDjcG096g3QR6J2vmeKwFPbe6pOI9sGQJNqKZwyrWgWxhIvdgU",
            "privateKey":"0htmPGudqXYlHUBAsrF_5bbfSJNEblzaKFiUMBL27UM"
        };

        webpush.setVapidDetails(
            'mailto:jcabarcasjulio@gmail.com',
            vapidKeys.publicKey,
            vapidKeys.privateKey
        );

        const payload = {
            "notification": {
                "title": req.body.title,
                "body": req.body.body,
                "vibrate": [100, 50, 100],
                icon: 'assets/img/logo_mobilsoft.png',
                "actions": [{
                    "action": "explore",
                    "title": "Revisar"
                }]
            }
        };

        try {
            let consult = await conn.raw(`select * from Browsers;`);
            if(consult[0].length){
                consult[0].forEach(browser => {
                    webpush.sendNotification(JSON.parse(browser.Browser),JSON.stringify(payload))
                    .then(res => {
                        // console.log('Enviado !!');
                    }).catch(err => {
                        // console.log('Error', err);
                    })
                });
            }
            
            managerDBConnections.closeonexion(conn);
            
            res.status(200).json({
                mensaje: "Procedimiento ejecutado correctamente",
                filas: { recordset: {} },
                status: true
            });
        } catch (err) {
            res.status(500).json({
                mensaje: "No se pudo ejecutar el procedimiento",
                error: new Error(err).message,
                status: false
            });
        }
    },

    
}