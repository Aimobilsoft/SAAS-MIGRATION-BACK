const db = require('../../config/connections/database');
const nodemailer = require("nodemailer");
const managerDBConnections = require('../../config/connections/managerDBConnections');


module.exports = {
    async sendMail(req, res) {
        try {
            let data = await managerDBConnections.getConnection().raw(`call Sp_admin_send_mail(null,null,null,null,null,null,null,null,'get')`);
            let results = data[0];

            let config = {
                pool: JSON.parse(results[0][0].pool),
                host: results[0][0].host,
                port: results[0][0].port,
                secure: JSON.parse(results[0][0].tsl), // true for 465, false for other ports
                auth: {
                    user: results[0][0].user, // generated ethereal user
                    pass: results[0][0].password, // generated ethereal password
                },
            }

            let transporter = nodemailer.createTransport(config);

            let send = await transporter.sendMail({
                from: '"Fred Foo " < ' + results[0][0].user + '>', // sender address
                to: "dugusas@gmail.com", // list of receivers
                subject: "Hello ", // Subject line
                text: "Hello world?", // plain text body
                html: "<b>Hello world?</b>", // html body
            });
            res.status(200).json({
                mensaje: "El correo fue enviado con exito",
                //data: config,
                status: true
            })
        } catch (err) {
            res.status(500).json({
                mensaje: "No se pudo ejecutar el procedimiento",
                error: new Error(err).message,
                status: false
            })
        }
    }
}