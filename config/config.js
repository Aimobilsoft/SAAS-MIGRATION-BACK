process.env.NODE_ENV = process.env.NODE_ENV || "dev";

module.exports = {
    logs: process.env.LOGS || true,
    port: process.env.PORT,
    endpoint_rest_post: process.env.ENDPOINT_REST || 'http://192.168.1.200:5800/api/createuser', //end point de crear usuario servicio auth
    endpoint_rest_send_email: process.env.ENDPOINT_REST || 'http://192.168.1.200:5900/api/sendEmailsWelcome'
}
// endpoint_rest_post: process.env.ENDPOINT_REST || 'http://192.168.0.31:5800/api/createuser', //end point de crear usuario servicio auth
//     endpoint_rest_send_email: process.env.ENDPOINT_REST || 'http://192.168.0.31:5900/api/sendEmailsWelcome'