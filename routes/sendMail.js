var express = require('express');
var router = express.Router();
//const almacensql = require('../controllers/controllersSQL/almacen');
const sendMailMySql = require('../controllers/controllerMysql/sendEmail' );

router.post('/', function(req, res) {
    if (motor == 'sql') {
        //almacensql.listar(req, res);
    } else {
        sendMailMySql.sendMail(req, res);
    }
}); 
  
module.exports = router;