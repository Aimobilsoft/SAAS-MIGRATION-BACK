var express = require('express');
var router = express.Router();
const almacensql = require('../controllers/controllersSQL/almacen');
const almacenmysql = require('../controllers/controllerMysql/almacen' );

router.post('/', function(req, res) {
    if (motor == 'sql') {
        almacensql.listar(req, res);
    } else {
        almacenmysql.listar(req, res);
    }
}); 
router.post('/consecutivo', function(req, res) {
    if (motor == 'sql') {
        almacensql.consecutivo(req, res);
    } else {
        almacenmysql.consecutivo(req, res);
    }
});    

module.exports = router;