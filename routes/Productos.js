var express = require('express');
var router = express.Router();
const productos = require('../controllers/controllersSQL/productos');
const productosMySQL = require('../controllers/controllerMysql/productos')

router.post('/', function (req, res) {
    if (motor == 'sql') {
        productos.listar(req, res);
    } else {
        productosMySQL.listar(req, res);
    }
});

router.get('/', function (req, res) {
    if (motor == 'sql') {
        productos.listar2(req, res);
    } else {
        productosMySQL.listar2(req, res);
    }
});

module.exports = router;