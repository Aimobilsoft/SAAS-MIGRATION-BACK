var express = require('express');
var router = express.Router();
const tercero = require('../controllers/controllersSQL/tercero');
const ServicosMysql = require('../controllers/controllerMysql/tercero');

router.post('/:id_tercero', function (req, res) {
    if (motor == 'sql') {
        tercero.listar(req, res);
    } else {
        ServicosMysql.listar(req, res);
    }
});

router.get('/digito/:digito', function (req, res) {
    if (motor == 'sql') {
        tercero.digito(req, res);
    } else {
        ServicosMysql.listarDigito(req, res);
    }
});


module.exports = router;