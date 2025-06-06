var express = require('express');
var router = express.Router();
const ServicoSql = require('../controllers/controllersSQL/consecutivos');
const ServicosMysql = require('../controllers/controllerMysql/consecutivos');

router.get('/tabla/:table/:columna/:longitud', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.consecutivos(req, res);
    } else {
        ServicosMysql.consecutivos(req, res);
    }
});

router.get('/tabla/:table/:columna/:longitud/:json', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.consecutivos(req, res);
    } else {
        ServicosMysql.consecutivos(req, res);
    }
});

router.get('/fuente/:fuente/:accion/:fecha?/:id_sucursal?', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.consecutivos(req, res);
    } else {
        ServicosMysql.consecutivos_fuentes(req, res);
    }
});
router.get('/:table', function(req, res) {
    consecutivos.consecutivos(req, res);
});
router.get('/:table/columna', function(req, res) {
    consecutivos.consecutivos(req, res);
});
router.get('/:table/:columna', function(req, res) {
    consecutivos.consecutivos(req, res);
});
router.get('/:table/:columna/:longitud', function(req, res) {
    consecutivos.consecutivos(req, res);
});
router.get('/:table/:columna/:longitud/:json', function(req, res) {
    consecutivos.consecutivos(req, res);
});


module.exports = router;