var express = require('express');
var router = express.Router();
const ServicoSql = require('../controllers/controllersSQL/ServiciosForm');
const ServicosMysql = require('../controllers/controllerMysql/ServiciosForm');

router.get('/Servicio/:table/:json', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.listar(req, res);
    }
});

router.get('/Servicio/:table', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.listar(req, res);
    }
});

router.get('/Servicio/procedure/:procedure/:params', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.call_procedure(req, res, "1");
    }
});
router.get('/Servicio/procedure/:procedure/:params/:is_string', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.call_procedure(req, res, "1");
    }
});

router.post('/Servicio/procedure', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.call_procedure(req, res, "2");
    }
});
router.post('/Servicio/procedure2', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.call_procedure_v2(req, res);
    }
});

router.get('/Servicio/consecutivo/:table', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.consecutivo(req, res);
    }
});

router.get('/cuentas/:clase/:filtro/:codigocuenta', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.cuentas(req, res);
    }
});

router.get('/cuentas', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.cuentas(req, res);
    }
});

router.get('/cuentas_generico/:clase/:filtro/:codigocuenta', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.cuentas_generico(req, res);
    }
});
router.post('/tercero/:id_tercero', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.Tercero_formio(req, res);
    }
});

router.post('/resumendash/', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.resumendash(req, res);
    }
});


router.post('/Servicio/GetNumMenuToday', function (req, res) {
    if (motor == 'sql') {
        ServicoSql.listar(req, res);
    } else {
        ServicosMysql.GetNumMenuToday(req, res);
    }
});


module.exports = router;