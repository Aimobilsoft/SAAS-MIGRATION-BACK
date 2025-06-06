const express = require('express');
const router = express.Router();

const forms = require('../controllers/controllersSQL/formularios');
const formsmysql = require('../controllers/controllerMysql/formularios');
const { handlerErrorAync } = require('../utils/handleErrorFuncAsync');
const { DATABASE_ENGINE } = require('../constants/connection.constants');

router.post('/', handlerErrorAync((req, res) => {
    if (motor === DATABASE_ENGINE.SQLSERVER) {
        return forms.listar(req, res);
    }
    if (motor === DATABASE_ENGINE.MYSQL) {
        return formsmysql.listar(req, res);
    }
    throw new Error('Not implemented');
}));

router.post('/save', handlerErrorAync((req, res) => {
    if (motor === DATABASE_ENGINE.SQLSERVER) {
        return forms.save(req, res);
    }
    if (motor === DATABASE_ENGINE.MYSQL) {
        return formsmysql.save(req, res);
    }
    throw new Error('Not implemented');
}));
router.post('/eliminar', handlerErrorAync((req, res) => {
    if (motor === DATABASE_ENGINE.SQLSERVER) {
        return forms.eliminar(req, res);
    }
    if (motor === DATABASE_ENGINE.MYSQL) {
        return formsmysql.eliminar(req, res);
    }
    throw new Error('Not implemented');
}));

router.post('/eliminarReg', handlerErrorAync(async (req, res) => {
    if (motor === DATABASE_ENGINE.SQLSERVER) {
        return forms.eliminarReg(req, res);
    }
    if (motor === DATABASE_ENGINE.MYSQL) {
        return formsmysql.eliminarReg(req, res);
    }
    throw new Error('Not implemented');
}));
router.post('/guardar', handlerErrorAync((req, res) => {
    if (motor === DATABASE_ENGINE.SQLSERVER) {
        return forms.Guardar(req, res);
    }
    if (motor === DATABASE_ENGINE.MYSQL) {
        return formsmysql.Guardar(req, res);
    }
    throw new Error('Not implemented');
}));
module.exports = router;