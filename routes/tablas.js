const express = require('express');
const router = express.Router();
const forms = require('../controllers/controllersSQL/tablas');
const formsMysql = require('../controllers/controllerMysql/tablas');
const { handlerErrorAync } = require('../utils/handleErrorFuncAsync');
const { DATABASE_ENGINE } = require('../constants/connection.constants');

router.post('/', handlerErrorAync((req, res, next) => {
    if (DATABASE_ENGINE.MYSQL) {
        return formsMysql.listar(req, res);
    }
    if (DATABASE_ENGINE.SQLSERVER) {
        return forms.listar(req, res);
    }
}));
router.post('/vistas', handlerErrorAync((req, res, next) => {
    if (DATABASE_ENGINE.MYSQL) {
        return formsMysql.listar_vistas(req, res)
    }
    if (DATABASE_ENGINE.SQLSERVER) {
        console.log(DATABASE_ENGINE)
        return forms.listar_vistas(req, res);
    }
}));


router.post('/reg', handlerErrorAync((req, res, next) => {
    if (DATABASE_ENGINE.MYSQL) {
        formsMysql.registro(req, res);
    }
    if (DATABASE_ENGINE.SQLSERVER) {
        forms.registro(req, res);
    }
}));

module.exports = router;