var express = require('express');
var router = express.Router();
// const login = require('../controllers/controllersSQL/login');
const loginMysql = require('../controllers/controllerMysql/login');

router.post('/in', function(req, res, next) {
    if (motor == 'sql') {
        // login.logear(req, res);
    } else {
        loginMysql.logear(req, res);
    }
});

module.exports = router;