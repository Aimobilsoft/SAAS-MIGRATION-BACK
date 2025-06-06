var express = require('express');
var router = express.Router();
const db = require('../controllers/controllersSQL/configdb');

router.post('/connection', function(req, res, next) {
    db.configurar(req, res);
});
router.post('/showConfig', function(req, res, next) {
    db.ver(req, res);
});


module.exports = router;