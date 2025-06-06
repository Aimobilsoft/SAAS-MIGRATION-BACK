var express = require('express');
var router = express.Router();
const controller = require('../controllers/controllerMysql/notifications');

router.post('/saveBrowser', function(req, res, next) {
    controller.saveBrowser(req, res);
});

router.post('/sendNotification', function(req, res, next) {
    controller.sendNotification(req, res);
});

module.exports = router;