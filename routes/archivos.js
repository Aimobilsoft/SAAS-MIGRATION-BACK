const express = require('express');
const router = express.Router();
const multer = require('multer');
const fs = require('fs');
const { DATABASE_ENGINE } = require('../constants/connection.constants');

const controller = require('../controllers/controllerMysql/archivos');
const { handlerErrorAync } = require('../utils/handleErrorFuncAsync');

const storage = multer.diskStorage({
    filename: function (req, file, cb) {
        const nombreArchivo = file.originalname;
        cb("", nombreArchivo);
    },
    destination: (req, file, cb) => {
        try {
            const fecha = new Date();
            const dirPartDate = `/${fecha.getFullYear()}/${fecha.getMonth() + 1}/`;
            const dirToUploadFiles = `./uploads/${req.headers.subdominio}${dirPartDate}`;
            fs.mkdirSync(dirToUploadFiles, { recursive: true });
            cb(null, dirToUploadFiles);
        } catch (err) {
            cb(err);
        }
    }
});
const upload = multer({
    storage: storage
});
router.post('/multiple', upload.any('archivos'), handlerErrorAync(async (req, res) => {
    if (motor === DATABASE_ENGINE.MYSQL)
        return controller.cargarArchivos(req, res);

    throw new Error('Not implemented');
}));

router.get('/:ruta/:carpeta/:anio/:mes/:id_archivo', handlerErrorAync(controller.enviarArchivo));
module.exports = router;