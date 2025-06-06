const fs = require('fs');
const path = require('path');
module.exports = {
    async cargarArchivos(req, res) {
        const fecha = new Date();
        const dir = `${req.headers.subdominio}/${fecha.getFullYear()}/${fecha.getMonth() + 1}`

        const arrayFile = req.files.map(element => {
            return `/uploads/${dir}/${element.filename}`;
        });
        return res.status(200).json({ message: 'Se ejecutó correctamente', data: arrayFile });
    },
    async grabarArchivos(req, res, conn) {
        const values = [
            req.body.id_operacion,
            req.body.tabla,
            JSON.stringify(req.body.files || []),
            1,
            'guardar'
        ];
        await conn.raw(`call Sp_gen_archivos(?,?,?,?,?)`, values);
        return res.status(200).json({ message: 'Se ejecutó correctamente' });
    },

    async enviarArchivo(req, res) {
        const options = {
            root: path.resolve(__dirname, '../../')
        };
        if (req.params.ruta != '' && req.params.carpeta != '') {
            if (fs.existsSync(`./${req.params.ruta}/${req.params.carpeta}/${req.params.anio}/${req.params.mes}/${req.params.id_archivo}`)) {
                return res.status(200).sendFile(`${req.params.ruta}/${req.params.carpeta}/${req.params.anio}/${req.params.mes}/${req.params.id_archivo}`, options, function (err) {
                    if (err) {
                        res.status(err.status).end();
                    }
                });
            }
            throw new Error('No existe este archivo');
        }
        throw new Error('Ruta inconsistente');
    }
}