const { DATABASE_ENGINE } = require('../constants/connection.constants');
async function adapterParamaterProcedures(type, nameStoreProcedure, body, connection) {
    const parametersValues = [];
    const parametersName = await getListFields(type, nameStoreProcedure, connection);
    parametersName.forEach(parameter => {
        const key = parameter.nombre_parametro;
        if (parameter.ORDINAL_POSITION !== parametersName.length) {
            const value = (body[key] !== undefined) ? body[key] : null;
            parametersValues.push(value);
        }
    });
    return parametersValues;
}

async function getListFields(type, nameStoreProcedure, connection) {
    if (type === DATABASE_ENGINE.MYSQL) return getFieldsMYSQL(nameStoreProcedure, connection);
    throw new Error("Not implemented");
}
async function getFieldsMYSQL(nameStoreProcedure, connection) {
    const data = await connection.raw(`call sp_listar_campos_proc ('${nameStoreProcedure}','listar')`);
    if (!(data && data.length)) throw new Error('Procedimiento no tiene parametros');
    const parameters = data[0][0] || [];
    return parameters;
}

function adapterBody(body) {
    if (Object.keys(body).length == 3) {
        let sp = body.sp;
        Object.keys(body).forEach(key => {
            if (typeof body[key] == "object") {
                body = body[key] || body;
                body.sp = sp;
            }
        });
        return body;
    }
    return body;
}
module.exports = {
    adapterParamaterProcedures,
    getListFields,
    getFieldsMYSQL,
    adapterBody
}