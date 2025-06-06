const handlerErrorAync = functionAsync => async (...args) => {
    const next = args[args.length - 1];
    try {
        return await functionAsync(...args);
    } catch (err) {
        return next(err);
    }
}
const getResultRecursive = (result) => {
    for (let i = 0; i < result.length; i++) {
        const apiResponse = (Array.isArray(result[i])) ? result[i][0] : result[i];
        if (apiResponse.sqlMessage) {
            return [false, apiResponse.sqlMessage];
        }
    }
    return [true, ''];
}

const verifyResponse = (result) => {
    if (!Array.isArray(result)) return [true, ''];

    if (!(result && result[0] && result[0][0]))
        return [false, 'No hubo respuesta'];
    if (result.length > 2) {
        const response = getResultRecursive(result);
        return response;
    }

    const apiResponse = result[0][0];
    if (apiResponse.sqlMessage)
        return [false, apiResponse.sqlMessage];

    return [true, ''];
}

module.exports = {
    handlerErrorAync,
    verifyResponse
}