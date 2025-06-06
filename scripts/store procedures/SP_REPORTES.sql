CREATE DEFINER = `serverapp` @`%` PROCEDURE `sp_reportes`(`p_id` INT, `p_operacion` varchar(25)) begin DECLARE lc_id_fuente int;

DECLARE lc_id_tercero int;

DECLARE lc_id_sucursal int;

declare lc_documento int;

declare lc_fecha date;

declare lc_comprobante varchar(15);

DECLARE p_no_ppl INT;

DECLARE lc_unidad_ciclo DECIMAL(10, 8);

DECLARE lc_ciclo_actual INT;

DECLARE n_ciclos INT;

DECLARE lc_ultimo_ciclo INT DEFAULT 1;

DECLARE lc_decimal DECIMAL(10, 8);

DECLARE lc_ciclo_entero INT;

DECLARE lc_ingredientes JSON DEFAULT '[]';

DECLARE lc_json_presupuesto JSON DEFAULT '[]';

DECLARE lc_minimo_menu INT;

DECLARE p_id_contrato INT;

DECLARE p_id_sucursal INT;

DECLARE p_fecha_inicio DATE;

DECLARE p_fecha_fin DATE;

select
    *,
    coalesce(
        (
            SELECT
                GROUP_CONCAT(trim(nombre) SEPARATOR '-')
            FROM
                desarrollo.gen_empresa_responsabilidades ger
                inner join gen_responsabilidades_dian grd on grd.id = ger.id_responsibilidad
            where
                ger.id_empresa = ve.id
        ),
        ''
    ) as responsabilidades,
    concat(
        'La presente factura de venta tiene carácter de título valor y se rige por la ley 1231 de julio 17/2008. El comprador y el aceptante declara haber recibido real y materialmente las mercancías descritas en este título valor y se obliga a pagar el precio en la forma pactada aquí mismo.',
        'Se hace constar que la firma de persona diferente al comprador está autorizada por el comprador para firmar, recibir y confesar la deuda. La mora en el pago causa intereses a la máxima tasa autorizada por la ley'
    ) as leyenda1,
    concat(
        'Autorizo a ',
        trim(ve.nombre),
        ' a quien presente sus derechos u ostente en el futuro la calidad de acreedor a reportar, procesar, solicitar y divulgar a la central de información financiera –CIFIN- que administra la asociación bancaria y entidades financieras de Colombia,',
        'o cualquier otra entidad que maneje o administre bases de datos con los mismos fines, total la información referente a mi comportamiento comercial'
    ) as leyenda2
from
    view_empresas_simple ve;

if(trim(p_operacion) = 'FACTURA_VENTA') then
select
    concat(trim(grd.prefijo), '-', cmo.numero_documento) as factura,
    cmo.fecha,
    cmo.fecha_vencimiento,
    trim(ct.nombre_tercero) as tercero,
    if(
        NOT ISNULL(ct.digito)
        and ct.digito != '',
        concat(ct.documento, '-', ct.digito),
        ct.documento
    ) as documento_tercero,
    ct.direccion,
    ct.email,
    gm.nombre as ciudad,
    concat(ct.telefono, '-', ct.celular) as telefonos,
    gp.nombre as vendedor,
    coalesce(pd.numero_documento, '') as pedido,
    if(cmo.credito > 0, 'CREDITO', 'CONTADO') as forma_pago,
    coalesce(cmo.cufe, 'FACTURA INVALIDA') as cufe,
    `desarrollo`.`RESOLUCION_DIAN`(`grd`.`codigo`) AS `resolucion_dian`,
    cmo.subtotal,
    cmo.valor_descuento,
    cmo.valor_iva,
    cmo.valor_retenciones,
    cmo.valor_anticipo,
    cmo.valor_flete,
    cmo.total as total_pagar,
    (cmo.subtotal + cmo.valor_iva + cmo.valor_flete) as total_factura,
    desarrollo.`numeros_letras`(cmo.total) as monto_letras,
    trim(cmo.observaciones) as observacion,
    grd.clase,
    ct2.nombre_tercero as usuario
from
    cio_maestro_operaciones cmo
    inner join con_fuentes cf on cf.id = cmo.id_tipo_operacion
    inner join gen_resoluciones_dian grd on grd.id = cmo.id_resolucion
    inner join gen_sucursales gs on gs.id = cmo.id_sucursal
    inner join admin_usuarios au on au.id = cmo.id_usuario
    inner join con_terceros ct2 on ct2.id = au.id_tercero
    inner join con_terceros ct on ct.id = cmo.id_tercero
    inner join gen_municipios gm on gm.id = ct.id_municipio
    inner join gen_personal gp on gp.id = cmo.id_personal
    left join cio_maestro_operaciones pd on pd.id = cmo.id_pedido
where
    cmo.id = p_id
    and cf.codigo in ('FC', 'FV');

select
    row_number() over(
        order by
            ip.nombre
    ) as id,
    ip.codigo,
    ip.nombre,
    trim(im.nombre) as medida,
    cdo.cantidad_conversion as cantidad,
    cdo.valor_conversion as precio_unidad,
    cdo.tasa_descuento,
    iic.tasa_iva,
(cdo.valor_conversion * cdo.cantidad_conversion) as total
from
    cio_detalle_operaciones cdo
    inner join inv_productos ip on ip.id = cdo.id_producto
    inner join inv_medidas im on im.id = ip.id_medida
    inner join inv_interfaz_contable iic on iic.id = ip.id_interfaz_contable
where
    cdo.id_maestro_operacion = p_id;

select
    tfp.nombre,
    sum(tpo.valor) as valor
from
    cio_maestro_operaciones cmo
    inner join tes_pago_operaciones tpo on tpo.id_cio_operacion = cmo.id
    inner join tes_formas_pago tfp on tfp.id = tpo.id_forma_pago
where
    cmo.id = p_id
group by
    tfp.nombre;

end if;

IF (TRIM(p_operacion) = 'RECIBO_CAJA') THEN
SELECT
    `tm`.`id` AS `id`,
    `tm`.id_clase_operacion,
    `cf`.`codigo` AS `tipo`,
    CONCAT(
        gs.codigo,
        '-',
        LPAD(`tm`.`numero_documento`, 4, '0')
    ) AS `no`,
    `tm`.`fecha` AS `fecha`,
    LOWER(`ct`.`documento`) AS `doc_tercero`,
    `ct`.`nombre_tercero` AS `tercero`,
    cc.codigo AS codigo_cuenta,
    cc.nombre AS nombre_cuenta,
    TRUNCATE(`tm`.`subtotal`, 0) AS `valor_recibido`,
    tm.total,
    COALESCE(TRUNCATE(`c`.`RETEFUENTE`, 0), 0.0) AS `retefuente`,
    COALESCE(TRUNCATE(`c`.`RETEICA`, 0), 0.0) AS `reteica`,
    COALESCE(TRUNCATE(`c`.`RETEIVA`, 0), 0.0) AS `reteiva`,
    COALESCE(TRUNCATE(`c`.`RETECREE`, 0), 0.0) AS `retecree`,
    COALESCE(TRUNCATE(`tm`.`valor_descuento`, 0), 0.0) AS `descuentos`,
    TRUNCATE(`tm`.`total`, 0) AS `valor_totales`,
    TRUNCATE(`fp`.`efectivo`, 0) AS `valor_efectivo`,
    TRUNCATE(`fp`.`cheques`, 0) AS `valor_cheques`,
    TRUNCATE(`fp`.`transferencia`, 0) AS `valor_bancaria`,
    TRUNCATE(`fp`.`tarjetas`, 0) AS `valor_tarjetas`,
    IF(
        (LENGTH(TRIM(`tm`.`observacion`)) = 0),
        '',
        `tm`.`observacion`
    ) AS `observaciones`,
    TRIM(`gs`.`nombre`) AS `sucursal`,
    ct_u.`nombre_tercero` AS nom_usuario,
    tm.documento_contable
FROM
    (
        (
            (
                (
                    (
                        `tes_movimientos` `tm`
                        JOIN `con_fuentes` `cf` ON ((`cf`.`id` = `tm`.`id_tipo_operacion`))
                    )
                    JOIN `con_terceros` `ct` ON ((`ct`.`id` = `tm`.`id_tercero`))
                )
                JOIN `gen_sucursales` `gs` ON ((`gs`.`id` = `tm`.`id_sucursal`))
            )
            JOIN `con_terceros` `ct_u` ON ((`ct_u`.`id` = `tm`.`id_usuario`))
            LEFT JOIN (
                SELECT
                    `e`.`id_movimiento` AS `id_movimiento`,
                    SUM(`e`.`efectivo`) AS `efectivo`,
                    SUM(`e`.`cheques`) AS `cheques`,
                    SUM(`e`.`transferencia`) AS `transferencia`,
                    SUM(`e`.`tarjetas`) AS `tarjetas`
                FROM
                    (
                        SELECT
                            `tpo`.`id_tes_operacion` AS `id_movimiento`,
                            (
                                CASE
                                    `tc`.`nombre`
                                    WHEN 'EFECTIVO' THEN SUM(`tpo`.`valor`)
                                    ELSE 0
                                END
                            ) AS `efectivo`,
                            (
                                CASE
                                    `tc`.`nombre`
                                    WHEN 'CREDITO' THEN SUM(`tpo`.`valor`)
                                    ELSE 0
                                END
                            ) AS `credito`,
                            (
                                CASE
                                    WHEN (
                                        (`tc`.`nombre` = 'TARJETA DEBITO')
                                        OR (`tc`.`nombre` = 'TARJETA CREDITO')
                                    ) THEN SUM(`tpo`.`valor`)
                                    ELSE 0
                                END
                            ) AS `tarjetas`,
                            (
                                CASE
                                    WHEN (`tc`.`nombre` = 'TRANSFERENCIA BANCARIA') THEN SUM(`tpo`.`valor`)
                                    ELSE 0
                                END
                            ) AS `transferencia`,
                            (
                                CASE
                                    WHEN (`tc`.`nombre` = 'CHEQUES') THEN SUM(`tpo`.`valor`)
                                    ELSE 0
                                END
                            ) AS `cheques`
                        FROM
                            (
                                `tes_formas_pago` `tc`
                                JOIN `tes_pago_operaciones` `tpo` ON ((`tpo`.`id_forma_pago` = `tc`.`id`))
                            )
                        GROUP BY
                            `tpo`.`id_tes_operacion`,
                            `tc`.`nombre`
                    ) `e`
                GROUP BY
                    `e`.`id_movimiento`
            ) `fp` ON ((`fp`.`id_movimiento` = `tm`.`id`))
        )
        LEFT JOIN (
            SELECT
                `tcm`.`id_movimiento` AS `id_movimiento`,
                (
                    CASE
                        `gti`.`nombre`
                        WHEN 'IVA' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                        ELSE 0
                    END
                ) AS `IVA`,
                (
                    CASE
                        `gti`.`nombre`
                        WHEN 'RETEFUENTE' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                        ELSE 0
                    END
                ) AS `RETEFUENTE`,
                (
                    CASE
                        `gti`.`nombre`
                        WHEN 'RETEICA' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                        ELSE 0
                    END
                ) AS `RETEICA`,
                (
                    CASE
                        `gti`.`nombre`
                        WHEN 'RETEIVA' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                        ELSE 0
                    END
                ) AS `RETEIVA`,
                (
                    CASE
                        `gti`.`nombre`
                        WHEN 'RETECREE' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                        ELSE 0
                    END
                ) AS `RETECREE`
            FROM
                (
                    (
                        `gen_tipos_impuesto` `gti`
                        JOIN `gen_conceptos` `gc` ON ((`gti`.`id` = `gc`.`id_tipo_impuesto`))
                    )
                    JOIN `tes_conceptos_movimientos` `tcm` ON ((`gc`.`id` = `tcm`.`id_concepto`))
                )
            GROUP BY
                `tcm`.`id_movimiento`,
                `gti`.`nombre`
        ) `c` ON ((`c`.`id_movimiento` = `tm`.`id`))
    )
    JOIN con_cuentas cc ON (tm.id_cuenta = cc.id)
WHERE
    (
        `cf`.`codigo` = 'RC'
        AND `tm`.`id` = p_id
    );

SELECT
    *
FROM
    view_detalle_recibo_caja
WHERE
    id_movimiento = p_id;

SELECT
    SUM(descuento) AS descuento,
    SUM(`RETEFUENTE`) AS `retefuente`,
    SUM(`RETEICA`) AS `reteica`,
    SUM(`RETEIVA`) AS `reteiva`,
    SUM(`RETECREE`) AS `retecree`
FROM
    (
        SELECT
            `tcm`.`id_movimiento` AS `id_movimiento`,
            0.00 AS descuento,
            (
                CASE
                    `gti`.`nombre`
                    WHEN 'RETEFUENTE' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                    ELSE 0
                END
            ) AS `RETEFUENTE`,
            (
                CASE
                    `gti`.`nombre`
                    WHEN 'RETEICA' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                    ELSE 0
                END
            ) AS `RETEICA`,
            (
                CASE
                    `gti`.`nombre`
                    WHEN 'RETEIVA' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                    ELSE 0
                END
            ) AS `RETEIVA`,
            (
                CASE
                    `gti`.`nombre`
                    WHEN 'RETECREE' THEN ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0)
                    ELSE 0
                END
            ) AS `RETECREE`
        FROM
            `gen_tipos_impuesto` `gti`
            JOIN `gen_conceptos` `gc` ON ((`gti`.`id` = `gc`.`id_tipo_impuesto`))
            JOIN `tes_conceptos_movimientos` `tcm` ON ((`gc`.`id` = `tcm`.`id_concepto`))
        WHERE
            tcm.id_movimiento = p_id
        GROUP BY
            `tcm`.`id_movimiento`,
            `gti`.`nombre`
        UNION
        ALL
        SELECT
            `tcm`.`id_movimiento` AS `id_movimiento`,
            ROUND(SUM((`tcm`.`base` * (`tcm`.`tasa` * 0.01))), 0) AS descuento,
            0.00 AS `RETEFUENTE`,
            0.00 AS `RETEICA`,
            0.00 AS `RETEIVA`,
            0.00 AS `RETECREE`
        FROM
            `gen_conceptos` `gc`
            JOIN `tes_conceptos_movimientos` `tcm` ON ((`gc`.`id` = `tcm`.`id_concepto`))
        WHERE
            gc.naturaleza = 'D'
            AND `gc`.`id_tipo_impuesto` IS NULL
            AND tcm.id_movimiento = p_id
        GROUP BY
            `tcm`.`id_movimiento`,
            `gc`.`nombre`
    ) c;

END IF;

IF (TRIM(p_operacion) = 'EGRESO') THEN
/*MAESTRO DEL EGRESO*/
SELECT
    `tm`.`id` AS `id`,
    `cf`.`codigo` AS `operacion`,
    CAST(
        TRIM(`tm`.`documento_contable`) AS CHAR(12) CHARSET utf8mb4
    ) AS `comprobante`,
    `tm`.`fecha` AS `fecha`,
    `ct`.`id_tipo_regimen`,
    `ct`.`nombre_tercero` AS `beneficiario`,
    `ct`.`documento` AS `doc_beneficiario`,
    `cc`.`codigo` as cuenta,
    CONCAT(`cc`.`codigo`, '-', `cc`.`nombre`) AS `cuenta_contable`,
    lpad(`tm`.`numero_documento`, 4, '0') AS `documento`,
    `tm`.`total` AS `valor_egreso`,
    lpad(gs.codigo, 3, '0') as cod_sucursal,
    `gs`.`nombre` AS `sucursal`,
    IF(
        (`tb`.`tipo_banco` = 3),
        CONCAT(
            TRIM(`tb`.`nombre`),
            CONVERT(SPACE(1) USING utf8mb4),
            `tcb`.`nombre`
        ),
        TRIM(`tcb`.`nombre`)
    ) AS `banco`,
    `tfp`.`codigo` AS `metodo_pago`,
    tpo.numero as numero_pago,
    `tm`.`estado` AS `estado`,
    `tm`.`observacion`,
    tm.id_clase_operacion
FROM
    `tes_movimientos` `tm`
    JOIN `con_fuentes` `cf` ON ((`cf`.`id` = `tm`.`id_tipo_operacion`))
    JOIN `gen_sucursales` `gs` ON ((`gs`.`id` = `tm`.`id_sucursal`))
    JOIN `tes_cuentas_bancarias` `tcb` ON ((`tcb`.`id` = `tm`.`id_cuenta_bancaria`))
    JOIN `tes_bancos` `tb` ON ((`tb`.`id` = `tcb`.`id_banco`))
    JOIN `con_terceros` `ct` ON ((`ct`.`id` = `tm`.`id_tercero`))
    JOIN `con_cuentas` `cc` ON ((`cc`.`id` = `tm`.`id_cuenta`))
    JOIN `tes_pago_operaciones` `tpo` ON ((`tm`.`id` = `tpo`.`id_tes_operacion`))
    JOIN `tes_formas_pago` `tfp` ON ((`tfp`.`id` = `tpo`.`id_forma_pago`))
WHERE
    `cf`.`codigo` = 'CE'
    and tm.`id` = p_id;

/* CONTABILIDAD DEL DOCUMENTO*/
SELECT
    c.`codigo`,
    concat(
        c.`nombre`,
        ' ',
        if(
            tm.id_clase_operacion in(2, 3),
            upper(trim(mc.detalle)),
            ''
        )
    ) as nombre,
    cc.`codigo` AS cc,
    mc.debito,
    mc.credito
FROM
    con_movimiento_contable mc
    LEFT JOIN tes_movimientos tm ON (
        tm.documento_contable = mc.`comprobante`
        and tm.id_tipo_operacion = mc.id_fuente
        and tm.id_tercero = mc.id_tercero
    )
    LEFT JOIN con_fuentes cf ON (
        cf.id = mc.`id_fuente`
        AND mc.`id_sucursal` = tm.id_sucursal
        AND mc.`id_tercero` = tm.id_tercero
    )
    LEFT JOIN con_cuentas c ON c.id = mc.`id_cuenta`
    LEFT JOIN con_centros_costo cc ON cc.id = mc.`id_centro_costo`
WHERE
    tm.id = p_id;

/* DETALLE DEL DOCUMENTO*/
select
    gs.codigo as sucursal,
    cf.codigo as tipo,
    cmo.numero_documento as documento,
    cmo.fecha,
    cmo.total as valor_factura,
    (cmo.valor_abono - tdm.valor) as abonos,
    tdm.valor as valor_pago,
    (cmo.total - cmo.valor_abono) as nuevo_saldo,
    cc.codigo as cuenta_x_pagar
from
    tes_detalle_movimientos tdm
    inner join cio_maestro_operaciones cmo on cmo.id = tdm.id_documento_afectado
    inner join con_fuentes cf on cf.id = cmo.id_tipo_operacion
    inner join gen_sucursales gs on gs.id = cmo.id_sucursal
    inner join con_cuentas cc on cc.id = cmo.id_cuenta
    inner join con_terceros ct on ct.id = tdm.id_tercero
WHERE
    tdm.id_movimiento = p_id;

END IF;

IF (TRIM(p_operacion) = 'CAUSA_GASTOS') THEN
SELECT
    m.`numero_documento`,
    m.`documento_contable`,
    m.fecha,
    m.`fecha_vencimiento`,
    m.`total`,
    m.`observaciones`,
    t.`nombre_tercero`,
    t.`documento` documento_tercero,
    t.`digito`,
    s.`codigo` codigo_sede,
    s.`nombre` nombre_sede,
    usu.nombre usuario,
    m.`subtotal`,
    m.valor_iva,
    m.`valor_retenciones`,
    m.`valor_descuento`
FROM
    `cio_maestro_operaciones` `m`
    JOIN `con_terceros` `t` ON `m`.`id_tercero` = `t`.`id`
    JOIN `gen_sucursales` `s` ON `m`.`id_sucursal` = `s`.`id`
    JOIN `view_usuarios` usu ON m.`id_usuario` = usu.`id`
WHERE
    m.id = p_id;

SELECT
    c.id,
    c.detalle,
    cc.`codigo` codigo_cc,
    cc.`nombre` nombre_cc,
    t.`nombre_tercero`,
    cu.codigo codigo_cu,
    cu.`nombre` nombre_cu,
    c.`debito`,
    c.`credito`
FROM
    con_movimiento_contable c
    join con_fuentes cf on cf.id = c.id_fuente
    join `cio_maestro_operaciones` cmo on cmo.documento_contable = c.comprobante
    and c.id_fuente = cmo.id_tipo_operacion
    JOIN `con_centros_costo` cc ON c.`id_centro_costo` = cc.`id`
    JOIN `con_terceros` `t` ON `c`.`id_tercero` = `t`.`id`
    JOIN `con_cuentas` cu ON c.`id_cuenta` = cu.`id`
where
    cmo.id = p_id
    and cf.codigo in('FP', 'FG');

END IF;

if (TRIM(p_operacion) = 'consignacion') then
select
    gs.nombre as sucursal,
    cc.codigo as cuenta_banco_consigna,
    cc2.codigo AS cuenta_banco_recibe,
    if(
        tb.tipo_banco = 3,
        concat(trim(tb.nombre), space(1), tcb.nombre),
        trim(tcb.nombre)
    ) as banco_consigna,
    if(
        tb2.tipo_banco = 3,
        concat(trim(tb2.nombre), space(1), tcb2.nombre),
        trim(tcb2.nombre)
    ) as banco_recibe,
    tc.numero_consignacion,
    tc.fecha,
    tc.valor_cheque,
    tc.valor_retencion,
    tc.valor_efectivo,
    tc.valor_retencion as valor_retefuente,
    tc.valor_reteica,
    tc.valor_reteiva,
    tc.valor_comision,
    tc.total_consignacion,
    trim(cf.nombre) as tipo
from
    tes_consignaciones tc
    inner join gen_sucursales gs on gs.id = tc.id_sucursal
    inner join con_fuentes cf on cf.id = tc.id_tipo_operacion
    inner join tes_cuentas_bancarias tcb on tcb.id = tc.id_cuenta_banco
    inner join tes_bancos tb on tb.id = tcb.id_banco
    INNER JOIN `con_cuentas` `cc` ON `cc`.`id` = `tcb`.`id_cuenta`
    inner join tes_cuentas_bancarias tcb2 on tcb2.id = tc.id_cuenta_banco_recibe
    INNER JOIN `con_cuentas` `cc2` ON `cc2`.`id` = `tcb2`.`id_cuenta`
    inner join tes_bancos tb2 on tb2.id = tcb2.id_banco
    inner join admin_usuarios au on au.id = tc.id_usuario
    inner join con_terceros ct on ct.id = au.id_tercero
where
    tc.id = p_id;

select
    id_sucursal,
    id_tipo_operacion,
    comprobante into lc_id_sucursal,
    lc_id_fuente,
    lc_comprobante
from
    tes_consignaciones
where
    id = p_id;

SELECT
    c.`codigo`,
    c.`nombre` as nombre,
    cc.`codigo` AS cc,
    mc.debito,
    mc.credito,
    case
        c.id_tipo_cuenta
        WHEN 0 THEN ''
        WHEN 1 THEN 'CAJA'
        WHEN 2 THEN 'INVENTARIOS'
        WHEN 3 THEN 'IVA'
        WHEN 4 THEN 'RETEFUENTE'
        WHEN 5 THEN 'RETEICA'
        WHEN 6 THEN 'RETEIVA'
        WHEN 7 THEN 'BANCOS'
    END as grupo
FROM
    con_movimiento_contable mc
    inner JOIN con_fuentes cf ON cf.id = mc.`id_fuente`
    inner JOIN con_cuentas c ON c.id = mc.`id_cuenta`
    inner JOIN con_centros_costo cc ON cc.id = mc.`id_centro_costo`
WHERE
    mc.`comprobante` = lc_comprobante
    and mc.id_fuente = lc_id_fuente
    and mc.id_sucursal = lc_id_sucursal;

end if;

if (UPPER(TRIM(p_operacion)) = 'MOVIMIENTO_INV') then
select
    id_fuente,
    id_tercero,
    id_bodega,
    numero_documento,
    fecha into lc_id_fuente,
    lc_id_tercero,
    lc_id_sucursal,
    lc_documento,
    lc_fecha
from
    inv_kardex
where
    id = p_id;

select
    distinct concat(trim(gs.nombre), ' (', trim(gb.nombre), ')') as bodega,
    concat(trim(gc.codigo), '-', trim(gc.nombre)) as concepto,
    ik.numero_documento,
    ik.fecha,
    ifnull(ct.nombre_tercero, "SIN TERCERO") nombre_tercero,
    ctu.nombre_tercero as usuario,
    if(gc.naturaleza = 'E', 'ENTRADA', 'SALIDA') as tipo,
    ik.observacion
from
    inv_kardex ik
    inner join con_fuentes cf on cf.id = ik.id_fuente
    inner join gen_conceptos gc on trim(gc.codigo) = trim(cf.codigo)
    inner join gen_bodegas gb on gb.id = ik.id_bodega
    inner join gen_sucursales gs on gs.id = gb.id_sucursal
    LEFT join con_terceros ct on ct.id = ik.id_tercero
    inner join admin_usuarios au on au.id = ik.id_usuario
    inner join con_terceros ctu on ctu.id = au.id_tercero
where
    ik.id = p_id;

select
    ip.codigo,
    IF(
        pre.`id` IS NULL,
        ip.`nombre`,
        CONCAT(ip.nombre, ' - ', pre.`descripcion`)
    ) nombre,
    ifnull(pre.`descripcion`, ip.referencia) referencia,
    ik.cantidad,
    ik.valor_costo,
    (ik.cantidad * ik.valor_costo) as totales
from
    inv_kardex ik
    inner join inv_productos ip on ip.id = ik.id_producto
    LEFT JOIN `inv_productos_unidades` pre ON pre.id = ik.id_und_empaque
where
    ik.id_fuente = lc_id_fuente
    and ifnull(ik.id_tercero, 0) = ifnull(lc_id_tercero, 0)
    and ik.id_bodega = lc_id_sucursal
    and ik.numero_documento = lc_documento
    and ik.fecha = lc_fecha;

end if;

IF (UPPER(TRIM(p_operacion)) = 'NOTASBANCARIAS') THEN
SELECT
    ct.nombre_tercero AS tercero,
    IF(
        NOT ISNULL(ct.digito)
        AND ct.digito != '',
        CONCAT(TRIM(ct.documento), '-', ct.digito),
        TRIM(ct.documento)
    ) AS documento,
    ct.direccion,
    ct.telefono,
    TRIM(gm.nombre) AS municipio,
    tm.fecha,
    tm.`documento_contable`,
    tm.total,
    tm.observacion,
    ctu.nombre_tercero AS usuario
FROM
    tes_movimientos tm
    INNER JOIN con_fuentes cf ON cf.id = tm.id_tipo_operacion
    INNER JOIN con_terceros ct ON ct.id = tm.id_tercero
    INNER JOIN gen_municipios gm ON gm.id = ct.id_municipio
    INNER JOIN admin_usuarios au ON au.id = tm.id_usuario
    INNER JOIN con_terceros ctu ON ctu.id = au.id_tercero
WHERE
    cf.codigo = 'NB'
    AND tm.`id` = p_id;

SELECT
    cc.codigo,
    TRIM(gc.nombre) AS concepto,
    tdm.valor
FROM
    tes_detalle_movimientos tdm
    INNER JOIN gen_conceptos gc ON gc.id = tdm.id_concepto
    INNER JOIN con_cuentas cc ON cc.id = gc.id_cuenta
WHERE
    tdm.id_movimiento = p_id;

END IF;

IF (TRIM(p_operacion) = 'ORDEN_COMPRA') THEN
SELECT
    m.`numero_documento`,
    m.fecha,
    m.`fecha_vencimiento`,
    m.`total`,
    m.`observaciones`,
    t.`nombre tercero` nombre_tercero,
    t.`documento` documento_tercero,
    t.`direccion`,
    t.`celular`,
    t.`email`,
    t.ciudad,
    t.plazo,
    b.`codigo` codigo_bod,
    b.`nombre` AS nombre_bodega,
    s.`nombre` nombre_sede,
    usu.nombre usuario,
    m.`subtotal`,
    m.valor_iva,
    m.`valor_retenciones`,
    m.`valor_descuento`,
    m.valor_flete
FROM
    `cio_maestro_operaciones` `m`
    JOIN `view_terceros` `t` ON `m`.`id_tercero` = `t`.`id_tercero`
    JOIN `gen_bodegas` `b` ON `m`.`id_bodega` = `b`.`id`
    JOIN `gen_sucursales` s ON s.`id` = b.`id_sucursal`
    JOIN `view_usuarios` usu ON m.`id_usuario` = usu.`id`
WHERE
    m.id = p_id;

SELECT
    pro.`codigo` AS `referencia:80`,
    IF(
        pre.id IS NULL,
        pro.nombre,
        CONCAT(pro.nombre, " - ", pre.descripcion)
    ) AS `Nombre:FLT`,
    IF(pre.id IS NULL, me.`abreviatura`, pre.descripcion) abreviatura,
    d.`cantidad`,
    d.valor,
    d.`tasa_descuento`,
    d.`tasa_iva`,
    (d.valor * d.`cantidad`) - ROUND(
        ((d.valor * d.`tasa_descuento` / 100) * d.`cantidad`),
        2
    ) + ROUND(((d.valor * d.`tasa_iva` / 100) * d.`cantidad`), 2) total
FROM
    `cio_detalle_operaciones` d
    JOIN `inv_productos` pro ON pro.`id` = d.`id_producto`
    LEFT JOIN `inv_productos_unidades` pre ON d.`id_presentacion` = pre.`id`
    JOIN `inv_medidas` me ON d.`id_medida` = me.`id`
WHERE
    `id_maestro_operacion` = p_id;

END IF;

IF (TRIM(p_operacion) = 'ENTRADA_ALMACEN') THEN
SELECT
    DISTINCT m.`numero_documento`,
    m.fecha,
    m.`fecha_vencimiento`,
    m.`total`,
    m.`observaciones`,
    k.`numero_remision` AS no_entrada,
    t.`nombre tercero` nombre_tercero,
    t.`documento` documento_tercero,
    t.`direccion`,
    t.`celular`,
    t.`email`,
    t.ciudad,
    t.plazo,
    b.`codigo` codigo_bod,
    b.`nombre bodega` nombre_bodega,
    b.`sucursal` nombre_sede,
    usu.nombre usuario,
    m.`subtotal`,
    m.valor_iva,
    m.`valor_retenciones`,
    m.`valor_descuento`,
    m.valor_flete
FROM
    `cio_maestro_operaciones` `m`
    LEFT JOIN `view_terceros` `t` ON `m`.`id_tercero` = `t`.`id_tercero`
    LEFT JOIN `view_bodegas` `b` ON `m`.`id_bodega` = `b`.`id`
    LEFT JOIN `view_usuarios` usu ON m.`id_usuario` = usu.`id`
    LEFT JOIN inv_kardex k ON k.`numero_compra` = m.`numero_documento`
WHERE
    m.id = p_id;

SELECT
    DISTINCT cf.`codigo`,
    ip.id,
    ip.codigo,
    ic.nombre AS categoria,
    ip.nombre,
    im.id AS id_medida,
    im.`abreviatura`,
    cdo.`cantidad` AS requerida,
    cdo.`cantidad` AS autorizada,
    cdo.`cantidad_entregada` AS entregada,
    cdo.`cantidad_facturada` AS facturada,
    cdo.`costo`
FROM
    cio_maestro_operaciones cm
    INNER JOIN cio_detalle_operaciones cdo ON cdo.`id_maestro_operacion` = cm.id
    INNER JOIN inv_productos ip ON ip.id = cdo.`id_producto`
    INNER JOIN inv_categorias ic ON ic.id = ip.`id_categoria`
    INNER JOIN inv_medidas im ON im.id = ip.`id_medida`
    INNER JOIN con_fuentes cf ON cf.id = cm.`id_tipo_operacion`
WHERE
    cm.id = p_id;

END IF;

IF (TRIM(p_operacion) = 'TRASLADO_UNIDAD') THEN
SELECT
    b.nombre AS bodega_manda,
    un.nombre AS bodega_recibe,
    t.fecha,
    usu.nombre,
    t.*
FROM
    `inv_maestro_traslados` t
    LEFT JOIN gen_bodegas b ON b.id = t.id_bodega
    LEFT JOIN gen_bodegas un ON un.id = t.id_bodega_destino
    LEFT JOIN view_usuarios usu ON usu.id = t.id_usuario_recibe
WHERE
    t.id = p_id;

SELECT
    DISTINCT p.codigo,
    t.fecha,
    t.fecha_aprobacion,
    p.nombre,
    im.abreviatura,
    dt.cantidad AS enviada,
    dt.cantidad_aprobada,
    p.ultimo_costo,
    ii.cantidad AS cantidad_requerida
FROM
    inv_detalle_traslados dt
    INNER JOIN inv_maestro_traslados t ON t.id = dt.id_maestro_traslado
    INNER JOIN inv_productos p ON p.id = dt.id_producto
    INNER JOIN inv_medidas im ON im.id = p.id_medida
    INNER JOIN prod_minutas_contratos pmc ON pmc.id_producto = dt.id_producto
    INNER JOIN inv_inventario ii ON ii.id_producto = dt.id_producto
    AND ii.id_bodega = t.id_bodega
WHERE
    id_maestro_traslado = p_id;

END IF;

IF (TRIM(p_operacion) = 'TOMA_FISICA') THEN
select
    *
from
    `gen_bodegas`
where
    id = p_id;

SELECT
    NULL `fecha`,
    NULL `consecutivo`,
    pro.`id` id_producto,
    pro.codigo,
    sub.`id_linea`,
    IF(
        pre.`id` IS NULL,
        pro.`nombre`,
        CONCAT(pro.nombre, ' - ', pre.`descripcion`)
    ) nombre_producto,
    un.id id_unidad,
    pre.`id` id_presentacion,
    COALESCE(pre.`descripcion`, un.`nombre`) unidad,
    COALESCE(inv.`ultimo_costo`, pro.ultimo_costo) ultimo_costo,
    IFNULL(inv.`cantidad`, 0) sistema,
    0 fisico,
    IFNULL(inv.`cantidad`, 0) diferencia
FROM
    `inv_productos` pro
    JOIN `inv_sublineas` sub ON pro.`id_sublineas` = sub.`id`
    JOIN inv_medidas un ON pro.`id_medida` = un.`id`
    LEFT JOIN `inv_productos_unidades` pre ON pro.id = pre.`id_producto`
    LEFT JOIN `inv_inventario` inv ON pro.`id` = inv.`id_producto`
    AND COALESCE(pre.id, pro.`id_medida`) = inv.`id_und_empaque`
WHERE
    `id_tipo_producto` < 3
    AND (
        inv.`id_bodega` = p_id
        OR inv.`id_bodega` IS NULL
    )
ORDER BY
    pro.`nombre`;

END IF;

IF (TRIM(p_operacion) = 'APLICAR_TOMA_FISICA') THEN
SELECT
    *
FROM
    `gen_bodegas`
WHERE
    id =(
        SELECT
            id_almacen
        FROM
            inv_toma_fisica
        WHERE
            consecutivo = p_id
        LIMIT
            1
    );

SELECT
    tf.`fecha`,
    tf.`consecutivo`,
    pro.`id` id_producto,
    pro.codigo,
    sub.`id_linea`,
    IF(
        pre.`id` IS NULL,
        pro.`nombre`,
        CONCAT(pro.nombre, ' - ', pre.`descripcion`)
    ) nombre_producto,
    tf.id_unidad,
    tf.`id_unidad_empaque` id_presentacion,
    COALESCE(pre.`descripcion`, un.`nombre`) unidad,
    tf.ultimo_costo,
    tf.cantidad_sistema sistema,
    tf.`cantidad_fisica` fisico,
    tf.`diferencia` diferencia,
    if(
        tf.`diferencia` < 0,
((tf.`diferencia` * -1) * tf.ultimo_costo),
        0
    ) credito,
    IF(
        tf.`diferencia` > 0,
(tf.`diferencia` * tf.ultimo_costo),
        0
    ) debito,
    DATE(tf.`fechasys`) fechasys
FROM
    `inv_toma_fisica` tf
    JOIN `inv_productos` pro ON tf.`id_producto` = pro.id
    JOIN `inv_sublineas` sub ON pro.`id_sublineas` = sub.`id`
    JOIN inv_medidas un ON tf.`id_unidad` = un.`id`
    LEFT JOIN `inv_productos_unidades` pre ON tf.`id_unidad_empaque` = pre.`id`
WHERE
    tf.consecutivo = p_id
    AND tf.estado = 1
ORDER BY
    pro.`nombre`;

END IF;

end