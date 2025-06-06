drop procedure if exists sp_egresos;

delimiter $ $ CREATE PROCEDURE `sp_egresos`(
    `p_id` INT,
    `p_id_sucursal` INT,
    `p_id_tercero` INT,
    `p_id_cuenta_bancaria` INT,
    `p_id_cuenta` INT,
    `p_id_usuario` INT,
    `p_id_centro_costo` INT,
    `p_id_metodo_pago` INT,
    `p_fecha` DATE,
    `p_subtotal` decimal(18, 2),
    `p_valor_descuento` decimal(18, 2),
    `p_valor_retenciones` decimal(18, 2),
    `p_total` decimal(18, 2),
    `p_tercero_autorizado` varchar(150),
    `p_detalle` JSON,
    `p_detalle_conceptos` JSON,
    `p_detalle_pago` JSON,
    `p_tipo_egreso` INT,
    `p_accion` varchar(10)
) begin declare lc_id_fuente int;

declare lc_numero_documento int;

declare lc_id_periodo int;

declare lc_estado_periodo int;

declare lc_comprobante varchar(15);

declare lc_id_cuenta_anticipo_cliente int;

declare lc_id_cuenta_anticipo_proveedor int;

declare lc_detalle varchar(300);

declare lc_tercero varchar(100);

declare lc_centro_costo int;

declare p_numero_cheque int;

DECLARE P_JSON JSON default '[]';

declare p_fecsys datetime;

declare p_estado int;

DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
@text = MESSAGE_TEXT;

SET
    @full_error = CONCAT("ERROR ", @text);

SELECT
    @full_error as sqlMessage;

ROLLBACK;

END;

if(p_accion = 'ter_DAC') then
select
    *,
(
        select
            sum(saldo_anticipo) as saldo
        from
            view_tercero_anticipos
        where
            saldo_anticipo > 0
            and id_tercero = vt.id_tercero
    ) as saldo_cartera
from
    view_terceros vt
where
    vt.id_tercero in(
        select
            id_tercero
        from
            view_tercero_anticipos
        where
            saldo_anticipo > 0
    );

end if;

if(p_accion = 'ter_CE') then
select
    *,
    (
        select
            SUM((cmo.total - cmo.valor_abono)) as saldo
        from
            con_fuentes cf
            inner join cio_maestro_operaciones cmo on cmo.id_tipo_operacion = cf.id
            inner join gen_sucursales gs on gs.id = cmo.id_sucursal
        where
            cf.codigo in ('FP', 'ND', 'FG')
            and cmo.estado = 1
            and (cmo.total - cmo.valor_abono) > 0
            and cmo.id_tercero = vt.id_tercero
    ) as saldo_cartera
from
    view_terceros vt
where
    vt.id_tercero in(
        select
            distinct cmo.id_tercero
        from
            con_fuentes cf
            inner join cio_maestro_operaciones cmo on cmo.id_tipo_operacion = cf.id
        where
            cf.codigo in ('FP', 'ND', 'FG')
            and cmo.estado = 1
            and (cmo.total - cmo.valor_abono) > 0
    );

end if;

if(p_accion = 'ter_AP') then
select
    *,
(
        select
            SUM((cmo.total -(cmo.valor_abono)) - cmo.valor_anticipo) as saldo
        from
            con_fuentes cf
            inner join cio_maestro_operaciones cmo on cmo.id_tipo_operacion = cf.id
            inner join gen_sucursales gs on gs.id = cmo.id_sucursal
        where
            cf.codigo = 'OC'
            and cmo.estado = 1
            and cmo.id_tercero = vt.id_tercero
    ) as saldo_cartera
from
    view_terceros vt
where
    vt.id_tercero in(
        select
            distinct cmo.id_tercero
        from
            con_fuentes cf
            inner join cio_maestro_operaciones cmo on cmo.id_tipo_operacion = cf.id
        where
            cf.codigo = 'OC'
            and cmo.estado = 1
    );

end if;

if(p_accion = 'listar_doc') then if(p_tipo_egreso = 2) then
select
    cmo.id,
    cf.codigo as tipo,
    cmo.numero_documento,
    cmo.id_sucursal,
    gs.nombre as sucursal,
    cmo.fecha,
    cmo.fecha as fecha_vencimiento,
    DATEDIFF(CURDATE(), cmo.fecha) as mora,
    cmo.total,
    cmo.valor_anticipo + cmo.valor_abono as valor_anticipo,
    (cmo.total -(cmo.valor_abono)) - cmo.valor_anticipo as saldo,
    0.00 as valor_pago,
    false as `check`
from
    con_fuentes cf
    inner join cio_maestro_operaciones cmo on cmo.id_tipo_operacion = cf.id
    inner join gen_sucursales gs on gs.id = cmo.id_sucursal
where
    cf.codigo = 'OC'
    and cmo.estado = 1
    and cmo.id_tercero = p_id_tercero
order by
    cmo.fecha asc;

end if;

if(p_tipo_egreso = 3) then
select
    cmo.id,
    cf.codigo as tipo,
    cmo.numero_documento,
    cmo.id_sucursal,
    gs.nombre as sucursal,
    cmo.fecha,
    cmo.fecha_vencimiento,
    DATEDIFF(CURDATE(), cmo.fecha_vencimiento) as mora,
    cmo.total,
    cmo.valor_anticipo + cmo.valor_abono as valor_anticipo,
    (cmo.total - cmo.valor_abono) as saldo,
    0.00 as valor_pago,
    false as `check`
from
    con_fuentes cf
    inner join cio_maestro_operaciones cmo on cmo.id_tipo_operacion = cf.id
    inner join gen_sucursales gs on gs.id = cmo.id_sucursal
where
    cf.codigo in ('FP', 'ND', 'FG')
    and cmo.estado = 1
    and (cmo.total - cmo.valor_abono) > 0
    and cmo.id_tercero = p_id_tercero
order by
    cmo.fecha_vencimiento asc;

end if;

if(p_tipo_egreso = 4) then
select
    id,
    id as numero_documento,
    'AC' as tipo,
    id_sucursal,
    sucursal,
    current_date() as fecha,
    current_date() as fecha_vencimiento,
    0 as mora,
    valor_anticipo as total,
    0.00 as valor_anticipo,
    saldo_anticipo as saldo,
    0.00 as valor_pago,
    false as `check`
from
    view_tercero_anticipos
where
    saldo_anticipo > 0
    and id_tercero = p_id_tercero;

end if;

end if;

if (p_accion = 'guardar') then start transaction;

set
    p_estado = 1;

set
    lc_centro_costo = p_id_centro_costo;

set
    p_valor_descuento = coalesce(p_valor_descuento, 0.00);

set
    p_valor_retenciones = coalesce(p_valor_retenciones, 0.00);

set
    p_fecsys = current_timestamp();

SELECT
    TRIM(coalesce(nombre_tercero, 'NO REGISTRA')) into lc_tercero
FROM
    desarrollo.con_terceros
WHERE
    id = p_id_tercero;

SELECT
    id into lc_id_fuente
FROM
    desarrollo.con_fuentes
WHERE
    codigo = 'CE';

if (
    (
        SELECT
            count(*)
        FROM
            con_periodos
        where
            codigo = concat(year(p_fecha), lpad(month(p_fecha), 2, '0'))
    ) > 0
) then
SELECT
    id,
    estado into lc_id_periodo,
    lc_estado_periodo
FROM
    con_periodos
where
    codigo = concat(year(p_fecha), lpad(month(p_fecha), 2, '0'));

if (lc_estado_periodo = 1) then signal sqlstate '45000'
set
    message_text = '[ERROR] el periodo contable está cerrado';

end if;

else
insert into
    con_periodos (codigo, estado)
values
    (
        concat(year(p_fecha), lpad(month(p_fecha), 2, '0')),
        0
    );

select
    last_insert_id() into lc_id_periodo;

end if;

if (p_id_metodo_pago = 6) then
update
    tes_cuentas_bancarias
set
    consecutivo_cheque = coalesce(consecutivo_cheque, 0) + 1
where
    id = p_id_cuenta_bancaria;

select
    consecutivo_cheque into p_numero_cheque
from
    desarrollo.tes_cuentas_bancarias
where
    id = p_id_cuenta_bancaria;

else
set
    p_numero_cheque = 0;

END IF;

call `sp_consecutivo_fuentes`(
    p_id_sucursal,
    'CE',
    p_fecha,
    'GL',
    false,
    lc_numero_documento
);

select
    concat(
        year(p_fecha),
        '-',
        lpad(month(p_fecha), 2, '0'),
        '-',
        lpad(lc_numero_documento, 4, '0')
    ) into lc_comprobante;

INSERT INTO
    `desarrollo`.`tes_movimientos` (
        `id_tipo_operacion`,
        `id_sucursal`,
        `id_tercero`,
        `id_cuenta`,
        `id_usuario`,
        `numero_documento`,
        `numero_cheque`,
        `documento_contable`,
        `fecha`,
        `subtotal`,
        `valor_descuento`,
        `valor_retenciones`,
        `total`,
        `tercero_autorizado`,
        `fecsys`,
        `estado`,
        `id_cuenta_bancaria`,
        id_clase_operacion
    )
VALUES
    (
        lc_id_fuente,
        p_id_sucursal,
        p_id_tercero,
        p_id_cuenta,
        p_id_usuario,
        lc_numero_documento,
        p_numero_cheque,
        lc_comprobante,
        p_fecha,
        p_subtotal,
        coalesce(p_valor_descuento, 0.00),
        coalesce(p_valor_retenciones, 0.00),
        p_total,
        p_tercero_autorizado,
        p_fecsys,
        p_estado,
        p_id_cuenta_bancaria,
        p_tipo_egreso
    );

select
    last_insert_id() into p_id;

if (p_tipo_egreso != 1) then
INSERT INTO
    `desarrollo`.`tes_detalle_movimientos` (
        `id_movimiento`,
        `id_documento_afectado`,
        `valor`,
        `estado`
    )
select
    p_id as id_movimiento,
    dv.`id_documento_afectado`,
    dv.`valor`,
    1 as `estado`
from
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_documento_afectado` int PATH "$.id_documento_afectado" null on empty null on error,
            `valor` decimal(18, 2) PATH "$.valor" null on empty null on error
        )
    ) dv;

else
INSERT INTO
    `desarrollo`.`tes_detalle_movimientos` (
        `id_movimiento`,
        `id_documento_afectado`,
        `valor`,
        `id_tercero`,
        `estado`
    )
select
    distinct p_id as id_movimiento,
    null as `id_documento_afectado`,
    dv.debito as `valor`,
    dv.id_tercero,
    1 as `estado`
from
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_tercero` int PATH '$.id_tercero' null on empty null on error,
            `debito` decimal(18, 2) PATH '$.debito' null on empty null on error
        )
    ) dv
where
    dv.debito > 0;

end if;

if (NOT ISNULL(p_detalle_conceptos)) THEN
INSERT INTO
    `desarrollo`.`tes_conceptos_movimientos` (
        `id_movimiento`,
        `id_concepto`,
        `tasa`,
        `base`,
        `estado`
    )
select
    p_id as id_movimiento,
    v.`id_concepto`,
    v.`tasa`,
    v.`base`,
    1 as `estado`
from
    JSON_TABLE(
        p_detalle_conceptos,
        '$[*]' columns(
            `id_concepto` int path "$.id_concepto" null on empty null on error,
            `tasa` numeric(8, 5) path "$.tasa" null on empty null on error,
            `base` decimal(18, 2) path "$.base" null on empty null on error
        )
    ) v;

end if;

if (NOT ISNULL(p_detalle_conceptos)) THEN
INSERT INTO
    `desarrollo`.`tes_pago_operaciones` (
        `id_forma_pago`,
        `id_franquicia`,
        `id_cio_operacion`,
        `id_tes_operacion`,
        `numero`,
        `valor`,
        `fecha`,
        `estado`
    )
select
    p_id_metodo_pago as `id_forma_pago`,
    null as `id_franquicia`,
    null as `id_cio_operacion`,
    p_id as `id_tes_operacion`,
    coalesce(
        if(
            replace(JSON_EXTRACT(p_detalle_pago, '$.numero'), '"', '') = 'null',
            null,
            replace(JSON_EXTRACT(p_detalle_pago, '$.numero'), '"', '')
        ),
        lc_numero_documento
    ) as `numero`,
    p_total as `valor`,
    replace(JSON_EXTRACT(p_detalle_pago, '$.fecha'), '"', '') as `fecha`,
    1 as `estado`;

end if;

SET
    SQL_SAFE_UPDATES = 0;

if (
    p_tipo_egreso = 2
    or p_tipo_egreso = 3
) then
INSERT INTO
    `desarrollo`.`gen_detalle_movimientos` (
        `id_fuente`,
        `id_operacion_origen`,
        `id_operacion_afectada`,
        `id_sucursal`,
        `id_tercero`,
        `id_clase`,
        `id_usuario`,
        `fecha`,
        `debito`,
        `credito`,
        `fecsys`,
        `estado`
    )
select
    if(
        p_tipo_egreso = 2,
        (
            select
                id
            from
                con_fuentes
            where
                codigo = 'AP'
        ),
        lc_id_fuente
    ) as `id_fuente`,
    p_id as `id_operacion_origen`,
    dv.id_documento_afectado as `id_operacion_afectada`,
    p_id_sucursal as `id_sucursal`,
    p_id_tercero as `id_tercero`,
    1 as `id_clase`,
    p_id_usuario as `id_usuario`,
    p_fecha as `fecha`,
    dv.valor as `debito`,
    0.0 as `credito`,
    now() as `fecsys`,
    1 as `estado`
from
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_documento_afectado` int PATH "$.id_documento_afectado" null on empty null on error,
            `valor` decimal(18, 2) PATH "$.valor" null on empty null on error
        )
    ) dv;

update
    cio_maestro_operaciones cmo,
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_documento_afectado` int PATH "$.id_documento_afectado" null on empty null on error,
            `valor` decimal(18, 2) PATH "$.valor" null on empty null on error
        )
    ) dv
set
    cmo.valor_anticipo = if(
        p_tipo_egreso = 2,
        coalesce(cmo.valor_anticipo, 0.0) + dv.valor,
        cmo.valor_anticipo
    ),
    cmo.valor_abono = if(
        p_tipo_egreso = 2,
        cmo.valor_abono,
        coalesce(cmo.valor_abono, 0.00) + dv.valor
    )
where
    cmo.id = dv.id_documento_afectado;

end if;

if (p_tipo_egreso = 4) then
update
    `desarrollo`.`tes_anticipos` ta,
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_documento_afectado` int PATH "$.id_documento_afectado" null on empty null on error,
            `valor` decimal(18, 2) PATH "$.valor" null on empty null on error
        )
    ) dv
set
    ta.`abono_anticipo` = coalesce(ta.abono_anticipo, 0.0) + dv.valor,
    ta.`saldo_anticipo` = if(
        coalesce(ta.saldo_anticipo, 0.0) <= 0,
        0,
        coalesce(ta.saldo_anticipo, 0.0) - dv.valor
    )
where
    ta.`id` = dv.id_documento_afectado;

INSERT INTO
    `desarrollo`.`gen_detalle_movimientos` (
        `id_fuente`,
        `id_operacion_origen`,
        `id_operacion_afectada`,
        `id_sucursal`,
        `id_tercero`,
        `id_clase`,
        `id_usuario`,
        `fecha`,
        `debito`,
        `credito`,
        `fecsys`,
        `estado`
    )
select
    lc_id_fuente as `id_fuente`,
    p_id as `id_operacion_origen`,
    dv.id_documento_afectado as `id_operacion_afectada`,
    p_id_sucursal as `id_sucursal`,
    p_id_tercero as `id_tercero`,
    2 as `id_clase`,
    p_id_usuario as `id_usuario`,
    p_fecha as `fecha`,
    0.00 as `debito`,
    dv.valor as `credito`,
    now() as `fecsys`,
    1 as `estado`
from
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_documento_afectado` int PATH "$.id_documento_afectado" null on empty null on error,
            `valor` decimal(18, 2) PATH "$.valor" null on empty null on error
        )
    ) dv;

end if;

SET
    SQL_SAFE_UPDATES = 1;

if (p_tipo_egreso = 1) then
select
    JSON_MERGE_PRESERVE(
        P_JSON,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'id_tercero',
                coalesce(dv.id_tercero, p_id_tercero),
                'id_fuente',
                lc_id_fuente,
                'id_periodo',
                lc_id_periodo,
                'id_sucursal',
                p_id_sucursal,
                'id_centro_costo',
                dv.id_centro_costo,
                'id_cuenta',
                dv.id_cuenta,
                'id_usuario',
                p_id_usuario,
                'comprobante',
                lc_comprobante,
                'documento_origen',
                lc_numero_documento,
                'debito',
                dv.debito,
                'credito',
                dv.credito,
                'fecha',
                p_fecha,
                'detalle',
                concat(
                    if(
                        dv.debito > 0,
                        concat(
                            'Pago a ',
                            trim(ct.nombre_tercero),
                            ', egreso N°: '
                        ),
                        'Valor total egreso N°: '
                    ),
                    lc_numero_documento
                )
            )
        )
    ) into P_JSON
from
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_tercero` int PATH '$.id_tercero' null on empty null on error,
            `id_centro_costo` int PATH '$.id_centro_costo' null on empty null on error,
            `id_cuenta` int PATH '$.id_cuenta' null on empty null on error,
            `debito` decimal(18, 2) PATH '$.debito' null on empty null on error,
            `credito` decimal(18, 2) PATH '$.credito' null on empty null on error
        )
    ) dv
    inner join desarrollo.con_terceros ct on ct.id = coalesce(dv.id_tercero, p_id_tercero);

end if;

set
    lc_id_cuenta_anticipo_proveedor = (
        SELECT
            gdpc.id_cuenta
        FROM
            desarrollo.gen_tipo_registros gtr
            inner join desarrollo.gen_detalle_parametros_compras gdpc on gdpc.id_tipo_registro = gtr.id
        where
            gtr.tipo = 2
            and trim(gtr.nombre) = 'cuenta_anticipo_proveedor'
    );

set
    lc_id_cuenta_anticipo_cliente = (
        SELECT
            gdpv.id_cuenta
        FROM
            desarrollo.gen_tipo_registros gtr
            inner join desarrollo.gen_detalle_parametros_ventas gdpv on gdpv.id_tipo_registro = gtr.id
        where
            gtr.tipo = 3
            and trim(gtr.nombre) = 'cuenta_anticipo_clientes'
    );

if (p_tipo_egreso = 2) then
select
    JSON_MERGE_PRESERVE(
        P_JSON,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'id_tercero',
                p_id_tercero,
                'id_fuente',
                lc_id_fuente,
                'id_periodo',
                lc_id_periodo,
                'id_sucursal',
                p_id_sucursal,
                'id_centro_costo',
                coalesce(cc.id, lc_centro_costo),
                'id_cuenta',
                if(
                    p_tipo_egreso = 2,
                    lc_id_cuenta_anticipo_proveedor,
                    lc_id_cuenta_anticipo_cliente
                ),
                'id_usuario',
                p_id_usuario,
                'comprobante',
                lc_comprobante,
                'documento_origen',
                lc_numero_documento,
                'debito',
                dv.valor,
                'credito',
                0.00,
                'fecha',
                p_fecha,
                'detalle',
                concat(
                    'Anticipo a orden de compra No. ',
                    trim(cmo.numero_documento)
                )
            )
        )
    ) into P_JSON
from
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_documento_afectado` int PATH '$.id_documento_afectado' null on empty null on error,
            `valor` decimal(18, 2) PATH '$.valor' null on empty null on error
        )
    ) dv
    inner join desarrollo.cio_maestro_operaciones cmo on cmo.id = dv.id_documento_afectado
    left join (
        select
            distinct id_sucursal,
            id
        from
            desarrollo.con_centros_costo
        where
            por_defecto
    ) cc on cmo.id_sucursal = cc.id_sucursal;

end if;

if (p_tipo_egreso = 4) then
select
    JSON_MERGE_PRESERVE(
        P_JSON,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'id_tercero',
                p_id_tercero,
                'id_fuente',
                lc_id_fuente,
                'id_periodo',
                lc_id_periodo,
                'id_sucursal',
                p_id_sucursal,
                'id_centro_costo',
                coalesce(cc.id, lc_centro_costo),
                'id_cuenta',
                lc_id_cuenta_anticipo_cliente,
                'id_usuario',
                p_id_usuario,
                'comprobante',
                lc_comprobante,
                'documento_origen',
                lc_numero_documento,
                'debito',
                dv.valor,
                'credito',
                0.00,
                'fecha',
                p_fecha,
                'detalle',
                concat(
                    'Cancelamos anticipo entregado por el cliente: ',
                    trim(lc_tercero)
                )
            )
        )
    ) into P_JSON
from
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_documento_afectado` int PATH '$.id_documento_afectado' null on empty null on error,
            `valor` decimal(18, 2) PATH '$.valor' null on empty null on error
        )
    ) dv
    inner join desarrollo.tes_anticipos ta on ta.id = dv.id_documento_afectado
    left join (
        select
            distinct id_sucursal,
            id
        from
            desarrollo.con_centros_costo
        where
            por_defecto
    ) cc on ta.id_sucursal = cc.id_sucursal;

end if;

if(p_tipo_egreso = 3) then
select
    JSON_MERGE_PRESERVE(
        P_JSON,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'id_tercero',
                p_id_tercero,
                'id_fuente',
                lc_id_fuente,
                'id_periodo',
                lc_id_periodo,
                'id_sucursal',
                p_id_sucursal,
                'id_centro_costo',
                coalesce(cc.id, lc_centro_costo),
                'id_cuenta',
                cmo.id_cuenta,
                'id_usuario',
                p_id_usuario,
                'comprobante',
                lc_comprobante,
                'documento_origen',
                lc_numero_documento,
                'debito',
                dv.valor,
                'credito',
                0.00,
                'fecha',
                p_fecha,
                'detalle',
                concat(
                    'Abono a factura de compra No. ',
                    trim(cmo.numero_documento)
                )
            )
        )
    ) into P_JSON
from
    JSON_TABLE(
        p_detalle,
        '$[*]' columns(
            `id_documento_afectado` int PATH '$.id_documento_afectado' null on empty null on error,
            `valor` decimal(18, 2) PATH '$.valor' null on empty null on error
        )
    ) dv
    inner join desarrollo.cio_maestro_operaciones cmo on cmo.id = dv.id_documento_afectado
    left join (
        select
            distinct id_sucursal,
            id
        from
            desarrollo.con_centros_costo
        where
            por_defecto
    ) cc on cmo.id_sucursal = cc.id_sucursal;

end if;

if (p_tipo_egreso in(2, 3, 4)) then
select
    JSON_MERGE_PRESERVE(
        P_JSON,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'id_tercero',
                p_id_tercero,
                'id_fuente',
                lc_id_fuente,
                'id_periodo',
                lc_id_periodo,
                'id_sucursal',
                p_id_sucursal,
                'id_centro_costo',
                lc_centro_costo,
                'id_cuenta',
                p_id_cuenta,
                'id_usuario',
                p_id_usuario,
                'comprobante',
                lc_comprobante,
                'documento_origen',
                lc_numero_documento,
                'debito',
                0.00,
                'credito',
                p_total,
                'fecha',
                p_fecha,
                'detalle',
                concat(
                    'Pago N°: ',
                    lc_numero_documento,
                    ' realizado a: ',
                    trim(lc_tercero)
                )
            )
        )
    ) into P_JSON;

end if;

if (
    NOT isnull(p_detalle_conceptos)
    AND JSON_LENGTH(p_detalle_conceptos) > 0
) then
select
    JSON_MERGE_PRESERVE(
        P_JSON,
        JSON_ARRAYAGG(
            JSON_OBJECT(
                'id_tercero',
                p_id_tercero,
                'id_fuente',
                lc_id_fuente,
                'id_periodo',
                lc_id_periodo,
                'id_sucursal',
                p_id_sucursal,
                'id_centro_costo',
                lc_centro_costo,
                'id_cuenta',
                gc.id_cuenta,
                'id_usuario',
                p_id_usuario,
                'comprobante',
                lc_comprobante,
                'documento_origen',
                lc_numero_documento,
                'debito',
                if(gc.naturaleza = 'D', v.valor, 0.0),
                'credito',
                if(gc.naturaleza = 'C', v.valor, 0.0),
                'fecha',
                p_fecha,
                'detalle',
                concat(
                    'Descuento realizado por concepto ',
                    trim(gc.nombre),
                    ' en pago N°: ',
                    lc_numero_documento,
                    ' realizado a: ',
                    trim(lc_tercero)
                )
            )
        )
    ) into P_JSON
from
    JSON_TABLE(
        p_detalle_conceptos,
        '$[*]' columns(
            `id_concepto` int PATH '$.id_concepto' null on empty null on error,
            `valor` decimal(18, 2) PATH '$.valor' null on empty null on error
        )
    ) v
    inner join desarrollo.gen_conceptos gc on gc.id = v.id_concepto;

end if;

IF (
    (
        select
            sum(coalesce(`debito`, 0.00) - coalesce(`credito`, 0.00))
        from
            json_table(
                P_JSON,
                '$[*]' columns (
                    `debito` decimal(18, 2) path '$.debito' null on empty null on error,
                    `credito` decimal(18, 2) path '$.credito' null on empty null on error
                )
            ) cc
    ) != 0
) then signal sqlstate '45000'
set
    message_text = '[ERROR] Comprobante descuadrado';

END IF;

IF (
    (
        select
            count(*)
        from
            json_table(
                P_JSON,
                '$[*]' columns (
                    `id_cuenta` int path '$.id_cuenta' null on empty null on error
                )
            ) cc
            left join con_cuentas cc2 on cc2.id = cc.id_cuenta
        where
            cc2.isAuxiliar = 0
            or isnull(cc2.id)
    ) != 0
) then signal sqlstate '45000'
set
    message_text = '[ERROR] Cuentas contables no son auxiliar o no existe, revise los parámetros';

END IF;

if(
    (
        select
            count(*)
        from
            json_table(
                P_JSON,
                '$[*]' columns (
                    `id_cuenta` int path '$.id_cuenta' null on empty null on error
                )
            ) cc
    ) = 0
) then signal sqlstate '45000'
set
    message_text = '[ERROR] No se ha generado contabilidad';

end if;

INSERT INTO
    `desarrollo`.`con_movimiento_contable` (
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
        `id_tercero`,
        `comprobante`,
        `documento_origen`,
        `debito`,
        `credito`,
        `fecha`,
        `detalle`,
        `fecsys`,
        `modulo_origen`,
        `estado`
    )
select
    `id_fuente`,
    `id_periodo`,
    `id_sucursal`,
    `id_centro_costo`,
    `id_cuenta`,
    `id_usuario`,
    `id_tercero`,
    `comprobante`,
    `documento_origen`,
    coalesce(`debito`, 0.00),
    coalesce(`credito`, 0.00),
    `fecha`,
    `detalle`,
    now() as `fecsys`,
    'TE' as `modulo_origen`,
    1 as `estado`
from
    json_table(
        P_JSON,
        '$[*]' columns (
            `id_fuente` int path '$.id_fuente' null on empty null on error,
            `id_periodo` int path '$.id_periodo' null on empty null on error,
            `id_sucursal` int path '$.id_sucursal' null on empty null on error,
            `id_centro_costo` int path '$.id_centro_costo' null on empty null on error,
            `id_cuenta` int path '$.id_cuenta' null on empty null on error,
            `id_usuario` int path '$.id_usuario' null on empty null on error,
            `id_tercero` int path '$.id_tercero' null on empty null on error,
            `comprobante` varchar(15) path '$.comprobante' null on empty null on error,
            `documento_origen` varchar(15) path '$.documento_origen' null on empty null on error,
            `debito` decimal(18, 2) path '$.debito' null on empty null on error,
            `credito` decimal(18, 2) path '$.credito' null on empty null on error,
            `fecha` date path '$.fecha' null on empty null on error,
            `detalle` varchar(200) path '$.detalle' null on empty null on error
        )
    ) cc
WHERE
    (
        coalesce(cc.`debito`, 0.00) + coalesce(cc.`credito`, 0.00)
    ) > 0;

select
    p_id as id,
    p_id as id_reg,
    'tes_movimientos' as tabla,
    'EGRESOS' as operacion;

commit;

end if;

end $ $