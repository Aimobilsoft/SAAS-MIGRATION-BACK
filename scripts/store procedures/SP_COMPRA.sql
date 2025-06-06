CREATE DEFINER = `serverapp` @`%` PROCEDURE `sp_compra`(
    `p_id` INT,
    `p_id_bodega` INT,
    `p_id_sede_recibe` INT,
    `p_id_contrato` INT,
    `p_id_requisicion` INT,
    `p_id_tercero` INT,
    `p_id_tercero_fletes` INT,
    `p_fecha` DATETIME,
    `p_numero_remision` bigINT,
    `p_numero_factura` INT,
    `p_numero_factura_flete` INT,
    `p_plazo` INT,
    `p_plazo_flete` INT,
    `p_realiza_orden` TINYINT,
    `p_realiza_entrada` TINYINT,
    `p_contabiliza_factura` TINYINT,
    `p_terceriza_fletes` TINYINT,
    `p_aplica_Ley` TINYINT,
    `p_retencion_base` TINYINT,
    `p_productos` JSON,
    `p_valor_fletes` decimal(18, 2),
    `p_subtotal` decimal(18, 2),
    `p_valor_ajuste_peso` decimal(10, 5),
    `p_total_iva` decimal(18, 2),
    `p_total_orden` decimal(18, 2),
    `p_total_retefuente` decimal(18, 2),
    `p_total_rete_ica` decimal(18, 2),
    `p_total_rete_iva` decimal(18, 2),
    `p_total_descuento` decimal(18, 2),
    `p_total_factura` decimal(18, 2),
    `p_id_usuario` INT,
    `p_accion` varchar(12)
) begin DECLARE lc_numero_entrada int;

DECLARE lc_numero_orden_compra int default 0;

DECLARE lc_numero_factura_compra int;

DECLARE lc_comprobante varchar(12);

DECLARE lc_nombre_tercero varchar(200);

DECLARE lc_centro_costo int;

DECLARE lc_id_periodo int;

DECLARE lc_estado_periodo int;

declare p_id_sucursal int;

DECLARE lc_fecha DATE;

declare lc_id_orden int;

DECLARE EXIT HANDLER FOR SQLEXCEPTION,
SQLWARNING BEGIN GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
@text = MESSAGE_TEXT;

SET
    @full_error = CONCAT("ERROR ", @text);

SELECT
    @full_error as sqlMessage;

ROLLBACK;

END;

IF (p_accion = 'no_remision') THEN
SELECT
    COUNT(numero_remision) AS valid
FROM
    inv_kardex
WHERE
    numero_remision = p_numero_remision
    AND id_tercero = p_id_tercero;

end if;

if (p_accion = 'list_bode') then
SELECT
    gb.*,
    un.`no_ppl`
FROM
    gen_bodegas gb
    LEFT JOIN `prod_unidad_servicios` un ON gb.`id_unidad` = un.`id`
    inner join view_sucursales_usuario vsu on vsu.id_sucursal = gb.id_sucursal
where
    vsu.id_usuario = p_id_usuario;

end if;

IF (p_accion = 'prods_oc') THEN
SELECT
    `p`.`id` AS `id_producto`,
    p.codigo,
    `p`.`referencia` AS `referencia`,
    `p`.`nombre` AS `nombre`,
    `p`.`id_medida` AS `id_medida`,
    `med`.`abreviatura` AS `abreviatura`,
    cio.cantidad AS cantidad_pedida,
    cio.cantidad_facturada,
    cio.cantidad_entregada,
    cio.`cantidad`,
    cio.`costo` AS valor_unitario,
    `ic`.`tasa_iva` AS `porcentaje_iva`,
    cio.`tasa_descuento` AS porcentaje_descuento,
    COALESCE(cio.`cantidad`, 0) * COALESCE(cio.costo, 0) AS valor_total_row,
    COALESCE(cio.`cantidad`, 0) * COALESCE(cio.costo, 0) AS valor_total
FROM
    cio_detalle_operaciones cio
    JOIN `inv_productos` `p` ON p.id = cio.`id_producto`
    JOIN `inv_tipo_producto` `tp` ON `tp`.`id` = `p`.`id_tipo_producto`
    JOIN `inv_interfaz_contable` `ic` ON `ic`.`id` = `p`.`id_interfaz_contable`
    JOIN `inv_medidas` `med` ON `med`.`id` = `p`.`id_medida`
WHERE
    cio.`id_maestro_operacion` = p_id;

END IF;

if (p_accion = 'list_prod') then
select
    `p`.`id` AS `id`,
    `p`.`id_medida` AS `id_medida`,
    `med`.`abreviatura` AS `abreviatura`,
    `p`.`codigo` AS `codigo`,
    `p`.`nombre` AS `nombre`,
    `ic`.`tasa_iva` AS `tasa_iva`,
    `p`.`referencia` AS `referencia`,
    coalesce(
        (
            select
                costo_promedio
            from
                inv_inventario i
            where
                i.id_producto = p.id
            limit
                1
        ), 0.0
    ) as costo
from
    `inv_productos` `p`
    join `inv_tipo_producto` `tp` on `tp`.`id` = `p`.`id_tipo_producto`
    join `inv_interfaz_contable` `ic` on `ic`.`id` = `p`.`id_interfaz_contable`
    join `inv_medidas` `med` on `med`.`id` = `p`.`id_medida`;

end if;

if(p_accion = 'list_ord') then
select
    *
from
    (
        select
            ct.*,
            cmo.id_bodega,
            cmo.numero_documento,
            case
                cmo.estado
                when 1 then true
                when 2 then true
                else false
            end as realiza_entrada,
            if(cmo.estado = 2, true, false) as realiza_factura,
            (
                SELECT
                    JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'id',
                            `vdo`.`id`,
                            'id_producto',
                            vdo.id_producto,
                            'producto',
                            vdo.producto,
                            'medida',
                            vdo.medida,
                            'cantidad',
                            vdo.cantidad,
                            'cantidad_entregada',
                            vdo.cantidad_entregada,
                            'cantidad_facturada',
                            vdo.cantidad_facturada,
                            'tasa_iva',
                            vdo.iva,
                            'porcentaje_descuento',
                            vdo.descuento,
                            'costo',
                            vdo.costo
                        )
                    ) AS `detalle`
                FROM
                    view_detalle_operaciones vdo
                WHERE
                    (vdo.id_operacion = cmo.id)
            ) AS `detalle`
        from
            cio_maestro_operaciones cmo
            inner join view_terceros ct on ct.id_tercero = cmo.id_tercero
            inner join con_fuentes cf on cf.id = cmo.id_tipo_operacion
        where
            cf.codigo = 'OC'
            and cmo.id = coalesce(p_id, cmo.id)
    ) c
where
    c.detalle is not null;

end if;

if(p_accion = 'list_ent') then
select
    *
from
    (
        select
            ct.*,
            cmo.id_bodega,
            cmo.numero_documento,
            case
                cmo.estado
                when 2 then true
                when 3 then true
                else false
            end as realiza_entrada,
            if(cmo.estado = 2, true, false) as realiza_factura,
            (
                SELECT
                    JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'id_detalle',
                            `vdo`.`id`,
                            'id_producto',
                            vdo.id_producto,
                            'producto',
                            vdo.producto,
                            'medida',
                            vdo.medida,
                            'cantidad',
                            vdo.cantidad,
                            'cantidad_entregada',
                            vdo.cantidad_entregada,
                            'cantidad_facturada',
                            vdo.cantidad_facturada,
                            'tasa_iva',
                            vdo.iva,
                            'porcentaje_descuento',
                            vdo.descuento,
                            'costo',
                            vdo.costo
                        )
                    ) AS `detalle`
                FROM
                    view_detalle_operaciones vdo
                WHERE
                    (vdo.id_operacion = cmo.id)
            ) AS `detalle`
        from
            cio_maestro_operaciones cmo
            inner join view_terceros ct on ct.id_tercero = cmo.id_tercero
            inner join con_fuentes cf on cf.id = cmo.id_tipo_operacion
        where
            cf.codigo = 'OC'
            and cmo.id = coalesce(p_id, cmo.id)
            and cmo.estado = 1
            and cmo.id_bodega = coalesce(p_id_bodega, cmo.id_bodega)
    ) c
where
    c.detalle is not null;

end if;

if (p_accion = 'guardar') then start transaction;

set
    lc_fecha = cast(p_fecha as date);

select
    id_sucursal into p_id_sucursal
from
    gen_bodegas
where
    id = p_id_bodega;

set
    p_total_retefuente = coalesce(p_total_retefuente, 0.00);

set
    p_total_rete_ica = coalesce(p_total_rete_ica, 0.00);

set
    p_total_rete_iva = coalesce(p_total_rete_iva, 0.00);

set
    p_valor_ajuste_peso = coalesce(p_valor_ajuste_peso, 0.0);

set
    p_retencion_base = coalesce(p_retencion_base, false);

set
    p_terceriza_fletes = coalesce(p_terceriza_fletes, false);

set
    p_total_descuento = coalesce(p_total_descuento, 0.00);

set
    p_valor_fletes = coalesce(p_valor_fletes, 0.00);

if (p_realiza_orden) then call `sp_consecutivo_fuentes`(
    p_id_sucursal,
    'OC',
    lc_fecha,
    'GL',
    false,
    lc_numero_orden_compra
);

INSERT INTO
    `cio_maestro_operaciones` (
        `id_tercero`,
        id_contrato,
        id_requisicion,
        `id_bodega`,
        id_sede_recibe,
        `id_sucursal`,
        `id_usuario`,
        `id_tipo_operacion`,
        `numero_documento`,
        `documento_contable`,
        `fecha`,
        `fecha_vencimiento`,
        `subtotal`,
        `valor_iva`,
        `valor_retenciones`,
        `valor_descuento`,
        `valor_flete`,
        `ajuste_peso`,
        `total`,
        `fecsys`,
        `estado`,
        credito
    )
VALUES
(
        p_id_tercero,
        p_id_contrato,
        p_id_requisicion,
        p_id_bodega,
        p_id_sede_recibe,
        p_id_sucursal,
        p_id_usuario,
(
            select
                id
            from
                con_fuentes
            where
                codigo = 'OC'
        ),
        lc_numero_orden_compra,
        '',
        lc_fecha,
        lc_fecha,
        p_subtotal,
        coalesce(p_total_iva, 0),
        coalesce(
            (
                p_total_retefuente + p_total_rete_ica + p_total_rete_iva
            ),
            0.0
        ),
        p_total_descuento,
        p_valor_fletes,
        p_valor_ajuste_peso,
        p_total_orden,
        now(),
case
            when (
                p_realiza_entrada
                and coalesce(p_contabiliza_factura, false)
            ) then 3
            when (
                p_realiza_entrada
                and NOT coalesce(p_contabiliza_factura, false)
            ) then 2
            else 1
        end,
        p_total_orden
    );

select
    last_insert_id() into p_id;

SELECT
    lc_numero_orden_compra AS doc,
    p_id AS id_reg,
    p_id AS id,
    'cio_maestro_operaciones' AS tabla,
    'ORDEN_COMPRA' AS operacion;

INSERT INTO
    `cio_detalle_operaciones` (
        `id_maestro_operacion`,
        `id_producto`,
        id_presentacion,
        `id_medida`,
        `cantidad`,
        `cantidad_conversion`,
        `valor`,
        `valor_conversion`,
        `tasa_iva`,
        `tasa_descuento`,
        `costo`,
        `costo_conversion`,
        `valor_fletes`,
        `estado`,
        cantidad_entregada
    )
select
    p_id,
    mv.id_producto,
    mv.id_presentacion,
    mv.id_medida,
    mv.cantidad_pedida,
    0,
    mv.valor_unitario,
    0.0,
    mv.porcentaje_iva,
    mv.porcentaje_descuento,
    mv.valor_unitario,
    0.00,
    coalesce(mv.valor_flete_und, 0),
    1 as estado,
    if(p_realiza_entrada, mv.cantidad_pedida, 0.00) as cantidad_entregada
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            if(
                coalesce(p_contabiliza_factura, false),
                `detalle`.`cantidad_facturada`,
                0.00
            ) AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv;

end if;

if (p_realiza_entrada) then
SET
    SQL_SAFE_UPDATES = 0;

if (
    NOT p_realiza_orden
    and NOT coalesce(p_contabiliza_factura, false)
) then if(ISNULL(p_id)) then signal sqlstate '45000'
set
    message_text = '[ERROR] No reportó el identificador de la orden';

else if(
    select
        count(*)
    from
        json_table(
            p_productos,
            '$[*]' columns (
                `id_detalle` int path '$.id_detalle' default '0' on empty default '0' on error
            )
        ) v
    where
        coalesce(v.id_detalle, 0) = 0
) > 0 then signal sqlstate '45000'
set
    message_text = '[ERROR] No reportó el identificador del detalle de la orden';

end if;

UPDATE
    `cio_detalle_operaciones` cdo,
    (
        select
            detalle.`id_detalle`,
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            detalle.cantidad_pedida
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_detalle` int path '$.id_detalle' default '0' on empty default '0' on error,
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) c
SET
    `cantidad_entregada` = coalesce(`cantidad_entregada`, 0.00) + c.cantidad_pedida
where
    cdo.id = c.id_detalle;

end if;

end if;

SET
    SQL_SAFE_UPDATES = 0;

UPDATE
    `cio_maestro_operaciones`
SET
    `numero_remision` = p_numero_remision
WHERE
    id = p_id;

call `sp_consecutivo_fuentes`(
    p_id_sucursal,
    'EN',
    lc_fecha,
    'GL',
    false,
    lc_numero_entrada
);

update
    `inv_inventario`
    inner join (
        select
            iv.id as id_,
            mv.cantidad_pedida + iv.cantidad as cantidad_inv,
            (
                (
                    mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
                ) + coalesce(mv.valor_flete_und, 0)
            ) * mv.cantidad_pedida as valor_nueva_en,
            (iv.cantidad * iv.`costo_promedio`) as valor_inve,
            (
                (
                    mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
                ) + coalesce(mv.valor_flete_und, 0)
            ) as ultimo_costo_
        from
            (
                select
                    `detalle`.`id_producto` AS `id_producto`,
                    detalle.id_presentacion,
                    `detalle`.`id_medida` AS `id_medida`,
                    `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
                    `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
                    `detalle`.`valor_unitario` AS `valor_unitario`,
                    `detalle`.`valor_fletes` AS `valor_fletes`,
                    `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
                    `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
                    `detalle`.`valor_total` AS `valor_total`,
                    `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
                    `detalle`.`valor_flete_und` AS `valor_flete_und`
                from
                    json_table(
                        p_productos,
                        '$[*]' columns (
                            `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                            `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                            `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                            `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                            `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                            `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                            `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                            `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                            `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                            `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                            `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                            `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                        )
                    ) as `detalle`
            ) mv
            inner join inv_inventario iv on iv.id_producto = mv.id_producto
            and iv.id_und_empaque = mv.id_presentacion
            and iv.id_bodega = p_id_bodega
    ) c on inv_inventario.id = c.id_
set
    `cantidad` = c.cantidad_inv,
    `costo_promedio` = cast(
        ((c.valor_nueva_en + c.valor_inve) / c.cantidad_inv) as decimal (18, 2)
    ),
    `ultimo_costo` = c.ultimo_costo_
where
    id = c.id_;

SET
    SQL_SAFE_UPDATES = 1;

INSERT INTO
    `inv_inventario` (
        `id_bodega`,
        `id_producto`,
        id_und_empaque,
        `cantidad`,
        `costo_promedio`,
        `ultimo_costo`,
        `cantidad_maxima`,
        `cantidad_minima`,
        `estado`
    )
select
    p_id_bodega as id_bodega,
    mv.id_producto as id_producto,
    mv.id_presentacion,
    mv.cantidad_pedida as cantidad,
    (
        (
            mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
        ) + coalesce(mv.valor_flete_und, 0)
    ) as valor_costo,
    (
        (
            mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
        ) + coalesce(mv.valor_flete_und, 0.00)
    ) as ultimo_costo,
    0.00 as cantidad_maxima,
    0.00 as cantidad_minima,
    1 as estado
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    left join inv_inventario iv on iv.id_producto = mv.id_producto
    and iv.id_und_empaque = mv.id_presentacion
    and iv.id_bodega = p_id_bodega
where
    iv.id is null;

insert into
    inv_kardex (
        id_bodega,
        id_producto,
        id_und_empaque,
        id_fuente,
        id_tercero,
        id_usuario,
        numero_documento,
        fecha,
        cantidad,
        valor_costo,
        valor_venta,
        numero_remision,
        numero_compra,
        observacion,
        fecsys
    )
select
    p_id_bodega as id_sucursal,
    mv.id_producto as id_producto,
    mv.id_presentacion,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'EN'
    ) as id_fuente,
    p_id_tercero as id_tercero,
    p_id_usuario as id_usuario,
    coalesce(lc_numero_entrada, 1) as numero_documento,
    lc_fecha as fecha,
    mv.cantidad_pedida as cantidad,
    (
        (
            mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
        ) + coalesce(mv.valor_flete_und, 0)
    ) as valor_costo,
    0.00 as valor_venta,
    p_numero_remision as numero_remision,
    p_id as numero_compra,
    'ENTRADA POR ORDEN DE COMPRA' as observacion,
    now() as fecsys
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv;

end if;

if (p_contabiliza_factura) then if (
    NOT p_realiza_orden
    and NOT coalesce(p_realiza_entrada, false)
) then if(ISNULL(p_id)) then signal sqlstate '45000'
set
    message_text = '[ERROR] No reportó el identificador de la orden';

else if(
    select
        count(*)
    from
        json_table(
            p_productos,
            '$[*]' columns (
                `id_detalle` int path '$.id_detalle' default '0' on empty default '0' on error
            )
        ) v
    where
        coalesce(v.id_detalle, 0) = 0
) > 0 then signal sqlstate '45000'
set
    message_text = '[ERROR] No reportó el identificador del detalle de la orden';

end if;

SET
    SQL_SAFE_UPDATES = 0;

UPDATE
    `cio_detalle_operaciones` cdo,
    (
        select
            detalle.`id_detalle`,
            `detalle`.`id_producto` AS `id_producto`,
            detalle.cantidad_pedida
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_detalle` int path '$.id_detalle' default '0' on empty default '0' on error,
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) c
SET
    `cantidad_facturada` = coalesce(`cantidad_facturada`, 0.00) + c.cantidad_pedida
where
    cdo.id = c.id_detalle;

SET
    SQL_SAFE_UPDATES = 1;

end if;

end if;

select
    nombre_tercero into lc_nombre_tercero
from
    con_terceros
where
    id = p_id_tercero;

if (
    (
        SELECT
            count(*)
        FROM
            con_periodos
        where
            codigo = concat(year(lc_fecha), lpad(month(lc_fecha), 2, '0'))
    ) > 0
) then
SELECT
    id,
    estado into lc_id_periodo,
    lc_estado_periodo
FROM
    con_periodos
where
    codigo = concat(year(lc_fecha), lpad(month(lc_fecha), 2, '0'));

if (lc_estado_periodo = 1) then signal sqlstate '45000'
set
    message_text = '[ERROR] el periodo contable está cerrado';

end if;

else
insert into
    con_periodos (codigo, estado)
values
    (
        concat(year(lc_fecha), lpad(month(lc_fecha), 2, '0')),
        0
    );

select
    last_insert_id() into lc_id_periodo;

end if;

call `sp_consecutivo_fuentes`(
    p_id_sucursal,
    'FP',
    lc_fecha,
    'GL',
    false,
    lc_numero_factura_compra
);

select
    concat(
        year(lc_fecha),
        '-',
        lpad(month(lc_fecha), 2, '0'),
        '-',
        lpad(lc_numero_factura_compra, 4, '0')
    ) into lc_comprobante;

INSERT INTO
    `cio_maestro_operaciones` (
        `id_tercero`,
        `id_cuenta`,
        `id_pedido`,
        `id_sucursal`,
        `id_usuario`,
        `id_tipo_operacion`,
        `numero_documento`,
        `documento_contable`,
        `fecha`,
        `fecha_vencimiento`,
        `subtotal`,
        `valor_iva`,
        `valor_retenciones`,
        `valor_descuento`,
        `valor_flete`,
        `ajuste_peso`,
        `total`,
        `fecsys`,
        `estado`
    )
VALUES
(
        p_id_tercero,
(
            SELECT
                gdpc.id_cuenta
            FROM
                gen_detalle_parametros_compras gdpc
                inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
            where
                gtr.nombre = 'cuenta_proveedor'
        ),
        p_id,
        p_id_sucursal,
        p_id_usuario,
(
            select
                id
            from
                con_fuentes
            where
                codigo = 'FP'
        ),
        p_numero_factura,
        lc_comprobante,
        lc_fecha,
        DATE_ADD(lc_fecha, INTERVAL p_plazo DAY),
        p_subtotal,
        COALESCE(p_total_iva, 0),
(
            p_total_retefuente + p_total_rete_ica + p_total_rete_iva
        ),
        p_total_descuento,
        if(p_terceriza_fletes, 0.00, p_valor_fletes),
        p_valor_ajuste_peso,
        p_total_factura,
        now(),
        1
    );

select
    last_insert_id() into p_id;

INSERT INTO
    `cio_detalle_operaciones` (
        `id_maestro_operacion`,
        `id_producto`,
        id_presentacion,
        `id_medida`,
        `cantidad`,
        `cantidad_conversion`,
        `valor`,
        `valor_conversion`,
        `tasa_iva`,
        `tasa_descuento`,
        `costo`,
        `costo_conversion`,
        `valor_fletes`,
        `estado`
    )
select
    p_id,
    mv.id_producto,
    mv.id_presentacion,
    mv.id_medida,
    mv.cantidad_pedida,
    0,
    mv.valor_unitario,
    0.0,
    mv.porcentaje_iva,
    mv.porcentaje_descuento,
    mv.valor_unitario,
    0.00,
    coalesce(mv.valor_flete_und, 0),
    1 as estado
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv;

if (
    (
        SELECT
            count(*)
        FROM
            `con_centros_costo`
        where
            por_defecto = 1
        limit
            1
    ) > 0
) then
SELECT
    id into lc_centro_costo
FROM
    `con_centros_costo`
where
    por_defecto = 1
limit
    1;

else signal sqlstate '45000'
set
    message_text = '[ERROR] no se ha registrado un centro de costo';

end if;

INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
    idic.`id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    round(
        sum(
            mv.valor_total - if(p_terceriza_fletes, mv.valor_fletes, 0.00)
        ) + if(
            (
                select
                    contabiliza_descuento
                from
                    gen_parametros_compras
            ),
            sum(
                (
                    (mv.valor_unitario * mv.cantidad_facturada) *(mv.porcentaje_descuento * 0.01)
                )
            ),
            0
        ),
        0
    ) as `debito`,
    0.00 as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. Factura de Compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    inner join inv_productos ip on ip.id = mv.id_producto
    inner join inv_detalle_interfaz_contable idic on ip.id_interfaz_contable = idic.id_interfaz_contable
    inner join gen_tipo_registros gtr on gtr.id = idic.id_tipo_registro
where
    gtr.nombre = 'cuenta_compras'
group by
    idic.`id_cuenta`
union
select
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
    idic.`id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    round(
        sum(
            (
                (mv.valor_unitario * mv.cantidad_facturada) -(
                    (mv.valor_unitario * mv.cantidad_facturada) *(mv.porcentaje_descuento * 0.01)
                )
            ) *(mv.porcentaje_iva * 0.01)
        ),
        0
    ) as `debito`,
    0.00 as `credito`,
    now() as `fecha`,
    concat(
        'Vr. iva compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    lc_fecha as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    inner join inv_productos ip on ip.id = mv.id_producto
    inner join inv_detalle_interfaz_contable idic on ip.id_interfaz_contable = idic.id_interfaz_contable
    inner join gen_tipo_registros gtr on gtr.id = idic.id_tipo_registro
where
    gtr.nombre = 'cuenta_iva_compras'
group by
    idic.`id_cuenta`
UNION
select
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_proveedor'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
(
        p_total_factura - if(p_terceriza_fletes, p_valor_fletes, 0)
    ) as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Contabilizamos Obligación por FP No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

if (p_valor_ajuste_peso != 0) then if (p_valor_ajuste_peso > 0) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_ajuste_peso_debito'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    p_valor_ajuste_peso as `debito`,
    0.00 as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Ajuste al peso en Compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

if (p_valor_ajuste_peso < 0) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_ajuste_peso_credito'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    abs(p_valor_ajuste_peso) as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Ajuste al peso CR en Compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

end if;

if (p_terceriza_fletes) then
INSERT INTO
    `cio_maestro_operaciones` (
        `id_tercero`,
        `id_cuenta`,
        `id_sucursal`,
        `id_usuario`,
        `id_tipo_operacion`,
        `numero_documento`,
        `documento_contable`,
        `fecha`,
        `fecha_vencimiento`,
        `subtotal`,
        `valor_iva`,
        `valor_retenciones`,
        `valor_descuento`,
        `valor_flete`,
        `ajuste_peso`,
        `total`,
        `fecsys`,
        `estado`
    )
VALUES
(
        p_id_tercero_fletes,
(
            SELECT
                gdpc.id_cuenta
            FROM
                gen_detalle_parametros_compras gdpc
                inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
            where
                gtr.nombre = 'cuenta_proveedor'
        ),
        p_id_sucursal,
        p_id_usuario,
(
            select
                id
            from
                con_fuentes
            where
                codigo = 'FP'
        ),
        p_numero_factura_flete,
        lc_comprobante,
        lc_fecha,
        DATE_ADD(lc_fecha, INTERVAL p_plazo_flete DAY),
        p_valor_fletes,
        0.00,
        0.00,
        0.00,
        0.0,
        0.00,
        p_valor_fletes,
        now(),
        1
    );

INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero_fletes,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_proveedor'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_valor_fletes as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Contabilizamos Obligación por FP No.: ',
        p_numero_factura_flete
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero_fletes,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
    idic.`id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    sum(mv.valor_fletes) as `debito`,
    0.00 as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Valor de fletes tercerizado N° Fac: ',
        p_numero_factura_flete
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    inner join inv_productos ip on ip.id = mv.id_producto
    inner join inv_detalle_interfaz_contable idic on ip.id_interfaz_contable = idic.id_interfaz_contable
    inner join gen_tipo_registros gtr on gtr.id = idic.id_tipo_registro
where
    gtr.nombre = 'cuenta_compras'
group by
    idic.`id_cuenta`
having
    sum(mv.valor_fletes) > 0;

end if;

if (
    p_total_descuento > 0
    and (
        select
            contabiliza_descuento
        from
            gen_parametros_compras
    )
) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_descuentos'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_total_descuento as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Contabilizamos descuento otorgado en compra No.: ',
        p_numero_factura,
        ' por: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

if (NOT p_retencion_base) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
    idic.`id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    round(sum(mv.valor_retencion_fuente), 0) as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. retencion en la fuente compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    inner join inv_productos ip on ip.id = mv.id_producto
    inner join inv_detalle_interfaz_contable idic on ip.id_interfaz_contable = idic.id_interfaz_contable
    inner join gen_tipo_registros gtr on gtr.id = idic.id_tipo_registro
where
    gtr.nombre = 'cuenta_retencion_compras'
group by
    idic.`id_cuenta`
having
    round(sum(mv.valor_retencion_fuente), 0) > 0;

else
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        select
            id_cuenta
        from
            con_terceros_impuestos cti
            inner join gen_impuestos gi on gi.id = cti.id_impuesto
        where
            gi.id_tipo_impuesto = 2
            and cti.id_tercero = p_id_tercero
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_total_retefuente as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. retencion en la fuente compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

if (p_total_rete_ica > 0) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        select
            id_cuenta
        from
            con_terceros_impuestos cti
            inner join gen_impuestos gi on gi.id = cti.id_impuesto
        where
            gi.id_tipo_impuesto = 3
            and cti.id_tercero = p_id_tercero
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_total_rete_ica as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. retencion ICA en compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

if (p_total_rete_iva > 0) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        select
            id_cuenta
        from
            con_terceros_impuestos cti
            inner join gen_impuestos gi on gi.id = cti.id_impuesto
        where
            gi.id_tipo_impuesto = 4
            and cti.id_tercero = p_id_tercero
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_total_rete_iva as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. retencion IVA en compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

end if;

commit;

end if;

if (p_accion = 'editar') then start transaction;

set
    lc_fecha = cast(p_fecha as date);

select
    id_sucursal into p_id_sucursal
from
    gen_bodegas
where
    id = p_id_bodega;

-- SELECT lc_numero_orden_compra AS doc,10000 AS id_reg, 10000 AS id, 'cio_maestro_operaciones' AS tabla,'ORDEN_COMPRA' AS operacion;
set
    p_total_retefuente = coalesce(p_total_retefuente, 0.00);

set
    p_total_rete_ica = coalesce(p_total_rete_ica, 0.00);

set
    p_total_rete_iva = coalesce(p_total_rete_iva, 0.00);

set
    p_valor_ajuste_peso = coalesce(p_valor_ajuste_peso, 0.0);

set
    p_retencion_base = coalesce(p_retencion_base, false);

set
    p_terceriza_fletes = coalesce(p_terceriza_fletes, false);

set
    p_total_descuento = coalesce(p_total_descuento, 0.00);

set
    p_valor_fletes = coalesce(p_valor_fletes, 0.00);

if (p_realiza_orden) then call `sp_consecutivo_fuentes`(
    p_id_sucursal,
    'OC',
    lc_fecha,
    'GL',
    false,
    lc_numero_orden_compra
);

INSERT INTO
    `cio_maestro_operaciones` (
        `id_tercero`,
        id_contrato,
        id_requisicion,
        `id_bodega`,
        id_sede_recibe,
        `id_sucursal`,
        `id_usuario`,
        `id_tipo_operacion`,
        `numero_documento`,
        `documento_contable`,
        `fecha`,
        `fecha_vencimiento`,
        `subtotal`,
        `valor_iva`,
        `valor_retenciones`,
        `valor_descuento`,
        `valor_flete`,
        `ajuste_peso`,
        `total`,
        `fecsys`,
        `estado`,
        credito
    )
VALUES
(
        p_id_tercero,
        p_id_contrato,
        p_id_requisicion,
        p_id_bodega,
        p_id_sede_recibe,
        p_id_sucursal,
        p_id_usuario,
(
            select
                id
            from
                con_fuentes
            where
                codigo = 'OC'
        ),
        lc_numero_orden_compra,
        '',
        lc_fecha,
        lc_fecha,
        p_subtotal,
        COALESCE(p_total_iva, 0),
        coalesce(
            (
                p_total_retefuente + p_total_rete_ica + p_total_rete_iva
            ),
            0.0
        ),
        p_total_descuento,
        p_valor_fletes,
        p_valor_ajuste_peso,
        p_total_orden,
        now(),
case
            when (
                p_realiza_entrada
                and coalesce(p_contabiliza_factura, false)
            ) then 3
            when (
                p_realiza_entrada
                and NOT coalesce(p_contabiliza_factura, false)
            ) then 2
            else 1
        end,
        p_total_orden
    );

select
    last_insert_id() into p_id;

SELECT
    lc_numero_orden_compra AS doc,
    p_id AS id_reg,
    p_id AS id,
    'cio_maestro_operaciones' AS tabla,
    'ORDEN_COMPRA' AS operacion;

INSERT INTO
    `cio_detalle_operaciones` (
        `id_maestro_operacion`,
        `id_producto`,
        id_presentacion,
        `id_medida`,
        `cantidad`,
        `cantidad_conversion`,
        `valor`,
        `valor_conversion`,
        `tasa_iva`,
        `tasa_descuento`,
        `costo`,
        `costo_conversion`,
        `valor_fletes`,
        `estado`,
        cantidad_entregada
    )
select
    p_id,
    mv.id_producto,
    mv.id_presentacion,
    mv.id_medida,
    mv.cantidad_pedida,
    0,
    mv.valor_unitario,
    0.0,
    mv.porcentaje_iva,
    mv.porcentaje_descuento,
    mv.valor_unitario,
    0.00,
    coalesce(mv.valor_flete_und, 0),
    1 as estado,
    if(p_realiza_entrada, mv.cantidad_pedida, 0.00) as cantidad_entregada
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            if(
                coalesce(p_contabiliza_factura, false),
                `detalle`.`cantidad_facturada`,
                0.00
            ) AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv;

end if;

if (p_realiza_entrada) then
SET
    SQL_SAFE_UPDATES = 0;

if (
    NOT p_realiza_orden
    and NOT coalesce(p_contabiliza_factura, false)
) then if(ISNULL(p_id)) then signal sqlstate '45000'
set
    message_text = '[ERROR] No reportó el identificador de la orden';

else if(
    select
        count(*)
    from
        json_table(
            p_productos,
            '$[*]' columns (
                `id_detalle` int path '$.id_detalle' default '0' on empty default '0' on error
            )
        ) v
    where
        coalesce(v.id_detalle, 0) = 0
) > 0 then signal sqlstate '45000'
set
    message_text = '[ERROR] No reportó el identificador del detalle de la orden';

end if;

UPDATE
    `cio_detalle_operaciones` cdo,
    (
        select
            detalle.`id_detalle`,
            `detalle`.`id_producto` AS `id_producto`,
            detalle.cantidad_pedida
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_detalle` int path '$.id_detalle' default '0' on empty default '0' on error,
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) c
SET
    `cantidad_entregada` = coalesce(`cantidad_entregada`, 0.00) + c.cantidad_pedida
where
    cdo.id = c.id_detalle;

end if;

end if;

SET
    SQL_SAFE_UPDATES = 0;

UPDATE
    `cio_maestro_operaciones`
SET
    `numero_remision` = p_numero_remision
WHERE
    id = p_id;

call `sp_consecutivo_fuentes`(
    p_id_sucursal,
    'EN',
    lc_fecha,
    'GL',
    false,
    lc_numero_entrada
);

update
    `inv_inventario`
    inner join (
        select
            iv.id as id_,
            mv.cantidad_pedida + iv.cantidad as cantidad_inv,
            (
                (
                    mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
                ) + coalesce(mv.valor_flete_und, 0)
            ) * mv.cantidad_pedida as valor_nueva_en,
            (iv.cantidad * iv.`costo_promedio`) as valor_inve,
            (
                (
                    mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
                ) + coalesce(mv.valor_flete_und, 0)
            ) as ultimo_costo_
        from
            (
                select
                    `detalle`.`id_producto` AS `id_producto`,
                    detalle.id_presentacion,
                    `detalle`.`id_medida` AS `id_medida`,
                    `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
                    `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
                    `detalle`.`valor_unitario` AS `valor_unitario`,
                    `detalle`.`valor_fletes` AS `valor_fletes`,
                    `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
                    `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
                    `detalle`.`valor_total` AS `valor_total`,
                    `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
                    `detalle`.`valor_flete_und` AS `valor_flete_und`
                from
                    json_table(
                        p_productos,
                        '$[*]' columns (
                            `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                            `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                            `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                            `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                            `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                            `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                            `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                            `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                            `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                            `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                            `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                            `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                        )
                    ) as `detalle`
            ) mv
            inner join inv_inventario iv on iv.id_producto = mv.id_producto
            and iv.id_und_empaque = mv.id_presentacion
            and iv.id_bodega = p_id_bodega
    ) c on inv_inventario.id = c.id_
set
    `cantidad` = c.cantidad_inv,
    `costo_promedio` = cast(
        ((c.valor_nueva_en + c.valor_inve) / c.cantidad_inv) as decimal (18, 2)
    ),
    `ultimo_costo` = c.ultimo_costo_
where
    id = c.id_;

SET
    SQL_SAFE_UPDATES = 1;

INSERT INTO
    `inv_inventario` (
        `id_bodega`,
        `id_producto`,
        id_und_empaque,
        `cantidad`,
        `costo_promedio`,
        `ultimo_costo`,
        `cantidad_maxima`,
        `cantidad_minima`,
        `estado`
    )
select
    p_id_bodega as id_bodega,
    mv.id_producto as id_producto,
    mv.id_presentacion,
    mv.cantidad_pedida as cantidad,
    (
        (
            mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
        ) + coalesce(mv.valor_flete_und, 0)
    ) as valor_costo,
    (
        (
            mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
        ) + coalesce(mv.valor_flete_und, 0.00)
    ) as ultimo_costo,
    0.00 as cantidad_maxima,
    0.00 as cantidad_minima,
    1 as estado
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    left join inv_inventario iv on iv.id_producto = mv.id_producto
    and iv.id_bodega = p_id_bodega
where
    iv.id is null;

insert into
    inv_kardex (
        id_bodega,
        id_producto,
        id_und_empaque,
        id_fuente,
        id_tercero,
        id_usuario,
        numero_documento,
        fecha,
        cantidad,
        valor_costo,
        valor_venta,
        numero_remision,
        numero_compra,
        observacion,
        fecsys
    )
select
    p_id_bodega as id_sucursal,
    mv.id_producto as id_producto,
    mv.id_presentacion,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'EN'
    ) as id_fuente,
    p_id_tercero as id_tercero,
    p_id_usuario as id_usuario,
    coalesce(lc_numero_entrada, 1) as numero_documento,
    lc_fecha as fecha,
    mv.cantidad_pedida as cantidad,
    (
        (
            mv.valor_unitario -(mv.valor_unitario *(mv.porcentaje_descuento * 0.01))
        ) + coalesce(mv.valor_flete_und, 0)
    ) as valor_costo,
    0.00 as valor_venta,
    p_numero_remision as numero_remision,
    p_id as numero_compra,
    'ENTRADA POR ORDEN DE COMPRA' as observacion,
    now() as fecsys
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv;

end if;

if (p_contabiliza_factura) then if (
    NOT p_realiza_orden
    and NOT coalesce(p_realiza_entrada, false)
) then if(ISNULL(p_id)) then signal sqlstate '45000'
set
    message_text = '[ERROR] No reportó el identificador de la orden';

else if(
    select
        count(*)
    from
        json_table(
            p_productos,
            '$[*]' columns (
                `id_detalle` int path '$.id_detalle' default '0' on empty default '0' on error
            )
        ) v
    where
        coalesce(v.id_detalle, 0) = 0
) > 0 then signal sqlstate '45000'
set
    message_text = '[ERROR] No reportó el identificador del detalle de la orden';

end if;

SET
    SQL_SAFE_UPDATES = 0;

UPDATE
    `cio_detalle_operaciones` cdo,
    (
        select
            detalle.`id_detalle`,
            `detalle`.`id_producto` AS `id_producto`,
            detalle.id_presentacion,
            detalle.cantidad_pedida,
            `detalle`.`cantidad_facturada` AS `cant_facturada`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_detalle` int path '$.id_detalle' default '0' on empty default '0' on error,
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_presentacion` INT path '$.id_presentacion' DEFAULT '0' ON empty DEFAULT '0' ON error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) c
SET
    `cantidad_facturada` = c.`cant_facturada` + `cantidad_facturada`
where
    cdo.id_maestro_operacion = `p_id`
    and cdo.id_producto = c.`id_producto`
    AND cdo.id_presentacion = c.`id_presentacion`;

SET
    SQL_SAFE_UPDATES = 1;

end if;

end if;

select
    nombre_tercero into lc_nombre_tercero
from
    con_terceros
where
    id = p_id_tercero;

if (
    (
        SELECT
            count(*)
        FROM
            con_periodos
        where
            codigo = concat(year(lc_fecha), lpad(month(lc_fecha), 2, '0'))
    ) > 0
) then
SELECT
    id,
    estado into lc_id_periodo,
    lc_estado_periodo
FROM
    con_periodos
where
    codigo = concat(year(lc_fecha), lpad(month(lc_fecha), 2, '0'));

if (lc_estado_periodo = 1) then signal sqlstate '45000'
set
    message_text = '[ERROR] el periodo contable está cerrado';

end if;

else
insert into
    con_periodos (codigo, estado)
values
    (
        concat(year(lc_fecha), lpad(month(lc_fecha), 2, '0')),
        0
    );

select
    last_insert_id() into lc_id_periodo;

end if;

call `sp_consecutivo_fuentes`(
    p_id_sucursal,
    'FP',
    lc_fecha,
    'GL',
    false,
    lc_numero_factura_compra
);

select
    concat(
        year(lc_fecha),
        '-',
        lpad(month(lc_fecha), 2, '0'),
        '-',
        lpad(lc_numero_factura_compra, 4, '0')
    ) into lc_comprobante;

INSERT INTO
    `cio_maestro_operaciones` (
        `id_tercero`,
        `id_cuenta`,
        `id_pedido`,
        `id_sucursal`,
        `id_usuario`,
        `id_tipo_operacion`,
        `numero_documento`,
        `documento_contable`,
        `fecha`,
        `fecha_vencimiento`,
        `subtotal`,
        `valor_iva`,
        `valor_retenciones`,
        `valor_descuento`,
        `valor_flete`,
        `ajuste_peso`,
        `total`,
        `fecsys`,
        `estado`
    )
VALUES
(
        p_id_tercero,
(
            SELECT
                gdpc.id_cuenta
            FROM
                gen_detalle_parametros_compras gdpc
                inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
            where
                gtr.nombre = 'cuenta_proveedor'
        ),
        p_id,
        p_id_sucursal,
        p_id_usuario,
(
            select
                id
            from
                con_fuentes
            where
                codigo = 'FP'
        ),
        p_numero_factura,
        lc_comprobante,
        lc_fecha,
        DATE_ADD(lc_fecha, INTERVAL p_plazo DAY),
        p_subtotal,
        COALESCE(p_total_iva, 0),
(
            p_total_retefuente + p_total_rete_ica + p_total_rete_iva
        ),
        p_total_descuento,
        if(p_terceriza_fletes, 0.00, p_valor_fletes),
        p_valor_ajuste_peso,
        p_total_factura,
        now(),
        1
    );

select
    last_insert_id() into p_id;

if (
    (
        SELECT
            count(*)
        FROM
            `con_centros_costo`
        where
            por_defecto = 1
        limit
            1
    ) > 0
) then
SELECT
    id into lc_centro_costo
FROM
    `con_centros_costo`
where
    por_defecto = 1
limit
    1;

else signal sqlstate '45000'
set
    message_text = '[ERROR] no se ha registrado un centro de costo';

end if;

INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
    idic.`id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    round(
        sum(
            mv.valor_total - if(p_terceriza_fletes, mv.valor_fletes, 0.00)
        ) + if(
            (
                select
                    contabiliza_descuento
                from
                    gen_parametros_compras
            ),
            sum(
                (
                    (mv.valor_unitario * mv.cantidad_facturada) *(mv.porcentaje_descuento * 0.01)
                )
            ),
            0
        ),
        0
    ) as `debito`,
    0.00 as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. Factura de Compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    inner join inv_productos ip on ip.id = mv.id_producto
    inner join inv_detalle_interfaz_contable idic on ip.id_interfaz_contable = idic.id_interfaz_contable
    inner join gen_tipo_registros gtr on gtr.id = idic.id_tipo_registro
where
    gtr.nombre = 'cuenta_compras'
group by
    idic.`id_cuenta`
union
select
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
    idic.`id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    round(
        sum(
            (
                (mv.valor_unitario * mv.cantidad_facturada) -(
                    (mv.valor_unitario * mv.cantidad_facturada) *(mv.porcentaje_descuento * 0.01)
                )
            ) *(mv.porcentaje_iva * 0.01)
        ),
        0
    ) as `debito`,
    0.00 as `credito`,
    now() as `fecha`,
    concat(
        'Vr. iva compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    lc_fecha as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    inner join inv_productos ip on ip.id = mv.id_producto
    inner join inv_detalle_interfaz_contable idic on ip.id_interfaz_contable = idic.id_interfaz_contable
    inner join gen_tipo_registros gtr on gtr.id = idic.id_tipo_registro
where
    gtr.nombre = 'cuenta_iva_compras'
group by
    idic.`id_cuenta`
UNION
select
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_proveedor'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
(
        p_total_factura - if(p_terceriza_fletes, p_valor_fletes, 0)
    ) as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Contabilizamos Obligación por FP No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

if (p_valor_ajuste_peso != 0) then if (p_valor_ajuste_peso > 0) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_ajuste_peso_debito'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    p_valor_ajuste_peso as `debito`,
    0.00 as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Ajuste al peso en Compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

if (p_valor_ajuste_peso < 0) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_ajuste_peso_credito'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    abs(p_valor_ajuste_peso) as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Ajuste al peso CR en Compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

end if;

if (p_terceriza_fletes) then
INSERT INTO
    `cio_maestro_operaciones` (
        `id_tercero`,
        `id_cuenta`,
        `id_sucursal`,
        `id_usuario`,
        `id_tipo_operacion`,
        `numero_documento`,
        `documento_contable`,
        `fecha`,
        `fecha_vencimiento`,
        `subtotal`,
        `valor_iva`,
        `valor_retenciones`,
        `valor_descuento`,
        `valor_flete`,
        `ajuste_peso`,
        `total`,
        `fecsys`,
        `estado`
    )
VALUES
(
        p_id_tercero_fletes,
(
            SELECT
                gdpc.id_cuenta
            FROM
                gen_detalle_parametros_compras gdpc
                inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
            where
                gtr.nombre = 'cuenta_proveedor'
        ),
        p_id_sucursal,
        p_id_usuario,
(
            select
                id
            from
                con_fuentes
            where
                codigo = 'FP'
        ),
        p_numero_factura_flete,
        lc_comprobante,
        lc_fecha,
        DATE_ADD(lc_fecha, INTERVAL p_plazo_flete DAY),
        p_valor_fletes,
        0.00,
        0.00,
        0.00,
        0.0,
        0.00,
        p_valor_fletes,
        now(),
        1
    );

INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero_fletes,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_proveedor'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_valor_fletes as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Contabilizamos Obligación por FP No.: ',
        p_numero_factura_flete
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero_fletes,
    (
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
    idic.`id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    sum(mv.valor_fletes) as `debito`,
    0.00 as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Valor de fletes tercerizado N° Fac: ',
        p_numero_factura_flete
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    inner join inv_productos ip on ip.id = mv.id_producto
    inner join inv_detalle_interfaz_contable idic on ip.id_interfaz_contable = idic.id_interfaz_contable
    inner join gen_tipo_registros gtr on gtr.id = idic.id_tipo_registro
where
    gtr.nombre = 'cuenta_compras'
group by
    idic.`id_cuenta`
having
    sum(mv.valor_fletes) > 0;

end if;

if (
    p_total_descuento > 0
    and (
        select
            contabiliza_descuento
        from
            gen_parametros_compras
    )
) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        SELECT
            gdpc.id_cuenta
        FROM
            gen_detalle_parametros_compras gdpc
            inner join gen_tipo_registros gtr on gtr.id = gdpc.id_tipo_registro
        where
            gtr.nombre = 'cuenta_descuentos'
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_total_descuento as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Contabilizamos descuento otorgado en compra No.: ',
        p_numero_factura,
        ' por: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

if (NOT p_retencion_base) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
    idic.`id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    round(sum(mv.valor_retencion_fuente), 0) as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. retencion en la fuente compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`
from
    (
        select
            `detalle`.`id_producto` AS `id_producto`,
            `detalle`.`id_medida` AS `id_medida`,
            `detalle`.`cantidad_pedida` AS `cantidad_pedida`,
            `detalle`.`cantidad_facturada` AS `cantidad_facturada`,
            `detalle`.`valor_unitario` AS `valor_unitario`,
            `detalle`.`valor_fletes` AS `valor_fletes`,
            `detalle`.`porcentaje_iva` AS `porcentaje_iva`,
            `detalle`.`porcentaje_descuento` AS `porcentaje_descuento`,
            `detalle`.`valor_total` AS `valor_total`,
            `detalle`.`valor_retencion_fuente` AS `valor_retencion_fuente`,
            `detalle`.`valor_flete_und` AS `valor_flete_und`
        from
            json_table(
                p_productos,
                '$[*]' columns (
                    `id_producto` int path '$.id_producto' default '0' on empty default '0' on error,
                    `id_medida` int path '$.id_medida' default '0' on empty default '0' on error,
                    `cantidad_pedida` decimal(18, 2) path '$.cantidad_pedida' default '0' on empty default '0' on error,
                    `cantidad_facturada` decimal(18, 2) path '$.cantidad_facturada' default '0' on empty default '0' on error,
                    `valor_unitario` decimal(18, 2) path '$.valor_unitario' default '0' on empty default '0' on error,
                    `valor_fletes` decimal(18, 2) path '$.valor_fletes' default '0' on empty default '0' on error,
                    `porcentaje_iva` decimal(8, 5) path '$.porcentaje_iva' default '0' on empty default '0' on error,
                    `porcentaje_descuento` decimal(8, 5) path '$.porcentaje_descuento' default '0' on empty default '0' on error,
                    `valor_total` decimal(18, 2) path '$.valor_total' default '0' on empty default '0' on error,
                    `valor_retencion_fuente` decimal(18, 2) path '$.valor_retencion_fuente' default '0' on empty default '0' on error,
                    `valor_flete_und` decimal(18, 2) path '$.valor_flete_und' default '0' on empty default '0' on error
                )
            ) as `detalle`
    ) mv
    inner join inv_productos ip on ip.id = mv.id_producto
    inner join inv_detalle_interfaz_contable idic on ip.id_interfaz_contable = idic.id_interfaz_contable
    inner join gen_tipo_registros gtr on gtr.id = idic.id_tipo_registro
where
    gtr.nombre = 'cuenta_retencion_compras'
group by
    idic.`id_cuenta`
having
    round(sum(mv.valor_retencion_fuente), 0) > 0;

else
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        select
            id_cuenta
        from
            con_terceros_impuestos cti
            inner join gen_impuestos gi on gi.id = cti.id_impuesto
        where
            gi.id_tipo_impuesto = 2
            and cti.id_tercero = p_id_tercero
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_total_retefuente as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. retencion en la fuente compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

if (p_total_rete_ica > 0) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        select
            id_cuenta
        from
            con_terceros_impuestos cti
            inner join gen_impuestos gi on gi.id = cti.id_impuesto
        where
            gi.id_tipo_impuesto = 3
            and cti.id_tercero = p_id_tercero
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_total_rete_ica as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. retencion ICA en compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

if (p_total_rete_iva > 0) then
INSERT INTO
    `con_movimiento_contable` (
        `id_tercero`,
        `id_fuente`,
        `id_periodo`,
        `id_sucursal`,
        `id_centro_costo`,
        `id_cuenta`,
        `id_usuario`,
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
    p_id_tercero,
(
        select
            id
        from
            con_fuentes
        where
            codigo = 'FP'
    ) as `id_fuente`,
    lc_id_periodo as `id_periodo`,
    p_id_sucursal as `id_sucursal`,
    lc_centro_costo as `id_centro_costo`,
(
        select
            id_cuenta
        from
            con_terceros_impuestos cti
            inner join gen_impuestos gi on gi.id = cti.id_impuesto
        where
            gi.id_tipo_impuesto = 4
            and cti.id_tercero = p_id_tercero
    ) as `id_cuenta`,
    p_id_usuario as `id_usuario`,
    lc_comprobante as `comprobante`,
    p_numero_factura as `documento_origen`,
    0.00 as `debito`,
    p_total_rete_iva as `credito`,
    lc_fecha as `fecha`,
    concat(
        'Vr. retencion IVA en compra No.: ',
        p_numero_factura,
        ' A: ',
        UPPER(lc_nombre_tercero)
    ) as `detalle`,
    now() as `fecsys`,
    'CP' as `modulo_origen`,
    1 as `estado`;

end if;

end if;

commit;

end if;

end