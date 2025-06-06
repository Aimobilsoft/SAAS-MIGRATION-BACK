CREATE DEFINER = `serverapp` @`%` PROCEDURE `sp_consecutivo_fuentes`(
    `p_id_sucursal` INT,
    `p_codigo_fuente` varchar(2),
    `p_fecha` DATE,
    `p_accion` varchar(2),
    `p_transac` TINYINT,
    OUT `p_consec` INT
) begin declare lc_tipo_fuente varchar(2);

declare lc_id_consecutivo int;

declare lc_id_fuente int;

declare lc_sql varchar(2000);

DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE,
@text = MESSAGE_TEXT;

SET
    @full_error = CONCAT("ERROR ", @text);

if p_transac then
select
    @full_error as sqlMessage;

rollback;

end if;

signal sqlstate '45000'
set
    message_text = @full_error;

END;

if p_transac then start transaction;

end if;

if (
    (
        select
            count(*)
        from
            con_fuentes cf
            inner join con_tipos_fuente ctf on cf.id_tipo_fuente = ctf.id
        where
            cf.codigo = p_codigo_fuente
    ) > 0
) then
select
    coalesce(ctf.codigo, ''),
    coalesce(cf.id, 0) into lc_tipo_fuente,
    lc_id_fuente
from
    con_fuentes cf
    inner join con_tipos_fuente ctf on cf.id_tipo_fuente = ctf.id
where
    cf.codigo = p_codigo_fuente;

else
set
    lc_id_fuente = null;

end if;

if (isnull(lc_id_fuente)) then signal sqlstate '45001'
set
    message_text = ' la fuente no se encuentra';

end if;

if (
    (
        select
            count(*)
        from
            gen_consecutivos
        where
            id_sucursal = p_id_sucursal
            and id_fuente = lc_id_fuente
    ) > 0
) then if(lc_tipo_fuente = 'M') then
set
    lc_id_consecutivo = (
        select
            id
        from
            gen_consecutivos
        where
            id_fuente = lc_id_fuente
            and id_sucursal = p_id_sucursal
            and anio = year(p_fecha)
            and mes = month(p_fecha)
    );

end if;

if(lc_tipo_fuente = 'U') then
select
    id into lc_id_consecutivo
from
    gen_consecutivos
where
    id_fuente = lc_id_fuente
    and id_sucursal = p_id_sucursal;

end if;

if(lc_tipo_fuente = 'A') then
set
    lc_id_consecutivo = (
        select
            id
        from
            gen_consecutivos
        where
            id_fuente = lc_id_fuente
            and id_sucursal = p_id_sucursal
            and anio = year(p_fecha)
    );

end if;

else
set
    lc_id_consecutivo = null;

end if;

if (isnull(lc_id_consecutivo)) then
insert into
    gen_consecutivos(
        id_sucursal,
        id_fuente,
        anio,
        mes,
        consecutivo,
        estado
    )
values
    (
        p_id_sucursal,
        lc_id_fuente,
        if(
            lc_tipo_fuente = 'M'
            or lc_tipo_fuente = 'A',
            year(p_fecha),
            null
        ),
        if(lc_tipo_fuente = 'M', MONTH(p_fecha), null),
        0,
        1
    );

select
    last_insert_id() into lc_id_consecutivo;

end if;

if (
    p_accion = 'LI'
    or p_accion = 'L'
) then
set
    @_fecha = p_fecha;

select
    CONCAT(
        'select ',
case
            p_accion
            when 'LI' then 'coalesce(consecutivo,0)+1'
            when 'L' then 'coalesce(consecutivo,1)'
        end,
        ' as consecutivo from gen_consecutivos where id = ',
        lc_id_consecutivo
    ) into lc_sql;

set
    @sql_command = lc_sql;

PREPARE stmt1
FROM
    @sql_command;

EXECUTE stmt1;

DEALLOCATE PREPARE stmt1;

end if;

if (p_accion = 'GL') then
update
    gen_consecutivos
set
    consecutivo = consecutivo + 1
where
    id = lc_id_consecutivo;

select
    coalesce(consecutivo, 1) into p_consec
from
    gen_consecutivos
where
    id = lc_id_consecutivo;

end if;

if (p_accion = 'A') then
update
    gen_consecutivos
set
    consecutivo = coalesce(consecutivo, 0) + 1
where
    id = lc_id_consecutivo;

end if;

if p_transac then commit;

end if;

end