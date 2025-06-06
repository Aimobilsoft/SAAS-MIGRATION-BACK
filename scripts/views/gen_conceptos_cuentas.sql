create view gen_conceptos_cuentas as
SELECT
    gc.*,
    cc.codigo as cuenta
FROM
    gen_conceptos gc
    left join con_cuentas cc on cc.id = gc.id_cuenta;