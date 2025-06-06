USE `desarrollo`;
DROP procedure IF EXISTS `Sp_con_terceros`;

USE `desarrollo`;
DROP procedure IF EXISTS `desarrollo`.`Sp_con_terceros`;
;

DELIMITER $$
USE `desarrollo`$$
CREATE DEFINER=`develop`@`%` PROCEDURE `Sp_con_terceros`( `p_id` INT,`p_id_municipio` INT,`p_id_actividad_ciiu` INT,`p_id_tipo_regimen` INT,`p_id_tipo_documento` INT,`p_id_lista_precio` INT,`p_id_tipo_tercero` INT,`p_id_zona` INT,`p_id_tipo_forma_pago` INT,`p_documento` varchar(15),`p_digito` varchar(1),`p_nombre_tercero` varchar(200),`p_primer_nombre` varchar(50),`p_segundo_nombre` varchar(50),`p_primer_apellido` varchar(50),`p_segundo_apellido` varchar(50),`p_direccion` varchar(70),`p_telefono` varchar(20),`p_celular` varchar(20),`p_email` varchar(60),`p_cupo_credito` decimal(18,2),`p_plazo` INT,`p_estado` INT,
p_id_cuenta_retencion int , p_retencion_procentaje int , p_base_retencion varchar(45), p_id_cuenta_reteica int, p_reteica_porcentaje int,`p_accion` varchar(20) )
begin declare in_conteo int;
 IF (p_accion = 'guardar') then 
	 select count(*) into in_conteo from con_terceros where id=p_id;
	 IF in_conteo = 0 then 
		 INSERT INTO con_terceros( id_municipio,id_actividad_ciiu,id_tipo_regimen,id_tipo_documento,id_lista_precio,id_tipo_tercero,id_zona,id_tipo_forma_pago,documento,
		 digito,nombre_tercero,primer_nombre,segundo_nombre,primer_apellido,segundo_apellido,direccion,telefono,celular,email,cupo_credito,plazo,estado,id_cuenta_retencion, retencion_procentaje, base_retencion, id_cuenta_reteica, reteica_porcentaje ) 
		 VALUES ( p_id_municipio,p_id_actividad_ciiu,p_id_tipo_regimen,p_id_tipo_documento,p_id_lista_precio,p_id_tipo_tercero,p_id_zona,p_id_tipo_forma_pago,p_documento,
		 p_digito,p_nombre_tercero,coalesce(p_primer_nombre,''),coalesce(p_segundo_nombre,''),coalesce(p_primer_apellido,''),coalesce(p_segundo_apellido,''),p_direccion,p_telefono,p_celular,p_email,p_cupo_credito,p_plazo,p_estado, p_id_cuenta_retencion, p_retencion_procentaje, p_base_retencion, p_id_cuenta_reteica, p_reteica_porcentaje); 
	 end if;
 end if;  
 IF p_accion = 'editar' then
	 UPDATE con_terceros 
		SET 
			id_municipio=p_id_municipio,
            id_actividad_ciiu=p_id_actividad_ciiu,
            id_tipo_regimen=p_id_tipo_regimen,
            id_tipo_documento=p_id_tipo_documento,
            id_lista_precio=p_id_lista_precio,
            id_tipo_tercero=p_id_tipo_tercero,
            id_zona=p_id_zona,
            id_tipo_forma_pago=p_id_tipo_forma_pago,
            documento=p_documento,
            digito=p_digito,
            nombre_tercero=p_nombre_tercero,
            primer_nombre=coalesce(p_primer_nombre,''),
            segundo_nombre=coalesce(p_segundo_nombre,''),
            primer_apellido=coalesce(p_primer_apellido,''),
            segundo_apellido=coalesce(p_segundo_apellido,''),
            direccion=p_direccion,
            telefono=p_telefono,
            celular=p_celular,
            email=p_email,
            cupo_credito=p_cupo_credito,
            plazo=p_plazo,
            estado=p_estado,
            id_cuenta_retencion=p_id_cuenta_retencion,
            retencion_procentaje=p_retencion_procentaje,
            base_retencion=p_base_retencion,
            id_cuenta_reteica=p_id_cuenta_reteica,
            reteica_porcentaje=p_reteica_porcentaje
	 where id=p_id; 
 end if; 
 IF p_accion = 'eliminar' then
	DELETE from con_terceros where id=p_id; 
 end if; 
 IF p_accion = 'valid_documento' THEN
	select count(*) total from `con_terceros` where `documento`= p_documento;
 END IF; 
 
 
 end$$

DELIMITER ;
;

