ALTER TABLE `desarrollo`.`prod_zonas_detalle_contratos` ADD UNIQUE INDEX `zona_id_unidad_Unique` (`id_zona`, `id_unidad_servicio`); 

ALTER TABLE `desarrollo`.`con_terceros` 
ADD COLUMN `id_cuenta_retencion` INT NULL AFTER `estado`,
ADD COLUMN `retencion_procentaje` INT NULL AFTER `id_cuenta_retencion`,
ADD COLUMN `base_retencion` VARCHAR(45) NULL AFTER `retencion_procentaje`,
ADD COLUMN `id_cuenta_reteica` INT NULL AFTER `base_retencion`,
ADD COLUMN `reteica_porcentaje` INT NULL AFTER `id_cuenta_reteica`;
