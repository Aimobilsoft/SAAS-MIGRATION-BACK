module.exports = {
    queryTablaFormularios: function() {
        return `
        CREATE TABLE IF NOT EXISTS formularios (
            id int NOT NULL PRIMARY KEY AUTO_INCREMENT,
             nombre varchar(50) NOT NULL,
             form text NOT NULL,
             titulo varchar(50) NOT NULL
         )
         
        `
    },
    querySpGeneradorProcedure: function() {
        return `  SET QUOTED_IDENTIFIER OFF
       
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE name='sp_generador_procedure' and type='P')
        begin
        SET QUOTED_IDENTIFIER OFF

        declare @sql nvarchar(max)
                   set @sql ='create procedure [dbo].[sp_generador_procedure] (@P_NOMBRE_TABLA 	SYSNAME, @P_NOMBRE_PA SYSNAME)
                    as 
                    begin
                    SET QUOTED_IDENTIFIER OFF
                    /*
                    ----------------------------------------------------------------------------------------------------------------------
                            NO TOCAR DE ACÁ PARA ABAJO		NO TOCAR DE ACÁ PARA ABAJO		NO TOCAR DE ACÁ PARA ABAJO
                    ----------------------------------------------------------------------------------------------------------------------
                    */
                    DECLARE @SQL_CABECERA 	VARCHAR(8000)
                    DECLARE @SQL_WHERE 	VARCHAR(8000)
                    DECLARE @SQL_UPDATE	VARCHAR(8000)
                    DECLARE @SQL_INSERT	VARCHAR(8000)
                    DECLARE @SQL_VALUES	VARCHAR(8000)
                    DECLARE @SQL_DELETE	VARCHAR(8000)
                    declare @datos_cabecera varchar(max),
                            @columnasInsert varchar(max),
                            @columnaUpdate varchar(max),
                            @error varchar(90)
                
                    DECLARE @ID INT 
                    SET NOCOUNT ON;
                    IF LEN(@p_nombre_tabla)=0
                    BEGIN
                        raiserror("Error en la carga, no se ha registrado el nombre de la tabla",-1,-1)	 
                    END
                    IF OBJECT_ID(@p_nombre_tabla) IS NULL
                    BEGIN
                        set @error="Tabla no existe ["+@p_nombre_tabla+"]"
                        raiserror(@error,-1,-1)
                    END
                    select @ID = OBJECT_ID(@p_nombre_tabla)
                
                    IF isnull(@P_NOMBRE_PA,space(1)) = SPACE(1)
                        SET @P_NOMBRE_PA = "Sp_"+ lower(@P_NOMBRE_TABLA)
                
                
                    IF EXISTS (SELECT name FROM   sys.objects WHERE  name = @P_NOMBRE_PA  AND 	type = "P")
                    begin
                        declare @sql nvarchar(250) = "DROP PROCEDURE IF EXISTS " + trim(@P_NOMBRE_PA)
                        exec sp_executesql  @sql
                    end;
                
                     select @columnasInsert =  case when sa.is_identity = 1 then @columnasInsert else COALESCE( @columnasInsert + ", ",space(1))+sa.nombre end,
                     @columnaUpdate = case when sa.is_identity = 1 then @columnaUpdate else COALESCE( @columnaUpdate + ", ",space(1))+ sa.nombre+"="+sa.parametro end,
                     @SQL_VALUES = case when sa.is_identity = 1 then @SQL_VALUES else COALESCE( @SQL_VALUES + ", ",space(1))+sa.parametro end,
                     @SQL_WHERE = iif(sa.is_identity=1," where "+sa.nombre+"="+sa.parametro,@SQL_WHERE),
                     @datos_cabecera = COALESCE( @datos_cabecera + ", ",space(1))+sa.parametro+SPACE(1)+sa.tipo_dato+iif(sa.is_identity=1,space(1)," = null ")
                     from (
                     select trim(QUOTENAME(LTRIM(RTRIM(SAC.NAME)))) as nombre    
                         ,CASE              
                          WHEN LTRIM(RTRIM(UPPER(st.name))) IN ("NUMERIC","DECIMAL") THEN TRIM(st.name)+"("+CONVERT(VARCHAR,sac.[precision])+","+CONVERT(VARCHAR,sac.[scale])+")"               
                          WHEN LTRIM(RTRIM(UPPER(st.name))) IN ("CHAR","VARCHAR","NCHAR","NVARCHAR","VARBINARY","BINARY") THEN TRIM(st.name)+"("+case when sac.[max_length]<0 then "max" else CONVERT(VARCHAR,sac.[max_length]) end+")"              
                          WHEN LTRIM(RTRIM(UPPER(st.name))) IN ("DATE","DATETIME2","DATETIME","INT","BIGINT","BIT","MONEY","TEXT","FLOAT","TINYINT","SMALLINT","IMAGE","sysname") THEN UPPER(TRIM(st.name))              
                         END as tipo_dato,  
                         "@"+LTRIM(RTRIM(SAC.NAME)) as parametro,
                         sac.is_identity ,
                         sac.column_id
                         from sys.all_columns sac          
                         inner join sys.types  st on  sac.user_type_id=st.user_type_id                  
                         where  sac.object_id = @id     
                      )sa
                      order by sa.column_id     
                
                      /**********************************************************************
                      *Se inicializan las variables y concatenan con los valores de la tabla *
                      ***********************************************************************/
                
                    SET @SQL_WHERE    = trim(@SQL_WHERE)
                    SET @SQL_INSERT	  = "	INSERT INTO " + @P_NOMBRE_TABLA + "( "+trim(@columnasInsert)+ " )"
                    SET @SQL_VALUES   =  " VALUES ( "+trim(@SQL_VALUES)+" )"
                    SET @SQL_UPDATE   = " UPDATE " + @P_NOMBRE_TABLA + " SET "+trim(@columnaUpdate)+SPACE(1)+trim(@sql_where)
                    set @SQL_CABECERA =  "CREATE PROCEDURE "+ @P_NOMBRE_PA + space(1) + "( "
                    set @SQL_CABECERA = @SQL_CABECERA+space(1)+trim(@datos_cabecera)
                    SET @SQL_DELETE = "	DELETE " + @P_NOMBRE_TABLA + space(1)
                
                    declare @query nvarchar(max)=""
                                
                    set @query = @SQL_CABECERA + ",@accion	VARCHAR(20)) AS IF (@accion = ''guardar'') BEGIN IF NOT EXISTS ( SELECT top 1 * from "+trim(@P_NOMBRE_TABLA)+space(1)+ trim(@SQL_WHERE) + " )"
                    + trim(@SQL_INSERT)+space(1)+ trim(@SQL_VALUES)
                    +" end  IF @accion = ''editar'' begin " +trim(@SQL_UPDATE)+" end IF @accion = ''eliminar'' begin "+trim(@SQL_DELETE)+ space(1)+trim(@SQL_WHERE) +'' end;''
                    select @query = trim(@query)
                
                    /**********************************************************************
                     *					Se ejecuta el T-SQL generado					  *
                     ***********************************************************************/
                    exec sp_executesql @query
                    end;'           
                    exec sp_executesql @sql
        end
        else
        begin
        print 'ya existe'
        end
        `
    },
    ExistSpFormularios: function() {
        return `SELECT * FROM sys.objects WHERE name='sp_formularios' and type='P'`
    },
    querySpFormularios: function() {
        return `
        SET QUOTED_IDENTIFIER OFF

        declare @sql nvarchar(max)
        set @sql ='
		create procedure [dbo].[sp_formularios](@id_formulario int = null, @nombre varchar(50) = null, @tabla varchar(50) = null,@src varchar(100)=null,@form varchar(MAX)=null,@titulo varchar(30)=null, @accion varchar(10)="L")
		as 
		begin
		begin try 
		begin tran
		set quoted_identifier off;

		if (@accion ="G")
        begin
            declare @nameSp varchar(50)=null
            set @nameSp= "Sp_"+ lower(@tabla)
            if not exists (select top 1 * from [dbo].formularios where id =@id_formulario)
            begin
                INSERT INTO formularios(nombre,form,titulo) VALUES (@nombre,@form,@titulo)
                exec [sp_generador_procedure] @P_NOMBRE_TABLA=@tabla, @P_NOMBRE_PA = @nameSp
            end
            else
            begin 
                update formularios set nombre=@nombre, form=@form,titulo=@titulo where id = @id_formulario
                exec [sp_generador_procedure] @P_NOMBRE_TABLA=@tabla, @P_NOMBRE_PA=@nameSp
            end
		end

		COMMIT TRAN 
		END TRY
		BEGIN CATCH 
			DECLARE @ERROR_MENSAJE VARCHAR(MAX)
			SELECT @ERROR_MENSAJE = ERROR_MESSAGE()
			SELECT @ERROR_MENSAJE AS ERROR_MENSAJE
			ROLLBACK TRAN 
			RAISERROR(@ERROR_MENSAJE,16,-1)
		END CATCH 
		enD
		if (@accion ="L") begin

		select id  as id_formulario,nombre,form,titulo from formularios where id =  coalesce(@id_formulario,id)

        end;
        if (@accion ="E") begin

        delete from formularios where id = @id_formulario

        end;
		'

    exec sp_executesql @sql

        `
    },
    querySpTablas: function() {
        return `
        CREATE PROCEDURE sp_tablas (
            name varchar(80),
            accion varchar(15)
        )
        BEGIN
        IF(accion='L') THEN 
            IF(COALESCE(name,"")="") THEN
                SELECT * FROM INFORMATION_SCHEMA.tables WHERE TABLE_SCHEMA=database();
            ELSE 
                SELECT COLUMN_NAME as name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA  LIKE database() AND TABLE_NAME = name;
                    
                set @query  =concat( "select * from ",name) ;
                PREPARE stmt FROM @query;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
                END IF;
            END IF;
        END
        `
    }

}