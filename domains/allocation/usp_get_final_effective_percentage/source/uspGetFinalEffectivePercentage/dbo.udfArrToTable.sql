CREATE FUNCTION [dbo].[udfArrToTable] ( @StringInput VARCHAR(MAX) )
RETURNS @OutputTable TABLE ( [id] INT IDENTITY(1,1) , [Value] Varchar(MAX))
AS
BEGIN
    DECLARE @String VARCHAR(MAX)

    WHILE LEN(@StringInput) > 0
    BEGIN
        SET @String = LEFT(@StringInput, 
                                ISNULL(NULLIF(CHARINDEX(',', @StringInput) - 1, -1),
                                LEN(@StringInput)))
        SET @StringInput = SUBSTRING(@StringInput,
                                     ISNULL(NULLIF(CHARINDEX(',', @StringInput), 0),
                                     LEN(@StringInput)) + 1, LEN(@StringInput))

        INSERT INTO @OutputTable ( [Value] )
        VALUES ( CAST(@String AS Varchar(MAX)) )
    END
    
    RETURN
END
