
CREATE FUNCTION [dbo].[udf_PE_GetPartnerImportEventID](
	@ClientID INT,
	@TaxPeriodID INT
)
RETURNS INT  
AS  
BEGIN

	DECLARE @PartnerImportEventID INT, @PartnerImportType VARCHAR(50)

	SET @PartnerImportEventID=0
	
	SELECT @PartnerImportType = M.MenuName
	FROM GlobalMenu M
	INNER JOIN ENU_GlobalMenuGroup GM
	ON M.GlobalMenuGroupID = GM.GlobalMenuGroupID
	AND GM.GroupName = 'Partner Import Methodology'
	WHERE M.[State] = 'C'
	AND M.ClientID = @ClientID
	AND M.TaxPeriodID = @TaxPeriodID

	IF @PartnerImportType = 'Master Import'
	BEGIN
		SELECT @PartnerImportEventID = EventTypeID
		FROM ENU_Event
		WHERE EventName = 'MasterImport_Partner'
	END
	ELSE 
		SELECT @PartnerImportEventID = EventTypeID
		FROM ENU_Event
		WHERE EventName = 'Import_Partner'
	
	

	RETURN @PartnerImportEventID
END
