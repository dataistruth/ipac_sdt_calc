CREATE FUNCTION [dbo].[udfGetPhaseID]
(@ClientId INT
,@TaxperiodId INT
)
RETURNS INT
AS
BEGIN

	DECLARE @PhaseID INT 
	-- FinalizeDate is Null means it is the 
    -- current phase for  ClientID/TaxperiodID.
	SELECT @PhaseID = PhaseID FROM Phase
	WHERE CLIENTID = @ClientId
	AND TAXPERIODID = @TaxperiodId
	AND EndDate IS NULL

   RETURN @PhaseID


END
