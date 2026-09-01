CREATE FUNCTION [dbo].[udfGetLastSubmittedWorkflow_Phase](
	@ClientID AS INT,
	@TaxPeriodID INT,
	@EventTypeID INT,
	@EntityID INT ,
	@PhaseID	INT=-1
)
RETURNS INT  
AS  
BEGIN
	DECLARE @WorkflowID INT

	--DECLARE @PhaseID INT
	IF @PhaseID= -1
	BEGIN
		SELECT @PhaseID = dbo.[udfGetPhaseID](@ClientID, @TaxPeriodID)
	END

	SELECT @WorkFlowID= MAX(WF.WorkflowID)
	FROM WorkFlow WF 
		INNER JOIN TransactionLog TL 
		ON TL.TransactionID=WF.TransactionID 
			AND TL.EventTypeID=@EventTypeID	
			AND TL.PhaseID	= WF.PhaseID
	WHERE TL.EntityID=@EntityID 
		AND TL.ClientID=@ClientID 
		AND TL.TaxPeriodID=@TaxPeriodID		
		AND TL.PhaseID=@PhaseID
		AND TL.StatusID NOT IN 
			( 
				SELECT StatusID FROM WorkflowStatus 
				WHERE EnumerationName  = 'Rejected'
			)
		

RETURN @WorkFlowID

END
