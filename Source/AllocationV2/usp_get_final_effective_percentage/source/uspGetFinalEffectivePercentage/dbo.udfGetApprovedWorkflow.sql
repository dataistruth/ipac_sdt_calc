
CREATE FUNCTION [dbo].[udfGetApprovedWorkflow](
	@ClientID AS INT,
	@TaxPeriodID INT,
	@EventTypeID INT,
	@EntityID INT 
)
RETURNS INT  
AS  
BEGIN
/*-------------------------------------------------------------
Author		Date		Comments
Pratheepa R	02/15/2012	Modified to get any workflow which has been approved over includeincalc 
						No need to check for workflowstep & role - TFS 35423
Saptarshi/Kirti 01/26/2023 ADO 268661/268952 Optimization fix
Meera 02/28/2023 ADO 272403: KKR | Import | Not able to wipe out Allocation Data import
Anubhav	10/26/2023	ADO 299683: Excludig Non Critical errors for adjustment event
--------------------------------------------------------------*/

	DECLARE @WorkFlowID INT
	DECLARE @IncludeInCalcStep SMALLINT
	DECLARE @Status AS TABLE (StatusID INT)  
	DECLARE @AdjustmentsEventTypeID INT  
	SELECT @AdjustmentsEventTypeID = EventTypeID FROM ENU_Event WHERE EventName = 'Adjustments';   
	 
	IF(@EventTypeID=@AdjustmentsEventTypeID)  
	BEGIN  
	INSERT INTO @Status(StatusID)  
	SELECT StatusID FROM WORKFLOWSTATUS WHERE EnumerationName IN ('Rejected', 'Err_Critical')  
	END  
	ELSE   
	BEGIN  
	INSERT INTO @Status(StatusID)  
	SELECT StatusID FROM WORKFLOWSTATUS WHERE EnumerationName IN ('Rejected', 'Err_Critical','Err_NonCritical')  
	END  
 
	
	/* Revised the logic based on updated specs, if include in calc is in step 1 and it's approved by step 2, still get it */
	DECLARE @PhaseID INT
	SELECT @PhaseID = dbo.[udfGetPhaseID](@ClientID, @TaxPeriodID)

	SELECT @IncludeInCalcStep = WorkflowStatusID
	FROM WorkFlowChain (NOLOCK)
	WHERE clientid=@ClientID 
		AND TaxPeriodID=@TaxPeriodID
		AND IncludeinCalc=1
		IF @EntityID <> -1
		BEGIN

	SELECT @WorkFlowID= ISNULL( MAX(WF.WorkflowID),0)
	FROM WorkFlow WF (NOLOCK)
			JOIN TransactionLog TL (NOLOCK)
					ON TL.TransactionID=WF.TransactionID 
					AND TL.EventTypeID=@EventTypeID	
					AND TL.PhaseID = WF.PhaseID
				WHERE TL.EntityID=@EntityID 
		AND TL.ClientID=@ClientID 
		AND TL.TaxPeriodID=@TaxPeriodID		
		AND TL.StatusID >= @IncludeInCalcStep		
		AND TL.PhaseID=@PhaseID     
		AND TL.StatusID NOT IN (SELECT StatusID FROM @Status)
		END
		ELSE 
		BEGIN
			SELECT @WorkFlowID= ISNULL( MAX(WF.WorkflowID),0)
	FROM WorkFlow WF (NOLOCK)
			JOIN TransactionLog TL 
					ON TL.TransactionID=WF.TransactionID 
					AND TL.EventTypeID=@EventTypeID	
					AND TL.PhaseID = WF.PhaseID
		WHERE TL.ClientID=@ClientID 
		AND TL.TaxPeriodID=@TaxPeriodID		
		AND TL.StatusID >= @IncludeInCalcStep		
		AND TL.PhaseID=@PhaseID
		AND TL.StatusID NOT IN (SELECT StatusID FROM @Status)
		END
  
RETURN @WorkFlowID

END


