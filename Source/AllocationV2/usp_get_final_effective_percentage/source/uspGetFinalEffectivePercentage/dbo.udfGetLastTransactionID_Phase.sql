

CREATE FUNCTION [dbo].[udfGetLastTransactionID_Phase](
	@ClientID AS INT,
	@TaxPeriodID INT,
	@IncludeFailed BIT,
	@EventTypeID INT,
	@EntityID INT = -1,
	@PhaseID INT = -1
)
RETURNS INT  
AS
BEGIN
/*-------------------------------------------------------------
Author		Date		Comments
Pramod K	09/14/2018	PBI#2262672: TPG_Import to Allow the Option to Not Calculate Withhlding 
						if Already Elected into Composite by State.
Shiv		07/22/2022	Product Backlog Item 231338: GSAM | States | Stop Payments From Allocating for Certain States
Rakesh N    09/01/2023  Fixed issue with Entity Config latest transactionID returning null
Shraddha    02/08/2024  Add entry for new datafeed to not consider entityid
--------------------------------------------------------------*/
DECLARE @ExcludeTransactionStatus INT 
SET @ExcludeTransactionStatus = 0 

DECLARE @StatusID TABLE  (StatusID INT) 

INSERT INTO @StatusID
SELECT	StatusID 
FROM	WorkflowStatus (NOLOCK) 
WHERE	EnumerationName IN ('Rejected','Err_Critical','Err_NonCritical')
		UNION 
SELECT  @ExcludeTransactionStatus 

----------------------GET PHASE ID----------------------------
IF @PhaseID= -1
BEGIN
	SELECT @PhaseID = dbo.[udfGetPhaseID](@ClientID, @TaxPeriodID)
END

DECLARE @UseEntityID BIT -- Flag to check whether to use the entity id
IF Exists(SELECT 1
			WHERE @EventTypeID IN (SELECT EventTypeID FROM ENU_Event
									WHERE EventName IN ('Import_EntityRelationship', 'Import_Historic', 'Import_MasterTaxableIncome','DataFeed_ByEntityInvestment' 
									, 'DataFeed_Entities', 'DataFeed_Deals-Specific', 'DataFeed_Investors-Specific', 'DataFeed_Investors'
									, 'DataFeed_Deals', 'DataFeed_Chart of Accounts', 'DataFeed_Financial', 'Import_CompositeWithholdingBridge','Import_WHPaymentAllocation','Import_EntityConfiguration')
									)
			)
	SET @UseEntityID = 0
ELSE
	SET @UseEntityID = 1

-- Get the max available transaction id
DECLARE @TransactionID INT 

-- Exclude Failed transactions. 
IF @IncludeFailed = 0 
BEGIN
	-- For some limited events we do not need the entity
	IF @UseEntityID = 0
	BEGIN
		SELECT @TransactionID = MAX(tl.TransactionID)   
								FROM TransactionLog tl (NOLOCK)
								WHERE tl.ClientID = @ClientID  
									AND tl.EventTypeID =@EventTypeID
									AND tl.TaxPeriodID =@TaxPeriodID
									AND tl.StatusID NOT IN    
									(	
										SELECT StatusID FROM @StatusID   
									)
									AND TL.PhaseID=@PhaseID
									
	END
	ELSE
	BEGIN
		SELECT @TransactionID = MAX(tl.TransactionID)   
								FROM TransactionLog tl (NOLOCK)
								INNER JOIN VW_Entity e
									ON tl.EntityID = e.EntityID
								WHERE tl.ClientID = @ClientID  
									AND tl.EventTypeID =@EventTypeID
									AND tl.TaxPeriodID =@TaxPeriodID
									AND tl.StatusID NOT IN    
									(	
										SELECT StatusID FROM @StatusID     
									)
									AND e.EntityID = @EntityID
									AND TL.PhaseID=@PhaseID
	END
END 
ELSE 
--Include all transactions
BEGIN
	IF @UseEntityID = 0
	BEGIN
		SELECT @TransactionID = MAX(tl.TransactionID)   
								FROM TransactionLog tl (NOLOCK)
									--JOIN dbo.Entity E (NOLOCK)
											--ON E.TransactionID = tl.TransactionID
									--		ON E.EntityID= tl.EntityID
								WHERE tl.ClientID = @ClientID
									AND	tl.TaxPeriodID =@TaxPeriodID
									AND tl.EventTypeID =@EventTypeID  
									AND tl.StatusID<> @ExcludeTransactionStatus
									AND TL.PhaseID=@PhaseID
									
							-- updated by tai on 2/21 to include phases
	END
	ELSE
	BEGIN
		SELECT @TransactionID = MAX(tl.TransactionID)   
								FROM TransactionLog tl (NOLOCK)
									JOIN  VW_Entity E (NOLOCK)
										ON tl.EntityID = @EntityID
								WHERE tl.ClientID = @ClientID
									AND	tl.TaxPeriodID =@TaxPeriodID
									AND tl.EventTypeID =@EventTypeID  
									AND E.EntityID = @EntityID
									AND tl.StatusID<> @ExcludeTransactionStatus
									AND TL.PhaseID=@PhaseID
									
							-- updated by tai on 2/21 to include phases
	END
END 
  
RETURN @TransactionID

END

