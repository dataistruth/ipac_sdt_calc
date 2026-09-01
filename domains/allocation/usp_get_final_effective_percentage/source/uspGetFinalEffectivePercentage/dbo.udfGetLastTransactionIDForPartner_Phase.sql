CREATE FUNCTION [dbo].[udfGetLastTransactionIDForPartner_Phase](
	@ClientID AS INT,
	@TaxPeriodID INT,
	@EventTypeID INT,
	@EntityID INT = -1,
	@PhaseID	INT = -1
)
RETURNS INT  
AS  
BEGIN

		DECLARE @TransactionID INT
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
		--DECLARE @PhaseID INT
		IF @PhaseID= -1
		BEGIN
			SELECT @PhaseID = dbo.[udfGetPhaseID](@ClientID, @TaxPeriodID)
		END

		DECLARE @UseEntityID BIT -- Flag to check whether to use the entity id
		DECLARE @MenuName VARCHAR(50) -- MenuID to Check which is check (Master Import or Fund Partner)

		SELECT @MenuName= CASE WHEN MenuName='Master Import' THEN 'Master'
						   ELSE 'Fund' END							   
		FROM GlobalMenu GM
		JOIN ENU_GlobalMenuGroup ENU 
			ON ENU.GlobalMenuGroupID=GM.GlobalMenuGroupID
			WHERE ENU.GroupName='Partner Import Methodology'
					AND GM.State='C' 
					AND ClientID=@ClientID
					AND TaxPeriodID=@TaxPeriodID
		
		IF @MenuName='Master'

		BEGIN
			SELECT @EventTypeID = EventTypeID FROM ENU_Event WHERE EventName = 'MasterImport_Partner'

			SELECT @TransactionID = MAX(TL.TransactionID)
										FROM TransactionLog TL
										--JOIN PARTNER P ON P.TransactionID=TL.TransactionID
										WHERE TL.EntityID=0 
										--AND P.EntityID=@EntityID
										AND TL.ClientID=@ClientID 
										AND TL.TaxPeriodID=@TaxPeriodID
										AND TL.EventTypeID =@EventTypeID
										AND TL.StatusID NOT IN    
											(	
												SELECT StatusID FROM @StatusID   
											)
										AND TL.PhaseID=@PhaseID
		END

		ELSE
		BEGIN
		--Get Last Txn ID for Import Partner WHen Master TxnID is null
		SELECT @EventTypeID=EventTypeID FROM Enu_Event WHERE EventName='Import_Partner'

		SELECT @TransactionID = MAX(tl.TransactionID)   
										FROM TransactionLog tl (NOLOCK)
										WHERE tl.ClientID = @ClientID
											AND tl.EntityID=@EntityID  										
											AND tl.TaxPeriodID =@TaxPeriodID
											AND tl.StatusID NOT IN    
											(	
												SELECT StatusID FROM @StatusID   
											)
											AND tl.EventTypeID =@EventTypeID
											AND TL.PhaseID=@PhaseID

		END


  
RETURN @TransactionID

END
