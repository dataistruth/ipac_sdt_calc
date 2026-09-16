CREATE FUNCTION [dbo].[udfGetLatestTransactionID](  
 @ClientID AS INT,  
 @TaxPeriodID INT,  
 @IncludeFailed BIT,  
 @EventTypeID INT,  
 @EntityID INT = -1  
)  
RETURNS INT    
AS    
BEGIN 
-- =============================================
-- Author:		 
-- Anshu     06/17/2015  Return Latest transaction id and does not excludes transaction which are not submitted for review.
-- Utsav D   03/28/2020  Added beneficial owner import to get transactionId without entityId.
---Satish P   04/26/2020 AD0#184821 Changes to select default entity
-- Raaghav M 01/19/2021  PBI 205362: Added the register check import to get the latest transaction id.
-- Shilpi G 02/19/2021 PBI 206072:: Added the Line and LineMapping import to get the latest transaction id.  
-- Utsav D   03/15/2021  PBI 211787: Added Process Entity Configurations import.
-- Utsav D   07/29/2021  PBI 224726: Added chart of accounts mapping import
-- Rakesh N  03/03/2023 PBI#254964 - Added Entity Level Threshold import in the list to enable use entity as 0
-- Muni      05/02/2024  ADO #296789: General Atlantic | Import | Addition of Available Mappings Tab, Excludig Non Critical errors for Tax Capital
-- Siva Balaji 08/05/2025 ADO #371535: Exclusing Non Critical Error status for Default Allocation Rule Import.
-- ============================================= 
  
--DECLARE @ExcludeTransactionStatus INT   
--SET @ExcludeTransactionStatus = 0   

DECLARE @TaxCapitalImportEventTypeID INT;
DECLARE @StatusID TABLE  (StatusID INT);
DECLARE @DefaultAllocationRuleImportEventTypeID INT;

SELECT @TaxCapitalImportEventTypeID = EventTypeID FROM dbo.ENU_Event (NOLOCK) WHERE EventName = 'Import_TaxCapital';
SELECT @DefaultAllocationRuleImportEventTypeID = EventTypeID FROM ENU_Event (NOLOCK) WHERE EventName = 'Import_DefaultAllocationRule'

IF(@EventTypeID IN (@TaxCapitalImportEventTypeID,@DefaultAllocationRuleImportEventTypeID))  
BEGIN  
	INSERT	INTO @StatusID (StatusID)  
	SELECT	StatusID FROM dbo.WorkflowStatus (NOLOCK) 
	WHERE	EnumerationName IN ('Rejected', 'Err_Critical');
END  
ELSE   
BEGIN 
	INSERT	INTO @StatusID (StatusID)
	SELECT	StatusID 
	FROM	dbo.WorkflowStatus (NOLOCK) WHERE EnumerationName IN ('Rejected', 'Err_Critical', 'Err_NonCritical'); 
	-- UNION 
	--SELECT @ExcludeTransactionStatus
END
  
----------------------GET PHASE ID----------------------------  
DECLARE @PhaseID INT  
SELECT @PhaseID = dbo.[udfGetPhaseID](@ClientID, @TaxPeriodID)  
  
  
DECLARE @UseEntityID BIT -- Flag to check whether to use the entity id  
IF Exists(SELECT 1  
   WHERE @EventTypeID IN (SELECT EventTypeID FROM ENU_Event  
         WHERE EventName IN ('Import_EntityRelationship', 'Import_Historic', 'Import_MasterTaxableIncome','Import_CYToStandardLinesMapping', 
         'Import_ECIFDAPRate', 'Import_BeneficialOwner', 'Import_CrossEntityPartnerGroup', 'Import_RegisterCheck',
          'Import_ManageStatePYCutOffLines', 'Import_TrueUpStateLineMapping', 'Import_ProcessEntityConfigurations', 'Import_ChartOfAccountMapping','Import_EntityLevelStateThreshold','Import_EntityConfiguration')
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
        LEFT JOIN VW_Entity e  
         ON tl.EntityID = e.EntityID  
        WHERE tl.ClientID = @ClientID    
         AND tl.EventTypeID =@EventTypeID  
         AND tl.TaxPeriodID =@TaxPeriodID  
         AND tl.StatusID NOT IN      
         (   
          SELECT StatusID FROM @StatusID       
         )  
         AND tl.EntityID = @EntityID  
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
         --  ON E.EntityID= tl.EntityID  
        WHERE tl.ClientID = @ClientID  
         AND tl.TaxPeriodID =@TaxPeriodID  
         AND tl.EventTypeID =@EventTypeID    
        
         AND TL.PhaseID=@PhaseID  
           
       -- updated by tai on 2/21 to include phases  
 END  
 ELSE  
 BEGIN  
  SELECT @TransactionID = MAX(tl.TransactionID)     
        FROM TransactionLog tl (NOLOCK)  
         LEFT JOIN dbo.Entity E (NOLOCK)  
          ON tl.EntityID = @EntityID  
        WHERE tl.ClientID = @ClientID  
         AND tl.TaxPeriodID =@TaxPeriodID  
         AND tl.EventTypeID =@EventTypeID    
         AND tl.EntityID = @EntityID  
        
         AND TL.PhaseID=@PhaseID  
           
       -- updated by tai on 2/21 to include phases  
 END  
END   
  
    
RETURN @TransactionID  
  
END  

