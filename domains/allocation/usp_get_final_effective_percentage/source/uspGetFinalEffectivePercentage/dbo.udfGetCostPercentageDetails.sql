
CREATE FUNCTION [dbo].[udfGetCostPercentageDetails](@WorkFlowID	INT)
RETURNS @CostPercentageSnapshot TABLE(
		WorkFlowID INT,
		TransactionID INT,
		ClientID INT,
		TaxPeriodID INT,
		EntityId INT,
		InvestmentID INT,
		PartnerNumber VARCHAR(200),
		Quarter VARCHAR(50),
		CommitmentPercent FLOAT,
		AllocationTypeId INT,
		Tag VARCHAR(5000),
		TrackingKey VARCHAR(4000),
		Underlyingtype INT,
		AllocatedAmount FLOAT,
		CostPercentageId INT
)
AS
BEGIN
/*========================================================================================================
Author		Date		Comment
Davinder	09/08/2023 	This function will return data from CostPercentage_Snapshot for the given WorkflowID.
						It will also ensure we exclude deleted investments from the results
Meera J		12/14/2023	Bug 307417: Regression_Asset Class percentages are ignored in the Reports
Anupama S   01/17/2023  Bug 310229: Apollo Global Management | Workflow | Calcs are taking longer than normal
/Aditya   
Yash J		06/26/2024	PBI 329049: General Atlantic | Allocations | Allocations based on Deal ID Part 2 (Allocations)
Shiv		08/06/2024	Bug 335262: TPG Capital | GAAP to Tax Underlying | K-1 Pickup Incorrect & Duplicate Line Item
Pavan R		11/21/2024	Performance optimization
Nafees      08/12/2026  Task 371099 | Optimization by filtering data early before final join
Rohan R     02/12/2026 Product Backlog Item 377585: Tech | Optimize Long running Import, Reports
=========================================================================================================*/  
 
DECLARE  
@CostPercentageSnapshotTemp TABLE(  
WorkFlowID int, TransactionID int, ClientID int, TaxPeriodID int, EntityId int, InvestmentID int, PartnerNumber varchar(200),  
Quarter varchar(50), CommitmentPercent float, AllocationTypeId int, Tag varchar(5000),   
TrackingKey varchar(4000), Underlyingtype int, AllocatedAmount  float, CostPercentageId int, DealID varchar(500))  
  

 DECLARE  
@CostPercentageSnapshotTempPreFiltered TABLE(  
WorkFlowID int, TransactionID int, ClientID int, TaxPeriodID int, EntityId int, InvestmentID int, PartnerNumber varchar(200),  
Quarter varchar(50), CommitmentPercent float, AllocationTypeId int, Tag varchar(5000),   
TrackingKey varchar(4000), Underlyingtype int, AllocatedAmount  float, CostPercentageId int, DealID varchar(500))  
  
DECLARE @CostPercentageSnapshotTempDistinct TABLE(  
EntityId int, InvestmentID int, Quarter varchar(50), AllocationTypeId int, Tag varchar(5000),   
TrackingKey varchar(4000), Underlyingtype int)  
  
DECLARE @EntityHierarchy TABLE(EntityID INT, UpperTierEntityID int, LowerTierEntityID int)  
DECLARE @tmpSelectedEntities TABLE (EntityID INT)  
DECLARE @TempEntityDeals TABLE(EntityID INT, UpperTierEntityID int, LowerTierEntityID int, Custom10 varchar(500))  
  
INSERT INTO @CostPercentageSnapshotTemp(WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityId, InvestmentID, PartnerNumber, Quarter, CommitmentPercent,      
  AllocationTypeId, Tag, TrackingKey, Underlyingtype, AllocatedAmount, CostPercentageId, DealID)     
  SELECT DISTINCT WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, C.EntityId, InvestmentID, PartnerNumber, Quarter, CommitmentPercent,      
  C.AllocationTypeId, ISNULL(Tag,''), ISNULL(TrackingKey,''), C.Underlyingtype, AllocatedAmount, CostPercentageId, DealID     
  FROM CostPercentage_Snapshot C (NOLOCK)     
  INNER JOIN VW_Entity E1 (NOLOCK) ON C.EntityID = E1.EntityID    
  WHERE C.WorkFlowID = @WorkFlowID

INSERT INTO @CostPercentageSnapshotTempPreFiltered
SELECT T.WorkFlowID, T.TransactionID, T.ClientID, T.TaxPeriodID, T.EntityId, T.InvestmentID, T.PartnerNumber, T.Quarter, T.CommitmentPercent,      
  T.AllocationTypeId, T.Tag, T.TrackingKey, T.Underlyingtype, T.AllocatedAmount, T.CostPercentageId, T.DealID
FROM @CostPercentageSnapshotTemp T
INNER JOIN ENU_UnderlyingType EU 
	ON EU.UnderlyingTypeID = T.Underlyingtype
WHERE T.InvestmentID = -2
	AND EU.UnderlyingType <> 'ASSET CLASS';    
  
INSERT INTO @CostPercentageSnapshotTempDistinct(EntityId, InvestmentID, Quarter, AllocationTypeId, Tag, TrackingKey, Underlyingtype)  
SELECT DISTINCT EntityId, InvestmentID, Quarter, AllocationTypeId,ISNULL(Tag,''), ISNULL(TrackingKey,''), Underlyingtype  
FROM @CostPercentageSnapshotTemp  
WHERE ISNULL(DealID,'') = '' and InvestmentID IS NULL 

-- Get Entity Hierarchy to get Investment EntityID and Custom 10 values  
INSERT INTO @tmpSelectedEntities(EntityID)  
Select DISTINCT EntityID from @CostPercentageSnapshotTemp WHERE ISNULL(DealID,'') <> ''  
                  
 ;WITH EntityUnderlyingCTE (EntityID,UppertierEntityid, LowerTierEntityId, LevelType)  
 AS (SELECT  
 T.EntityId,  
  UpperTierEntityId,  
  LowerTierEntityId,  
  0 [LevelType]  
 FROM EntityRelationShip ER  
 INNER JOIN @tmpSelectedEntities T  
  ON ER.UpperTierEntityID = T.EntityID  
  
 UNION ALL  
  
 SELECT  
 EntityID,  
  R.UpperTierEntityId,  
  R.LowerTierEntityId,  
  LevelType + 1  
 FROM EntityRelationShip (NOLOCK) R  
 JOIN EntityUnderlyingCTE U  
  ON R.UpperTierEntityId = U.LowerTierEntityId)  
  
 INSERT INTO @EntityHierarchy (EntityID,UpperTierEntityID, LowerTierEntityID)  
  SELECT DISTINCT  
  EntityID,  
  UpperTierEntityID,  
  LowerTierEntityID  
  FROM EntityUnderlyingCTE  
  
  --Include Local entity id  
  INSERT INTO @EntityHierarchy (EntityID,LowerTierEntityID)  
  SELECT Entityid,Entityid                    
  FROM @tmpSelectedEntities  
  WHERE Entityid <>-1  
  
  INSERT INTO @TempEntityDeals(EntityID, UpperTierEntityID, LowerTierEntityID, Custom10)  
  SELECT ER.EntityID, ER.UpperTierEntityID, ER.LowerTierEntityID, V.Custom10 from @EntityHierarchy ER  
  JOIN VW_Entity (NOLOCK) V on ER.LowerTierEntityID = V.EntityID   
  Where ISNULL(V.Custom10,'') <> ''  
  
INSERT INTO @CostPercentageSnapshot (WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityId, InvestmentID, PartnerNumber, Quarter, CommitmentPercent,  
  AllocationTypeId, Tag, TrackingKey, Underlyingtype, AllocatedAmount, CostPercentageId)  
SELECT DISTINCT WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, C.EntityId, InvestmentID, PartnerNumber, Quarter, CommitmentPercent,  
  C.AllocationTypeId, Tag, TrackingKey, C.Underlyingtype, AllocatedAmount, CostPercentageId   
FROM @CostPercentageSnapshotTemp C   
INNER JOIN ENU_UnderlyingType EU on EU.UnderlyingTypeID = C.Underlyingtype  
WHERE  EU.UnderlyingType <> 'ASSET CLASS' and C.InvestmentID=-1  
UNION   
SELECT DISTINCT WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, C.EntityId, InvestmentID, PartnerNumber, Quarter, CommitmentPercent,  
  C.AllocationTypeId, Tag, TrackingKey, C.Underlyingtype, AllocatedAmount, CostPercentageId   
FROM @CostPercentageSnapshotTemp C   
INNER JOIN VW_Entity E2 (NOLOCK) ON C.InvestmentID=E2.EntityID   
INNER JOIN ENU_UnderlyingType EU on EU.UnderlyingTypeID = C.Underlyingtype  
WHERE  EU.UnderlyingType <> 'ASSET CLASS' and c.InvestmentID NOT IN (-1,-2)  
UNION   
SELECT DISTINCT WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, C.EntityId, InvestmentID, PartnerNumber, Quarter, CommitmentPercent,    
  C.AllocationTypeId, Tag, TrackingKey, C.Underlyingtype, AllocatedAmount, CostPercentageId     
FROM @CostPercentageSnapshotTemp C   
INNER JOIN ENU_UnderlyingType EU on EU.UnderlyingTypeID = C.Underlyingtype  
INNER JOIN Enu_AssetClass EA ON C.InvestmentID = EA.AssetClassID   
WHERE  EU.UnderlyingType = 'ASSET CLASS'  

-- Insert percentages for investments with Deal level percentages having same custom10  
INSERT INTO @CostPercentageSnapshot (WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityId, InvestmentID, PartnerNumber, Quarter, CommitmentPercent,  
  AllocationTypeId, Tag, TrackingKey, Underlyingtype, AllocatedAmount, CostPercentageId)  
SELECT DISTINCT C.WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, E2.UpperTierEntityID, E2.LowerTierEntityID,C.PartnerNumber, C.Quarter, C.CommitmentPercent,  
  C.AllocationTypeId, C.Tag, C.TrackingKey, C.Underlyingtype, C.AllocatedAmount, C.CostPercentageId   
FROM @CostPercentageSnapshotTempPreFiltered C   
INNER JOIN @TempEntityDeals E2 ON C.DealID=E2.Custom10 AND C.EntityId = E2.EntityID  
LEFT JOIN @CostPercentageSnapshotTempDistinct C2   
 ON E2.LowerTierEntityID = C2.InvestmentID   
 AND C.AllocationTypeId = C2.AllocationTypeId   
 AND C.Underlyingtype = C2.Underlyingtype   
 AND C.Quarter = C2.Quarter   
 AND C.Tag = C2.Tag  
 AND C.TrackingKey = C2.TrackingKey

RETURN  

END  

