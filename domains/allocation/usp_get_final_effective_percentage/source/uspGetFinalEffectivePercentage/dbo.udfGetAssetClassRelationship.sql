

/* E:\IPC_QA_SQL\Agent1\_work\r13\a\_iPACSCore_Src_Release_Ty23\ReleaseArtifacts\Sprint Development\TY23 Feb Build\Client\DML\dbo.udfGetAssetClassRelationship.sql */

CREATE   FUNCTION [dbo].[udfGetAssetClassRelationship](
@ClientID INT  ,        
@TaxPeriodID INT ,      
@EntityIDs VARCHAR(max)=''
)
RETURNS @AssetClassOverrideDetail TABLE (LowerTierEntityID INT, AssetClassID INT, TrackingKey VARCHAR(4000))
AS 
/* ======================================================================================================            
Author  Date        Comment        
Anupama 05/25/2022 Initial Creation. PBI 222968: Partners Group - IPC/IPC2 Sync | Asset Class Override Import
Anupama 06/02/2022 Bug 246238: Asset Class Override Import | When 'Asset Class ER' and 'Override Asset Class' are blank,
                   it still take previous Asset Class ER as Allocation Rule
=========================================================================================================*/          
BEGIN
          
 DECLARE       
   @PhaseID INT,
   @AssetClassOverrideEvent INT ,
   @BasisOverrideImportEvent INT,
   @GlobalMenuGroupID INT
  
DECLARE @LocalEntityID VARCHAR(max) = @EntityIDs   

SELECT @AssetClassOverrideEvent = EventTypeID      
FROM Enu_Event ET      
WHERE ET.EventName = 'Import_AssetClassOverride'    

SELECT @GlobalMenuGroupID = GlobalMenuGroupID
FROM   ENU_GlobalMenuGroup
WHERE  GroupName='Other Logic/Imports'

DECLARE @tmpEntities TABLE (EntityId INT,AssetClassOverrideTransactionID INT)
DECLARE @BasisOverrideEntities TABLE(EntityID INT)

--CREATE TABLE @tmpEntities(EntityId INT,AssetClassOverrideTransactionID INT)
 INSERT INTO @tmpEntities(EntityId)
 SELECT Value from udfArrToTable(@LocalEntityID) 

 INSERT INTO @AssetClassOverrideDetail(LowerTierEntityID,AssetClassID)
 SELECT LowerTierEntityID, [Value] AssetClassID FROM BasisOverrideImportData BO
 INNER JOIN @tmpEntities T ON BO.UpperTierEntityID = T.EntityID 
 WHERE BO.LineDescription in ('Asset Class','AssetClass') AND Value <> '0'

 INSERT INTO @BasisOverrideEntities(EntityID)
 SELECT DISTINCT LowerTierEntityID
 FROM @AssetClassOverrideDetail

 IF EXISTS (SELECT top 1 1 FROM GlobalMenu WHERE GlobalMenuGroupID = @GlobalMenuGroupID AND MenuName = 'Asset Class Override Import' AND State = 'C')
 BEGIN

 UPDATE T SET AssetClassOverrideTransactionID = dbo.udfGetLatestTransactionID(@ClientID, @TaxPeriodID, 0, @AssetClassOverrideEvent, EntityID)       
 FROM @tmpEntities T

 INSERT INTO @AssetClassOverrideDetail(LowerTierEntityID,AssetClassID, TrackingKey)
 SELECT UnderlyingID, 
 CASE WHEN ISNULL(OverrideAssetClassID,'') = '' THEN CASE WHEN ISNULL(E.AssetClassID,'') = '' THEN -1
 ELSE
 E.AssetClassID END ELSE OverrideAssetClassID END, A.TrackingKey
 FROM AssetClassOverrideImportData A INNER JOIN @tmpEntities T ON A.TransactionID = T.AssetClassOverrideTransactionID
 INNER JOIN VW_Entity E ON  A.UnderlyingID = E.EntityId
 LEFT JOIN @BasisOverrideEntities B ON B.EntityID = T.EntityId
 WHERE B.EntityID IS NULL

 END 
 
RETURN
END
