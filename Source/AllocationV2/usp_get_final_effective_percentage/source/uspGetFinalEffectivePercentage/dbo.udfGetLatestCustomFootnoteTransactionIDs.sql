


/* E:\IPC_QA_SQL\Agent1\_work\r5\a\_iPACSCore_Src_Release_Ty21\ReleaseArtifacts\Sprint Development\TY21 Apr Build\Client\DML\dbo.udfGetLatestCustomFootnoteTransactionIDs.sql */

CREATE   FUNCTION [dbo].[udfGetLatestCustomFootnoteTransactionIDs](    
 @ClientID INT,    
 @TaxPeriodID INT,    
 @EntityListCSV VARCHAR(MAX),    
 @PhaseID INT,    
 @RegisterTypeID INT=-1,    
 @IsCallFromReports INT = 0,
 @IsLookThroughEntityListCSV INT =0
)    
RETURNS @CustomFootnoteTransactionIDs TABLE([EntityID] [INT] NULL, [TransactionID] [INT] NULL, [LineTypeID] [INT] NULL, [EventTypeid] [INT] NULL, [RegisterTypeID] [INT] NULL    
, [K1PackageID] [INT] NULL)    
    
AS    
BEGIN    
/*========================================================================================================    
Author  Date  Comment    
Subbu S  03/23/2022 Initial Creation to provide list of CustomFootnote TransactionIDs for entities    
    
=========================================================================================================*/      
 DECLARE @LocalClientID INT = @ClientID      
 , @LocalTaxPeriodID INT = @TaxPeriodID      
 , @LocalPhaseID INT = @PhaseID      
 , @LocalEntityID INT    
 , @InvstmentTypeID INT    
    
 DECLARE @TmpEntity TABLE (EntityID INT)      
 DECLARE @TmpCustomFootnoteEventIDs TABLE (CustomFootnoteEventTypeID INT,LineTypeID INT,RegisterTypeID INT)      
    
      
 SELECT @InvstmentTypeID = EntityTypeID from ENU_EntityType where EntityTypeName='Investment' and ClientID=@LocalClientID      
      
     
 --Populate the @TmpEntity with the incoming SP parameter @EntityListCSV.      
    
  INSERT INTO @TmpEntity(EntityID)      
  SELECT TIds AS EntityID FROM dbo.Split(@EntityListCSV, ',')  
  

  --IF all the direct and indirect underlyings were already passed in EntityCSV list then no need to take again underlying investments
 IF @IsLookThroughEntityListCSV =0
 BEGIN   
  INSERT INTO @TmpEntity(EntityID)    
  SELECT E.EntityID FROM EntityRelationship ER JOIN @TmpEntity TE ON ER.UpperTierEntityID=TE.EntityID     
  JOIN Entity E ON ER.LowerTierEntityID=E.EntityID    
  WHERE E.FundOrInvestmentID=@InvstmentTypeID    
 END   
    
 IF (@IsCallFromReports = 1)    
 BEGIN    
  INSERT INTO @TmpCustomFootnoteEventIDs(CustomFootnoteEventTypeID,LineTypeID,RegisterTypeID)    
      SELECT EE.EventTypeID,EL.LineTypeID,CD.GlobalMenuID FROM CustomImportDetail CD JOIN ENU_Event EE ON CD.ImportName = EE.EventName    
  JOIN ENU_LineType EL ON EL.LineType = CD.ImportName    
     WHERE CD.IsCustomFootnote = 1 AND CD.GlobalMenuID = @RegisterTypeID    
 END    
 ELSE    
 BEGIN    
    
 INSERT INTO @TmpCustomFootnoteEventIDs(CustomFootnoteEventTypeID, LineTypeID,RegisterTypeID)    
     SELECT EE.EventTypeID,EL.LineTypeID, CD.GlobalMenuID FROM CustomImportDetail CD JOIN ENU_Event EE ON CD.ImportName = EE.EventName    
  JOIN ENU_LineType EL ON EL.LineType = CD.ImportName    
     WHERE CD.IsCustomFootnote = 1    
    
 END    
      
      
 IF @LocalPhaseID IS NULL      
 BEGIN      
  SELECT @LocalPhaseID = dbo.udfGetPhaseID(@clientID,@TaxPeriodID)    
 END      
      
    
    
INSERT INTO  @CustomFootnoteTransactionIDs([EntityID] , [TransactionID] , [LineTypeID], [EventTypeid], [RegisterTypeID], [K1PackageID])    
  SELECT E.EntityID,dbo.udfGetLastTransactionID_Phase(@LocalClientID, @LocalTaxPeriodID,0, TF.CustomFootnoteEventTypeID, E.EntityID, @LocalPhaseID)     
  ,TF.LineTypeID    
  ,TF.CustomFootnoteEventTypeID    
  ,TF.RegisterTypeID    
  ,ISNULL(K.K1PackageID,0)    
  FROM @TmpEntity E JOIN K1Package K ON E.EntityID = K.UpperTierEntityID    
  JOIN @TmpCustomFootnoteEventIDs TF ON 1=1    
      
       
 RETURN      
END  
