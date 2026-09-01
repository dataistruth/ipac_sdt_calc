
CREATE FUNCTION [dbo].[udf_PE_GetPartnersListForReports](
	@ClientID INT,
	@TaxPeriodID INT,
	@EntityListCSV VARCHAR(MAX),
	@PhaseID INT = NULL
)
RETURNS @PartnersList TABLE([WorkFlowID] [INT] NULL, [PartnerID] [INT] NULL, [Transactionid] [INT] NULL, [Clientid] [INT] NULL, [TaxperiodID] [INT] NULL
, [MasterID] [VARCHAR](50) NULL, [PartnerNumber] [VARCHAR](50) NULL, [Name1] [VARCHAR](250) NULL, [Name2] [VARCHAR](250) NULL, [Name3] [VARCHAR](250) NULL
, [DomState] [VARCHAR](50) NULL
, [EIN] [VARCHAR](50) NULL, [GPorLP] [VARCHAR](50) NULL
, [DomOrForeign] [VARCHAR](50) NULL, [EntityType] [INT] NULL, [Residency] [VARCHAR](50) NULL, [FinalK1] [BIT] NULL
, [AmendedK1] [BIT] NULL, [ShareClass] [VARCHAR](50) NULL
, [EntityName] [VARCHAR](200) NULL, [EntityID] [INT] NULL, [EntityIdentification] [VARCHAR](150) NULL
, [DisplayOrder] [INT] NULL, [FullName] [VARCHAR](1000) NULL
, [UpperTierEntityIdentification] [VARCHAR](200) NULL, [Commitment] float NULL
, [StatePartnerType] [VARCHAR](150) NULL, [ExemptOrgStatePartnerType] VARCHAR(150) NULL
)

AS
BEGIN
/*========================================================================================================
Author		Date		Comment
Subbu S     06/03/2019  Initial creation to get the specific columns for partners used in reports instead of all columns.
                        This function is created by copying the function udf_PE_GetPartnersList
Vipin G		06/23/2020	Pbi#190557 Populating State Partner Type Column if the value is null.
Madhu B		06/09/2021	ADO 196669: handling entity level partner import
=========================================================================================================*/
	DECLARE @LocalClientID INT = @ClientID
	, @LocalTaxPeriodID INT = @TaxPeriodID
	, @LocalPhaseID INT = @PhaseID
	, @LocalEntityID INT
	, @PEEntityType INT
	, @PEModel VARCHAR(1),@FundTypeID INT

	DECLARE @PartnerImportEventID INT, @PEAllocationType VARCHAR(50)
	DECLARE @TmpEntity TABLE (EntityID INT, EntityType VARCHAR(50), PartnerLatestWorkflowID INT, PartnerLatestTransactionID INT)

	Select @FundTypeID = EntityTypeID from ENU_EntityType where EntityTypeName='Fund' and ClientID=@LocalClientID

	SELECT @LocalEntityID = CASE WHEN CHARINDEX (',', @EntityListCSV) = 0 THEN CONVERT(INT, LTRIM(RTRIM(@EntityListCSV))) ELSE NULL END

	--Populate the @TmpEntity with the incoming SP parameter @EntityListCSV.
	IF @LocalEntityID IS NULL
	BEGIN
		INSERT INTO @TmpEntity(EntityID)
		SELECT TIds AS EntityID FROM dbo.Split(@EntityListCSV, ',')
	END
	ELSE
	BEGIN
		INSERT INTO @TmpEntity(EntityID) VALUES(@LocalEntityID)
	IF ISNULL(@EntityListCSV,'') =''
	BEGIN
		INSERT INTO @TmpEntity(EntityID)
		SELECT EntityID AS EntityID FROM VW_Entity where ClientID =@LocalClientID and TaxPeriodID=@LocalTaxPeriodID
		and FundOrInvestmentID=@FundTypeID and EntityID<> @LocalEntityID
	END
	END

	SELECT @PartnerImportEventID = dbo.udf_PE_GetPartnerImportEventID(@LocalClientID,@LocalTaxPeriodID)

	IF @LocalPhaseID IS NULL
	BEGIN
		SELECT @LocalPhaseID = PhaseID
		FROM Phase WHERE EndDate IS NULL
		AND ClientID = @ClientID
		AND TaxPeriodID = @TaxPeriodID
	END

	SELECT @PEEntityType = ID
	FROM PE_ENU_DataList
	WHERE Value = 'PE Entity'

	 UPDATE E   
		SET  
		E.PartnerLatestWorkflowID = dbo.udfGetLastSubmittedWorkflow_Phase(@LocalClientID, @LocalTaxPeriodID, @PartnerImportEventID, E.EntityID, @LocalPhaseID),  
		E.PartnerLatestTransactionID = dbo.udfGetLastTransactionIDForPartner_Phase(@LocalClientID, @LocalTaxPeriodID, @PartnerImportEventID, E.EntityID, @LocalPhaseID)  
		FROM @TmpEntity E  
		INNER JOIN VW_Entity EN (NOLOCK)  
		ON E.[EntityID] = EN.EntityID  
		WHERE EN.ClientID = @LocalClientID   
		AND EN.TaxPeriodID = @LocalTaxPeriodID 

	INSERT INTO @PartnersList([WorkFlowID], [PartnerID], [Transactionid], [Clientid], [TaxperiodID]
	, [MasterID], [PartnerNumber], [Name1], [Name2], [Name3]
	, [DomState]
	, [EIN], [GPorLP]
	, [DomOrForeign], [EntityType], [Residency], [FinalK1]
	, [AmendedK1], [ShareClass]
	, [EntityName], [EntityID], [EntityIdentification]
	, [DisplayOrder], [FullName]
	, [UpperTierEntityIdentification], [Commitment]
	, [StatePartnerType], [ExemptOrgStatePartnerType]
	)
	SELECT DISTINCT [WorkFlowID], PS.[PartnerID], PS.[Transactionid], PS.[Clientid], PS.[TaxperiodID]
	, PS.[MasterID], PS.[PartnerNumber], PS.[Name1], PS.[Name2], PS.[Name3]
	, PS.[DomState]
	, PS.[EIN], PS.[GPorLP]
	, PS.[DomOrForeign], PS.[EntityType], PS.[Residency], PS.[FinalK1]
	, PS.[AmendedK1],  PS.[ShareClass]
	, PS.[EntityName], PS.[EntityID], PS.[EntityIdentification]
	, PS.[DisplayOrder], PS.[FullName]
	, PS.[UpperTierEntityIdentification], PS.[Commitment]
	, CASE WHEN ISNULL(PS.StatePartnerType,'') = '' THEN M.StatePartnerType ELSE PS.StatePartnerType END [StatePartnerType]
	, PS.[ExemptOrgStatePartnerType]
	
	FROM [Partner_Snapshot] PS (NOLOCK)
	INNER JOIN @TmpEntity TE
	ON PS.[EntityID] = TE.[EntityID]
	AND	CASE WHEN ISNULL(PS.[WorkFlowID], 0) <> 0 THEN ISNULL(PS.[WorkFlowID], 0) ELSE  ISNULL(PS.[Transactionid], 0) END
		= CASE WHEN ISNULL(PS.[WorkFlowID], 0) <> 0 THEN ISNULL(TE.[PartnerLatestWorkflowID], 0) ELSE  ISNULL(TE.[PartnerLatestTransactionID], 0) END
	LEFT JOIN K1GPartnerTypes K
	ON K.PartnerTypeID=PS.EntityType
	LEFT JOIN SM_FederaltoStatePartnerTypeMapping M
	ON M.FederalPartnerType=K.PartnerTypeDesc
	WHERE PS.[Clientid] = @LocalClientID
	AND PS.[TaxperiodID] = @LocalTaxPeriodID
	
	RETURN
END

