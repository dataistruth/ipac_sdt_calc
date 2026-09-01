CREATE TABLE [dbo].[CostPercentage_Snapshot](
	[WorkFlowID] INT NULL,
	[TransactionID] INT NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NULL,
	[EntityId] INT NULL,
	[InvestmentID] INT NULL,
	[PartnerNumber] VARCHAR(200) NULL,
	[Quarter] VARCHAR(50) NULL,
	[CommitmentPercent] FLOAT NULL,
	[AllocationTypeId] INT NULL,
	[Tag] VARCHAR(5000) NULL,
	[TrackingKey] VARCHAR(4000) NULL,
	[Underlyingtype] INT NULL,
	[AllocatedAmount] FLOAT NULL,
	[CostPercentageId] INT IDENTITY(1,1) NOT NULL,
	[DealID] VARCHAR(500) NULL
)