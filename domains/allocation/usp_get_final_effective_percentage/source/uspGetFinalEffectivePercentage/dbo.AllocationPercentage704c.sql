CREATE TABLE [dbo].[AllocationPercentage704c](
	[RunID] BIGINT NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[EntityID] INT NOT NULL,
	[InvestmentID] INT NULL,
	[PartnerNumber] VARCHAR(50) NOT NULL,
	[OrdinaryPercentage] FLOAT NULL,
	[CapitalPercentage] FLOAT NULL,
	[CapitalGainPercentage] FLOAT NULL,
	[CapitalLossPercentage] FLOAT NULL,
	[Quarter] VARCHAR(50) NULL,
	[AllocationTypeId] INT NULL,
	[704cAllocationTypeID] INT NULL,
	[Underlyingtype] INT NULL,
	[TrackingKey] VARCHAR(4000) NULL
)