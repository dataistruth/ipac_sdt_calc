CREATE TABLE [dbo].[LookThroughTaxableIncome](
	[RunID] BIGINT NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[EntityID] INT NOT NULL,
	[ShareClass] VARCHAR(200) NULL,
	[PartnerNumber] VARCHAR(50) NOT NULL,
	[TaxableIncome] FLOAT NULL,
	[ParentEntityID] INT NULL,
	[Tag] VARCHAR(5000) NULL
)