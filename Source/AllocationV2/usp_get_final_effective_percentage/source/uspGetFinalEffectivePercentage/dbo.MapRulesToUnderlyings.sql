CREATE TABLE [dbo].[MapRulesToUnderlyings](
	[TransactionID] INT NOT NULL,
	[RuleID] INT NOT NULL,
	[UnderlyingID] INT NOT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[EntityID] INT NOT NULL,
	[704cAllocationTypeID] INT NULL
)