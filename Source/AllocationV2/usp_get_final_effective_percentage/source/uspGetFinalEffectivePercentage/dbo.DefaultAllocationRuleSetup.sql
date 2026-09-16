CREATE TABLE [dbo].[DefaultAllocationRuleSetup](
	[TransactionID] INT NOT NULL,
	[RuleID] INT NOT NULL,
	[AllocationByID] INT NOT NULL,
	[UnderlyingTypeID] INT NOT NULL,
	[RuleTypeID] INT NOT NULL,
	[RuleGroupID] INT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[EntityID] INT NOT NULL,
	[AllocationPercentageTypeID] INT NOT NULL
)