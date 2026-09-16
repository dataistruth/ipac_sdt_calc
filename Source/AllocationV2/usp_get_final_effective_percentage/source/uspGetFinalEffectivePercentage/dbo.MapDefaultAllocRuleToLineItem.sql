CREATE TABLE [dbo].[MapDefaultAllocRuleToLineItem](
	[TransactionID] INT NOT NULL,
	[SourceID] INT NOT NULL,
	[SelectedMappingID] INT NOT NULL,
	[RuleID] INT NOT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[EntityID] INT NOT NULL,
	[StateID] INT NOT NULL,
	[ExcludeFromTransfers] INT NOT NULL
)