CREATE TABLE [dbo].[SM_StateLineAllocationRule_Snapshot](
	[WorkflowID] INT NULL,
	[TransactionID] INT NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NULL,
	[EntityID] INT NULL,
	[UnderlyingEntityID] INT NULL,
	[SourceID] INT NULL,
	[StateID] INT NULL,
	[StateLineID] INT NULL,
	[AllocationTypeID] INT NULL,
	[AdjustmentAllocationTypeID] INT NULL,
	[Tag] VARCHAR(500) NULL,
	[TrackingKey] VARCHAR(4000) NULL,
	[DealID] VARCHAR(500) NULL
)