CREATE TABLE [dbo].[BookEffective_Snapshot](
	[WorkflowID] INT NULL,
	[TransactionID] INT NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NULL,
	[EntityID] INT NULL,
	[UnderlyingEntityID] INT NULL,
	[SourceID] INT NULL,
	[FootNoteID] INT NULL,
	[LineID] INT NULL,
	[AllocationTypeID] INT NULL,
	[AdjustmentAllocationTypeID] INT NULL,
	[Tag] VARCHAR(500) NULL,
	[TrackingKey] VARCHAR(4000) NULL,
	[SourceEntityID] INT NULL,
	[IsExcludefromTransfer] BIT NULL,
	[DealID] VARCHAR(500) NULL
)