CREATE TABLE [dbo].[Form8886FlowUp](
	[RunID] BIGINT NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[EntityID] INT NOT NULL,
	[FlowupEntityID] INT NOT NULL,
	[SourceEntityID] INT NOT NULL,
	[Form8886ID] INT NOT NULL,
	[LineID] INT NOT NULL,
	[Amount] FLOAT NULL,
	[TextValue] VARCHAR(100) NULL,
	[TransactionName] VARCHAR(150) NULL,
	[TransactionEntityID] INT NULL,
	[Comments] VARCHAR(MAX) NULL,
	[SecIIComments] VARCHAR(MAX) NULL
)