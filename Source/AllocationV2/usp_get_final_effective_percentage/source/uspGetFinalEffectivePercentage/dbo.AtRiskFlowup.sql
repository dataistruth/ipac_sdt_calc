CREATE TABLE [dbo].[AtRiskFlowup](
	[RunID] BIGINT NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[EntityID] INT NOT NULL,
	[FlowupEntityID] INT NOT NULL,
	[SourceEntityID] INT NOT NULL,
	[AtRiskID] INT NOT NULL,
	[LineID] INT NOT NULL,
	[Amount] FLOAT NULL,
	[TextValue] VARCHAR(100) NULL
)