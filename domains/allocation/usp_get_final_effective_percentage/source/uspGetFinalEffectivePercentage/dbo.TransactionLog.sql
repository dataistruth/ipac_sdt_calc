CREATE TABLE [dbo].[TransactionLog](
	[TransactionID] INT IDENTITY(1,1) NOT NULL,
	[EventTypeID] INT NOT NULL,
	[ClientID] INT NULL,
	[EntityID] INT NULL,
	[TaxPeriodID] INT NOT NULL,
	[TransactionDate] DATETIME NOT NULL,
	[UserLoginName] VARCHAR(226) NULL,
	[StatusID] INT NULL,
	[PhaseID] INT NULL,
	[CustomImportId] INT NULL,
	[WorkflowImportId] INT NULL
)

ALTER TABLE [dbo].[TransactionLog] ADD PRIMARY KEY ([TransactionID])