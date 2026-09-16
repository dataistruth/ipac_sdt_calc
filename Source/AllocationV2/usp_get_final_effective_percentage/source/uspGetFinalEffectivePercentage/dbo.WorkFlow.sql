CREATE TABLE [dbo].[WorkFlow](
	[WorkflowID] INT IDENTITY(1,1) NOT NULL,
	[K1PackageID] INT NULL,
	[TransactionID] INT NULL,
	[SubmitByID] VARCHAR(100) NULL,
	[SubmitDate] DATETIME NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[PhaseID] INT NULL,
	[IsLastStep] BIT NULL,
	[UnderReviewLevelID] INT NULL
)

ALTER TABLE [dbo].[WorkFlow] ADD PRIMARY KEY ([WorkflowID])