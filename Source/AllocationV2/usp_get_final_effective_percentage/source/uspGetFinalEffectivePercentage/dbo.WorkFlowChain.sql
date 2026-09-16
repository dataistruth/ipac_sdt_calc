CREATE TABLE [dbo].[WorkFlowChain](
	[WorkflowChainID] INT IDENTITY(1,1) NOT NULL,
	[StepNumber] SMALLINT NOT NULL,
	[ClientID] INT NOT NULL,
	[RoleID] UNIQUEIDENTIFIER NULL,
	[WorkflowStatusID] INT NULL,
	[TaxPeriodID] INT NOT NULL,
	[IncludeInCalc] BIT NULL
)

ALTER TABLE [dbo].[WorkFlowChain] ADD PRIMARY KEY ([WorkflowChainID])