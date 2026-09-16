CREATE TABLE [dbo].[ENU_AllocationLogic](
	[AllocationTypeID] INT IDENTITY(1,1) NOT NULL,
	[AllocationTypeName] VARCHAR(100) NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NOT NULL,
	[IsDisplay] BIT NULL
)

ALTER TABLE [dbo].[ENU_AllocationLogic] ADD PRIMARY KEY ([AllocationTypeID])