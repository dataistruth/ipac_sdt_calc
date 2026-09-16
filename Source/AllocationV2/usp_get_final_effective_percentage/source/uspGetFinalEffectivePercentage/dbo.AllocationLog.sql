CREATE TABLE [dbo].[AllocationLog](
	[LogID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[Category] VARCHAR(50) NULL,
	[ProcessName] VARCHAR(100) NULL,
	[LogDescription] VARCHAR(1000) NULL,
	[RunID] INT NULL,
	[StartDate] DATETIME NOT NULL,
	[EndDate] DATETIME NULL
)

ALTER TABLE [dbo].[AllocationLog] ADD PRIMARY KEY ([LogID])