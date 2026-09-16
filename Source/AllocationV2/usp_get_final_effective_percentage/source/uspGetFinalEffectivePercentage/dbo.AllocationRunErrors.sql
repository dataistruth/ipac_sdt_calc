CREATE TABLE [dbo].[AllocationRunErrors](
	[ErrorID] INT IDENTITY(1,1) NOT NULL,
	[RunID] BIGINT NULL,
	[EntityID] INT NULL,
	[LineTypeID] INT NULL,
	[LineID] INT NULL,
	[ErrorMessage] VARCHAR(MAX) NULL,
	[LogID] INT NULL,
	[ErrororWarning] VARCHAR(20) NULL,
	[ErrorType] VARCHAR(20) NULL
)

ALTER TABLE [dbo].[AllocationRunErrors] ADD PRIMARY KEY ([ErrorID])