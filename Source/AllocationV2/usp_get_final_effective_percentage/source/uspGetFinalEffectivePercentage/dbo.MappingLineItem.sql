CREATE TABLE [dbo].[MappingLineItem](
	[LineID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[SourceID] INT NOT NULL,
	[LineDescription] VARCHAR(100) NOT NULL,
	[DatabaseName] VARCHAR(50) NOT NULL,
	[DisplayOrder] INT NOT NULL,
	[IsActive] BIT NOT NULL,
	[CreateBy] VARCHAR(50) NULL,
	[UpdateDate] DATETIME NULL,
	[RegisterTypeID] INT NULL
)

ALTER TABLE [dbo].[MappingLineItem] ADD PRIMARY KEY ([LineID])