CREATE TABLE [dbo].[Form926LineItem](
	[LineID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[PartNumber] INT NOT NULL,
	[LineDescription] VARCHAR(500) NULL,
	[LineDataType] VARCHAR(20) NULL,
	[ShortName] VARCHAR(50) NOT NULL,
	[DisplayOrder] INT NOT NULL,
	[IsActive] BIT NOT NULL,
	[IsAllocated] BIT NULL,
	[CreatedBy] VARCHAR(50) NULL,
	[UpdateDate] DATETIME NULL,
	[IsSpeciallyAllocated] BIT NULL,
	[IsConfigurable] BIT NOT NULL,
	[isActiveADJReclassImport] BIT NOT NULL,
	[IsXtractOverride] BIT NOT NULL
)

ALTER TABLE [dbo].[Form926LineItem] ADD PRIMARY KEY ([LineID])