CREATE TABLE [dbo].[Form8886LineItem](
	[LineID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[LineDescription] VARCHAR(2000) NULL,
	[LineDataType] VARCHAR(20) NULL,
	[ShortName] VARCHAR(250) NULL,
	[DisplayOrder] INT NOT NULL,
	[IsActive] BIT NOT NULL,
	[IsAllocated] BIT NULL,
	[CreatedBy] VARCHAR(50) NULL,
	[UpdateDate] DATETIME NULL,
	[IsSpeciallyAllocated] BIT NULL,
	[IsConfigurable] BIT NOT NULL,
	[isActiveADJReclassImport] BIT NOT NULL
)

ALTER TABLE [dbo].[Form8886LineItem] ADD PRIMARY KEY ([LineID])