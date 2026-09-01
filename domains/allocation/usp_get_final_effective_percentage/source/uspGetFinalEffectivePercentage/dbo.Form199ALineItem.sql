CREATE TABLE [dbo].[Form199ALineItem](
	[LineID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[LineDescription] VARCHAR(300) NOT NULL,
	[LineDataType] VARCHAR(20) NULL,
	[ShortName] VARCHAR(50) NOT NULL,
	[DisplayOrder] INT NOT NULL,
	[IsActive] BIT NOT NULL,
	[IsAllocated] BIT NULL,
	[CreatedBy] VARCHAR(50) NULL,
	[UpdateDate] DATETIME NULL,
	[IsSpeciallyAllocated] BIT NULL,
	[IsConfigurable] BIT NOT NULL,
	[isActiveADJReclassImport] BIT NOT NULL
)

ALTER TABLE [dbo].[Form199ALineItem] ADD PRIMARY KEY ([LineID])