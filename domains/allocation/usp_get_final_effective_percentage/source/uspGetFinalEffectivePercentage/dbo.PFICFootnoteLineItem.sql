CREATE TABLE [dbo].[PFICFootnoteLineItem](
	[LineID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[LineDescription] VARCHAR(300) NOT NULL,
	[LineDataType] VARCHAR(20) NOT NULL,
	[ShortName] VARCHAR(50) NOT NULL,
	[DisplayOrder] INT NOT NULL,
	[IsActive] BIT NOT NULL,
	[IsAllocated] BIT NULL,
	[CreatedBy] VARCHAR(50) NULL,
	[UpdateDate] DATETIME NULL,
	[IsSpeciallyAllocated] BIT NULL,
	[Classification] VARCHAR(50) NULL,
	[IsConfigurable] BIT NOT NULL,
	[DefaultAllocationRule] VARCHAR(200) NULL,
	[IsPartVAllocated] BIT NOT NULL,
	[ValidateRule] VARCHAR(50) NULL,
	[ValidateType] VARCHAR(50) NULL,
	[isActiveADJReclassImport] BIT NOT NULL,
	[Unique_Identifier] BIT NOT NULL,
	[RoundingTypeID] INT NULL,
	[IsXtractOverride] BIT NOT NULL
)

ALTER TABLE [dbo].[PFICFootnoteLineItem] ADD PRIMARY KEY ([LineID])