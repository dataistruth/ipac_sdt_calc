CREATE TABLE [dbo].[Form8865LineItem](
	[LineID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[Schedule] VARCHAR(3) NOT NULL,
	[ScheduleDescription] VARCHAR(300) NOT NULL,
	[LineDescription] VARCHAR(300) NOT NULL,
	[LineDataType] VARCHAR(20) NULL,
	[ShortName] VARCHAR(50) NOT NULL,
	[DisplayOrder] INT NOT NULL,
	[IsAllocated] BIT NULL,
	[CreatedBy] VARCHAR(50) NULL,
	[UpdateDate] DATETIME NULL,
	[IsConfigurable] BIT NOT NULL,
	[IsActive] BIT NOT NULL,
	[IsSpeciallyAllocated] BIT NOT NULL
)

ALTER TABLE [dbo].[Form8865LineItem] ADD PRIMARY KEY ([LineID])