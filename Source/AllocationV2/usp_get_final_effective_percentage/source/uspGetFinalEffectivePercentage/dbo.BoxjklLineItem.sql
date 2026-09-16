CREATE TABLE [dbo].[BoxjklLineItem](
	[LineID] INT IDENTITY(1,1) NOT NULL,
	[LineNumber] VARCHAR(10) NULL,
	[Box] VARCHAR(20) NULL,
	[LineDescription] VARCHAR(100) NULL,
	[TaxPeriodID] INT NULL,
	[Comment] VARCHAR(MAX) NULL,
	[AllocationType] VARCHAR(150) NULL,
	[DisplayOrder] INT NOT NULL,
	[LineDataType] VARCHAR(20) NULL,
	[ClientID] INT NOT NULL,
	[CreateBy] VARCHAR(50) NULL,
	[UpdateDate] DATETIME NULL
)

ALTER TABLE [dbo].[BoxjklLineItem] ADD PRIMARY KEY ([LineID])