CREATE TABLE [dbo].[ENU_LineType](
	[LineTypeID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[LineType] VARCHAR(50) NOT NULL,
	[DisplayOrder] INT NULL
)

ALTER TABLE [dbo].[ENU_LineType] ADD PRIMARY KEY ([LineTypeID])