CREATE TABLE [dbo].[ENU_AttributeType](
	[AttributeID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NULL,
	[AttributeType] VARCHAR(50) NULL,
	[AttributeValue] VARCHAR(200) NULL,
	[DisplayOrder] INT NULL,
	[IsHidden] BIT NULL
)