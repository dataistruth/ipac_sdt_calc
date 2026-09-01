CREATE TABLE [dbo].[ENU_EntityType](
	[EntityTypeID] INT IDENTITY(1,1) NOT NULL,
	[EntityTypeName] VARCHAR(50) NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NOT NULL
)

ALTER TABLE [dbo].[ENU_EntityType] ADD PRIMARY KEY ([EntityTypeID])