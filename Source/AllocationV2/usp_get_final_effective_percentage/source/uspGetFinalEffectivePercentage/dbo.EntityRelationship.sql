CREATE TABLE [dbo].[EntityRelationShip](
	[UpperTierEntityID] INT NOT NULL,
	[LowerTierEntityID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[ClientID] INT NULL,
	[TransactionID] INT NULL,
	[PK_ID] INT IDENTITY(1,1) NOT NULL,
	[UpdateDate] DATETIME NULL
)

ALTER TABLE [dbo].[EntityRelationShip] ADD PRIMARY KEY ([PK_ID])