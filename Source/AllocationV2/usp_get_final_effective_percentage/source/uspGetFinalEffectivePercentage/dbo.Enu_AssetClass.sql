CREATE TABLE [dbo].[Enu_AssetClass](
	[AssetClassID] INT IDENTITY(1,1) NOT NULL,
	[AssetClass] VARCHAR(100) NOT NULL,
	[ClientID] INT NULL,
	[DisplayOrder] INT NULL,
	[UpdatedBy] VARCHAR(50) NULL,
	[DateUpdated] DATETIME NULL
)

ALTER TABLE [dbo].[Enu_AssetClass] ADD PRIMARY KEY ([AssetClassID])