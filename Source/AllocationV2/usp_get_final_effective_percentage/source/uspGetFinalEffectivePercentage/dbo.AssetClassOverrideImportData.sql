CREATE TABLE [dbo].[AssetClassOverrideImportData](
	[TransactionID] INT NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NULL,
	[EntityID] INT NULL,
	[UnderlyingID] INT NULL,
	[TaxableIncome] FLOAT NULL,
	[AssetClassID] INT NULL,
	[OverrideAssetClassID] INT NULL,
	[TrackingKey] VARCHAR(4000) NULL
)