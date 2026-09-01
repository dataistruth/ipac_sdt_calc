CREATE TABLE [dbo].[AtRiskPackage](
	[AtRiskID] INT IDENTITY(1,1) NOT NULL,
	[K1PackageID] INT NULL,
	[ClientID] BIGINT NULL,
	[TaxPeriodID] INT NULL,
	[AtRiskName] VARCHAR(200) NULL
)