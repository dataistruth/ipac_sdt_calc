CREATE TABLE [dbo].[PFICFootnotePackage](
	[PFICFootnoteID] INT IDENTITY(1,1) NOT NULL,
	[K1PackageID] INT NOT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[IsDerivedPFIC] BIT NULL
)

ALTER TABLE [dbo].[PFICFootnotePackage] ADD PRIMARY KEY ([PFICFootnoteID])