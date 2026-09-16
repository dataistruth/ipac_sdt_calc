CREATE TABLE [dbo].[CustomFootNotePackage](
	[CustomFootnoteID] INT IDENTITY(1,1) NOT NULL,
	[K1PackageID] INT NOT NULL,
	[RowID] INT NOT NULL,
	[RegisterTypeID] INT NOT NULL,
	[ClientID] INT NOT NULL,
	[TaxPeriodID] INT NOT NULL,
	[TransactionID] INT NOT NULL
)

ALTER TABLE [dbo].[CustomFootNotePackage] ADD PRIMARY KEY ([CustomFootnoteID])