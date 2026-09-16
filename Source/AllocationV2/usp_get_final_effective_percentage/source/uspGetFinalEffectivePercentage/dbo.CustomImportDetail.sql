CREATE TABLE [dbo].[CustomImportDetail](
	[CustomImportID] INT IDENTITY(1,1) NOT NULL,
	[GlobalMenuID] INT NULL,
	[ImportName] VARCHAR(100) NULL,
	[ImportLevel] VARCHAR(20) NULL,
	[ImportAt] VARCHAR(20) NULL,
	[ModifiedBy] VARCHAR(50) NULL,
	[ModifiedDate] DATETIME NULL,
	[EnableMapping] BIT NULL,
	[IsLookThroughImport] BIT NULL,
	[IsWindCreditImport] INT NULL,
	[IsCustomFootnote] BIT NULL,
	[DoNotSuppressBlankRows] BIT NULL,
	[CustomTemplatePath] VARCHAR(1000) NULL,
	[StandardSiteClientID] INT NULL,
	[StandardSiteIdentifier] INT NULL,
	[IsLinkToXtract] BIT NULL
)

ALTER TABLE [dbo].[CustomImportDetail] ADD PRIMARY KEY ([CustomImportID])