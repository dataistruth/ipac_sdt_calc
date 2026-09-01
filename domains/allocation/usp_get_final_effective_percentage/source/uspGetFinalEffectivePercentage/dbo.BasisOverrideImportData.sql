CREATE TABLE [dbo].[BasisOverrideImportData](
	[UpperTierEntityID] INT NOT NULL,
	[LowerTierEntityID] INT NULL,
	[LineID] VARCHAR(2000) NULL,
	[Labeltype] VARCHAR(500) NULL,
	[Value] VARCHAR(200) NULL,
	[ReportingBasisID] INT NULL,
	[Box] VARCHAR(500) NULL,
	[LineDescription] VARCHAR(5000) NULL,
	[LineTypeId] VARCHAR(500) NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NULL,
	[PK_ID] INT IDENTITY(1,1) NOT NULL
)

ALTER TABLE [dbo].[BasisOverrideImportData] ADD PRIMARY KEY ([PK_ID])