CREATE TABLE [dbo].[GlobalMenu](
	[GlobalMenuID] INT IDENTITY(1,1) NOT NULL,
	[GlobalMenuGroupID] INT NULL,
	[MenuName] VARCHAR(400) NULL,
	[URL] VARCHAR(400) NULL,
	[State] VARCHAR(10) NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NULL,
	[IsHedge] BIT NULL,
	[IsTechConfig] BIT NULL,
	[UserID] NVARCHAR(128) NULL,
	[ValidFrom] DATETIME2 NOT NULL,
	[ValidTo] DATETIME2 NOT NULL,
	[IsStandardizationEnabled] BIT NULL,
	[AllowStandardizationUpdate] BIT NULL
)

ALTER TABLE [dbo].[GlobalMenu] ADD PRIMARY KEY ([GlobalMenuID])