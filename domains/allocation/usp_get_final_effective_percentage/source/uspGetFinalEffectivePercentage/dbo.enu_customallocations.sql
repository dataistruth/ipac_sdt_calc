CREATE TABLE [dbo].[enu_customallocations](
	[AllocationTypeID] INT IDENTITY(1,1) NOT NULL,
	[AllocationType] VARCHAR(100) NULL,
	[ClientID] INT NULL,
	[TaxPeriodID] INT NULL
)

ALTER TABLE [dbo].[enu_customallocations] ADD PRIMARY KEY ([AllocationTypeID])