CREATE TABLE [dbo].[MAP_DerivedLines](
	[MapID] INT IDENTITY(1,1) NOT NULL,
	[BaseLineID] INT NULL,
	[DerivedLineID] INT NULL,
	[AttributeID] INT NULL,
	[ParentK1GLineID] INT NULL,
	[Level] INT NULL
)

ALTER TABLE [dbo].[MAP_DerivedLines] ADD PRIMARY KEY ([MapID])