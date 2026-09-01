CREATE TABLE [dbo].[ENU_GlobalMenuGroup](
	[GlobalMenuGroupID] INT IDENTITY(1,1) NOT NULL,
	[GroupName] VARCHAR(50) NOT NULL,
	[GroupConfig] VARCHAR(1) NULL,
	[BOEParentId] INT NULL
)

ALTER TABLE [dbo].[ENU_GlobalMenuGroup] ADD PRIMARY KEY ([GlobalMenuGroupID])