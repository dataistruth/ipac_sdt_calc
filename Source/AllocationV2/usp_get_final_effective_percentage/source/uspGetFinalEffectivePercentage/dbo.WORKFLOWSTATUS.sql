CREATE TABLE [dbo].[WorkflowStatus](
	[StatusID] INT IDENTITY(1,1) NOT NULL,
	[DisplayName] VARCHAR(128) NOT NULL,
	[EnumerationName] VARCHAR(128) NOT NULL,
	[ImagePath] VARCHAR(256) NULL,
	[Priority] INT NOT NULL
)

ALTER TABLE [dbo].[WorkflowStatus] ADD PRIMARY KEY ([StatusID])