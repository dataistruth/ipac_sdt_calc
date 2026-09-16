CREATE TABLE [dbo].[ENU_MappingSource](
	[SourceID] INT IDENTITY(1,1) NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[SourceName] VARCHAR(100) NOT NULL
)

ALTER TABLE [dbo].[ENU_MappingSource] ADD PRIMARY KEY ([SourceID])