CREATE TABLE [dbo].[Enu_Event](
	[EventTypeID] INT IDENTITY(1,1) NOT NULL,
	[EventName] VARCHAR(64) NOT NULL,
	[EventDescription] VARCHAR(150) NULL,
	[IsWorkflowEvent] BIT NULL,
	[IsDataFeedEvent] BIT NOT NULL
)

ALTER TABLE [dbo].[Enu_Event] ADD PRIMARY KEY ([EventTypeID])