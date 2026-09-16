CREATE TABLE [dbo].[QuarterDates](
	[ID] INT IDENTITY(1,1) NOT NULL,
	[Quarter] VARCHAR(10) NULL,
	[StartDate] DATETIME NULL,
	[EndDate] DATETIME NULL,
	[Preference] INT NULL
)