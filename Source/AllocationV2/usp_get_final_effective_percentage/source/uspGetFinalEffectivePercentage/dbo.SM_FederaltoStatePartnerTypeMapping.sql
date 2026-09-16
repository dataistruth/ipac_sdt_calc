CREATE TABLE [dbo].[SM_FederaltoStatePartnerTypeMapping](
	[MapPartnerTypeID] INT IDENTITY(1,1) NOT NULL,
	[FederalPartnerType] VARCHAR(50) NOT NULL,
	[StatePartnerType] VARCHAR(50) NOT NULL
)