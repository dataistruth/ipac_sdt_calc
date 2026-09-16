CREATE TABLE [dbo].[K1GPartnerTypes](
	[PartnerTypeID] INT IDENTITY(1,1) NOT NULL,
	[PartnerTypeDesc] VARCHAR(200) NULL,
	[StatePartnerTypeDesc] VARCHAR(200) NULL
)

ALTER TABLE [dbo].[K1GPartnerTypes] ADD PRIMARY KEY ([PartnerTypeID])