CREATE TABLE [dbo].[PE_AllocationInput](
	[PEFundRunID] BIGINT NOT NULL,
	[ClientID] INT NOT NULL,
	[InvestmentID] INT NOT NULL,
	[LineTypeID] INT NOT NULL,
	[LineID] INT NOT NULL,
	[QuicklinkID] INT NULL,
	[InitialAmount] FLOAT NULL
)