CREATE TABLE [dbo].[PE_SM_AllocationInput](
	[PEFundRunID] BIGINT NOT NULL,
	[ClientID] INT NOT NULL,
	[InvestmentID] INT NOT NULL,
	[LineTypeID] INT NOT NULL,
	[StateID] INT NOT NULL,
	[StateLineID] INT NOT NULL,
	[InitialAmount] FLOAT NULL
)