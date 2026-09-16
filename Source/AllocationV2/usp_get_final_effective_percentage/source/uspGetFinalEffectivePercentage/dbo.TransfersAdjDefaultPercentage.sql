CREATE TABLE [dbo].[TransfersAdjDefaultPercentage](
	[RunID] BIGINT NOT NULL,
	[ClientID] BIGINT NOT NULL,
	[EntityID] INT NOT NULL,
	[PartnerNumber] VARCHAR(50) NOT NULL,
	[ShareClass] VARCHAR(200) NULL,
	[TransferPartnerNumber] VARCHAR(50) NULL,
	[TransferAdjPercent] FLOAT NULL,
	[EndingCommitmentPercent] FLOAT NULL,
	[TransferDate] DATETIME NULL,
	[TransferDirection] VARCHAR(5) NULL,
	[BeginningPercentUsage] FLOAT NULL,
	[EffectivePercent] FLOAT NULL,
	[AllocationComplete] VARCHAR(5) NULL,
	[IsEODTransfer] BIT NULL
)