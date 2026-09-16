CREATE TABLE [dbo].[EntityConfigurations](
	[ConfigID] INT IDENTITY(1,1) NOT NULL,
	[EntityId] INT NOT NULL,
	[Is16bchiLineCalcEnabled] BIT NULL,
	[IsOrdinaryIncomeCalSet] BIT NULL,
	[ApplyToAllBasis] BIT NULL,
	[isWacOverride] BIT NULL,
	[IsUseResidualGain] BIT NULL,
	[IsGainSameasLoss] BIT NULL,
	[IsUseResidualLoss] BIT NULL,
	[ExcessGainsSelected] VARCHAR(100) NULL,
	[ResidualLoss] VARCHAR(100) NULL,
	[isAdjustmentOverride] BIT NULL,
	[IsAlternativeStuffing] BIT NULL,
	[IsGain731a] BIT NULL,
	[IsParallelUpperTierCalcEnabled] BIT NULL,
	[IsCalcCleanupDisabled] BIT NULL,
	[UsePYData] BIT NULL,
	[UsePYFinal] BIT NULL,
	[EnableLegacyMK1Import] BIT NULL,
	[IsEntityLevelCustReptToDotNet] BIT NULL,
	[704cAllocationTypeID] INT NULL,
	[EnableCustomPeriodsforWAC] BIT NULL
)

ALTER TABLE [dbo].[EntityConfigurations] ADD PRIMARY KEY ([ConfigID])