# Column Reference

Auto-generated from manifest CREATE TABLE scripts.
Generated: 2026-05-04 13:52
Tables: 83 | BIT columns: 142 | DECIMAL columns: 13


## BIT columns (CRITICAL — use == True/False, NEVER == 1/0)

These columns are BOOLEAN in Spark. Apply type-safety rules:

```
dbo.BookEffective_Snapshot.IsExcludefromTransfer
dbo.CostPercentage_704c_Snapshot.GPPartnerReceivingCarry
dbo.CustomImportDetail.DoNotSuppressBlankRows
dbo.CustomImportDetail.EnableMapping
dbo.CustomImportDetail.IsCustomFootnote
dbo.CustomImportDetail.IsLinkToXtract
dbo.CustomImportDetail.IsLookThroughImport
dbo.ENU_AllocationLogic.IsDisplay
dbo.ENU_AttributeType.IsHidden
dbo.Entity.AcceptsInvestorMoneySec1471
dbo.Entity.DirectOwnership
dbo.Entity.EquityVotingRights
dbo.Entity.Filings5471
dbo.Entity.Filings8621
dbo.Entity.Filings8858
dbo.Entity.Filings8865
dbo.Entity.ForeignFundOwnershipBit
dbo.Entity.Form8832Election
dbo.Entity.FundsOwnershipPercentageBit
dbo.Entity.HideTaxCapitalDetails
dbo.Entity.IncludeInDebtAllocation
dbo.Entity.IsActive
dbo.Entity.IsCFC
dbo.Entity.IsDomesticBlocker
dbo.Entity.IsExternal
dbo.Entity.IsFeeder
dbo.Entity.IsForeign
dbo.Entity.IsHolding
dbo.Entity.IsInCarry
dbo.Entity.IsIssueK1
dbo.Entity.IsPFIC
dbo.Entity.IsPTP
dbo.Entity.IsQualifiedForeignCorporation
dbo.Entity.IsSuspendedLossDisabled
dbo.Entity.SecondaryInvestment
dbo.Entity.The926Filings
dbo.Entity.TransferThreshhold
dbo.Entity.USInvestorsOwnershipPercentageBit
dbo.Entity.WFPAgreement
dbo.EntityConfigurations.ApplyToAllBasis
dbo.EntityConfigurations.EnableCustomPeriodsforWAC
dbo.EntityConfigurations.EnableLegacyMK1Import
dbo.EntityConfigurations.Is16bchiLineCalcEnabled
dbo.EntityConfigurations.IsAlternativeStuffing
dbo.EntityConfigurations.IsCalcCleanupDisabled
dbo.EntityConfigurations.IsEntityLevelCustReptToDotNet
dbo.EntityConfigurations.IsGain731a
dbo.EntityConfigurations.IsGainSameasLoss
dbo.EntityConfigurations.IsOrdinaryIncomeCalSet
dbo.EntityConfigurations.IsParallelUpperTierCalcEnabled
dbo.EntityConfigurations.IsUseResidualGain
dbo.EntityConfigurations.IsUseResidualLoss
dbo.EntityConfigurations.UsePYData
dbo.EntityConfigurations.UsePYFinal
dbo.EntityConfigurations.isAdjustmentOverride
dbo.EntityConfigurations.isWacOverride
dbo.Enu_Event.IsDataFeedEvent
dbo.Enu_Event.IsWorkflowEvent
dbo.Form199ALineItem.IsActive
dbo.Form199ALineItem.IsAllocated
dbo.Form199ALineItem.IsConfigurable
dbo.Form199ALineItem.IsSpeciallyAllocated
dbo.Form199ALineItem.isActiveADJReclassImport
dbo.Form8865LineItem.IsActive
dbo.Form8865LineItem.IsAllocated
dbo.Form8865LineItem.IsConfigurable
dbo.Form8865LineItem.IsSpeciallyAllocated
dbo.Form8886LineItem.IsActive
dbo.Form8886LineItem.IsAllocated
dbo.Form8886LineItem.IsConfigurable
dbo.Form8886LineItem.IsSpeciallyAllocated
dbo.Form8886LineItem.isActiveADJReclassImport
dbo.Form926LineItem.IsActive
dbo.Form926LineItem.IsAllocated
dbo.Form926LineItem.IsConfigurable
dbo.Form926LineItem.IsSpeciallyAllocated
dbo.Form926LineItem.IsXtractOverride
dbo.Form926LineItem.isActiveADJReclassImport
dbo.GlobalMenu.AllowStandardizationUpdate
dbo.GlobalMenu.IsHedge
dbo.GlobalMenu.IsStandardizationEnabled
dbo.GlobalMenu.IsTechConfig
dbo.K1LineItem.DefaultDispositionGainLoss
dbo.K1LineItem.IncludeLookthroughData
dbo.K1LineItem.Is16BCalc
dbo.K1LineItem.Is16CCalc
dbo.K1LineItem.Is16HCalc
dbo.K1LineItem.IsActive
dbo.K1LineItem.IsFDAPLineClassification
dbo.K1LineItem.IsGPFeeAllocated
dbo.K1LineItem.IsGain731a
dbo.K1LineItem.IsK3PYLineItem
dbo.K1LineItem.IsM1Adjustment
dbo.K1LineItem.IsReadOnly
dbo.K1LineItem.IsRestrictDataFeedbyEntInvOverride
dbo.K1LineItem.IsRestrictOverrideXtract
dbo.K1LineItem.IsTaxHoldback
dbo.K1LineItem.IsTransactionDate
dbo.K1LineItem.IsTransfersAdjusted
dbo.K1LineItem.IsVisible
dbo.K1LineItem.IsVisiblePriorYear
dbo.K1LineItem.isGrossOverride
dbo.K1Package.IsAnnualized
dbo.K1Package.IsFullRedemption
dbo.MappingLineItem.IsActive
dbo.PFICFootnoteLineItem.IsActive
dbo.PFICFootnoteLineItem.IsAllocated
dbo.PFICFootnoteLineItem.IsConfigurable
dbo.PFICFootnoteLineItem.IsPartVAllocated
dbo.PFICFootnoteLineItem.IsSpeciallyAllocated
dbo.PFICFootnoteLineItem.IsXtractOverride
dbo.PFICFootnoteLineItem.Unique_Identifier
dbo.PFICFootnoteLineItem.isActiveADJReclassImport
dbo.PFICFootnotePackage.IsDerivedPFIC
dbo.Partner_Snapshot.AmendedK1
dbo.Partner_Snapshot.CarryPaying
dbo.Partner_Snapshot.FinalK1
dbo.Partner_Snapshot.ForceFileComposite
dbo.Partner_Snapshot.IsGeneralPartnership
dbo.Partner_Snapshot.IsSidePocketFlowUpPartner
dbo.Partner_Snapshot.LockPartner
dbo.Partner_Snapshot.OwnedthroughDisregardedEntity
dbo.Partner_Snapshot.PayStuffing
dbo.Partner_Snapshot.SpecialProvisionforComp
dbo.Partner_Snapshot.TaxFormSigned
dbo.Partner_Snapshot.isComposite
dbo.Partner_Snapshot.isTaxExempt
dbo.SM_StateLines.HasComposite
dbo.SM_StateLines.HasWithholding
dbo.SM_StateLines.IsInterestIncome
dbo.SM_StateLines.IsNYCNOLDeduction
dbo.SM_StateLines.IsPETaxCredit
dbo.SM_StateLines.IsRestrictDataFeedbyEntInvOverride
dbo.SM_StateLines.IsRestrictDataOverride
dbo.SM_StateLines.IsStateLineThreshold
dbo.SM_StateLines.IsStateSpecificLine
dbo.SM_StateLines.IsTaxHoldback
dbo.SM_StateLines.UsePYData
dbo.TransfersAdjCostDefaultPercentage.IsEODTransfer
dbo.TransfersAdjDefaultPercentage.IsEODTransfer
dbo.WorkFlow.IsLastStep
dbo.WorkFlowChain.IncludeInCalc
```

**Rules for these columns:**
- DataFrame API: `F.col("IsXxx") == True` not `== 1`
- Raw SQL: `SET IsXxx = true` not `= 1`
- Never use `_ns0()` on any of these — use `F.coalesce(F.col("x").cast("int"), F.lit(0))` inline
- When collected to Python: value is `True`/`False`, compare with `if val:` not `if val == 1:`

## DECIMAL columns (re-cast after F.round())

| Table | Column | Precision |
|---|---|---|
| dbo.CostPercentage_704c_Snapshot | BookIncomeQ1 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | BookIncomeQ2 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | BookIncomeQ3 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | BookIncomeQ4 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | DisparityAdjustmentQ1 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | DisparityAdjustmentQ2 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | DisparityAdjustmentQ3 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | DisparityAdjustmentQ4 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | SpecialAllocationAdjustmentQ1 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | SpecialAllocationAdjustmentQ2 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | SpecialAllocationAdjustmentQ3 | DECIMAL(25,0) |
| dbo.CostPercentage_704c_Snapshot | SpecialAllocationAdjustmentQ4 | DECIMAL(25,0) |
| dbo.Partner_Snapshot | Commitment | NUMERIC(20,10) |

## Table schemas

### dbo.AllocationInput

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| EntityID | INT | NOT NULL |
| LineTypeID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| QuicklinkID | INT | NULL |
| Amount704b | FLOAT | NULL |
| CategoryID | INT | NULL |
| PeriodID | INT | NULL |
| LineCode | VARCHAR(100) | NULL |
| ParentEntityID | INT | NULL |
| SuperParentEntityID | INT | NULL |
| AdjustmentTypeID | INT | NULL |
| Tag | VARCHAR(5000) | NULL |
| TrackingKey | VARCHAR(4000) | NULL |
| OriginalParentEntityID | INT | NULL |
| SchID | INT | NULL |
| FlowUpPartner | VARCHAR(50) | NULL |

### dbo.AllocationLog

| Column | Type | Nullable |
|---|---|---|
| LogID | INT IDENTITY | NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| Category | VARCHAR(50) | NULL |
| ProcessName | VARCHAR(100) | NULL |
| LogDescription | VARCHAR(1000) | NULL |
| RunID | INT | NULL |
| StartDate | DATETIME | NOT NULL |
| EndDate | DATETIME | NULL |

### dbo.AllocationPercentage704c

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| InvestmentID | INT | NULL |
| PartnerNumber | VARCHAR(50) | NOT NULL |
| OrdinaryPercentage | FLOAT | NULL |
| CapitalPercentage | FLOAT | NULL |
| CapitalGainPercentage | FLOAT | NULL |
| CapitalLossPercentage | FLOAT | NULL |
| Quarter | VARCHAR(50) | NULL |
| AllocationTypeId | INT | NULL |
| 704cAllocationTypeID | INT | NULL |
| Underlyingtype | INT | NULL |
| TrackingKey | VARCHAR(4000) | NULL |

### dbo.AllocationRun

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT IDENTITY | NULL |
| RunDate | DATETIME | NULL |
| ClientID | BIGINT | NOT NULL |
| EntityID | INT | NOT NULL |
| LowerTierEntityID | INT | NULL |
| TaxPeriodID | INT | NOT NULL |
| QueueID | BIGINT | NULL |
| YearlyWorkflowID | INT | NULL |
| PartnerWorkflowID | INT | NULL |
| K1WorkflowID | INT | NULL |
| RunStatus | VARCHAR(50) | NULL |
| StatusDesc | VARCHAR(MAX) | NULL |
| SPAWorkflowID | INT | NULL |
| SidePocketWorkflowID | INT | NULL |
| PartnerTransactionID | INT | NULL |
| RunType | VARCHAR(50) | NULL |
| PhaseID | INT | NULL |
| SPA704bWorkflowID | INT | NULL |
| StackTrace | VARCHAR(MAX) | NULL |
| ForeignCurrencyRateTransactionID | INT | NULL |
| K1InternationalWorkflowID | INT | NULL |
| StateSPAWorkflowID | BIGINT | NULL |
| RunEndDate | DATETIME | NULL |
| CostWorkflowID | BIGINT | NULL |
| CARWorkflowID | BIGINT | NULL |
| DARWorkflowID | BIGINT | NULL |
| PGWorkflowID | BIGINT | NULL |
| OffsetWorkflowID | INT | NULL |
| TransferWorkflowID | INT | NULL |
| RoundingOverrideWorkflowID | INT | NULL |
| ExcessPartnerWithholdingWorkflowID | INT | NULL |
| K3ValidationStatus | VARCHAR(20) | NULL |
| StateRoundingOverrideTransactionID | INT | NULL |
| VTNRCompositePartnerCnt | INT | NULL |

### dbo.AllocationRunErrors

| Column | Type | Nullable |
|---|---|---|
| ErrorID | INT IDENTITY | NULL |
| RunID | BIGINT | NULL |
| EntityID | INT | NULL |
| LineTypeID | INT | NULL |
| LineID | INT | NULL |
| ErrorMessage | VARCHAR(MAX) | NULL |
| LogID | INT | NULL |
| ErrororWarning | VARCHAR(20) | NULL |
| ErrorType | VARCHAR(20) | NULL |

### dbo.AssetClassOverrideImportData

| Column | Type | Nullable |
|---|---|---|
| TransactionID | INT | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| EntityID | INT | NULL |
| UnderlyingID | INT | NULL |
| TaxableIncome | FLOAT | NULL |
| AssetClassID | INT | NULL |
| OverrideAssetClassID | INT | NULL |
| TrackingKey | VARCHAR(4000) | NULL |

### dbo.AtRiskFlowup

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| FlowupEntityID | INT | NOT NULL |
| SourceEntityID | INT | NOT NULL |
| AtRiskID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| TextValue | VARCHAR(100) | NULL |

### dbo.AtRiskPackage

| Column | Type | Nullable |
|---|---|---|
| AtRiskID | INT IDENTITY | NULL |
| K1PackageID | INT | NULL |
| ClientID | BIGINT | NULL |
| TaxPeriodID | INT | NULL |
| AtRiskName | VARCHAR(200) | NULL |

### dbo.BasisOverrideImportData

| Column | Type | Nullable |
|---|---|---|
| UpperTierEntityID | INT | NOT NULL |
| LowerTierEntityID | INT | NULL |
| LineID | VARCHAR(2000) | NULL |
| Labeltype | VARCHAR(500) | NULL |
| Value | VARCHAR(200) | NULL |
| ReportingBasisID | INT | NULL |
| Box | VARCHAR(500) | NULL |
| LineDescription | VARCHAR(5000) | NULL |
| LineTypeId | VARCHAR(500) | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| PK_ID | INT IDENTITY | NULL |

### dbo.BookEffective_Snapshot

| Column | Type | Nullable |
|---|---|---|
| WorkflowID | INT | NULL |
| TransactionID | INT | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| EntityID | INT | NULL |
| UnderlyingEntityID | INT | NULL |
| SourceID | INT | NULL |
| FootNoteID | INT | NULL |
| LineID | INT | NULL |
| AllocationTypeID | INT | NULL |
| AdjustmentAllocationTypeID | INT | NULL |
| Tag | VARCHAR(500) | NULL |
| TrackingKey | VARCHAR(4000) | NULL |
| SourceEntityID | INT | NULL |
| IsExcludefromTransfer | **BIT** (→ BOOLEAN) | NULL |
| DealID | VARCHAR(500) | NULL |

### dbo.BoxjklLineItem

| Column | Type | Nullable |
|---|---|---|
| LineID | INT IDENTITY | NULL |
| LineNumber | VARCHAR(10) | NULL |
| Box | VARCHAR(20) | NULL |
| LineDescription | VARCHAR(100) | NULL |
| TaxPeriodID | INT | NULL |
| Comment | VARCHAR(MAX) | NULL |
| AllocationType | VARCHAR(150) | NULL |
| DisplayOrder | INT | NOT NULL |
| LineDataType | VARCHAR(20) | NULL |
| ClientID | INT | NOT NULL |
| CreateBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |

### dbo.CostPercentage_704c_Snapshot

| Column | Type | Nullable |
|---|---|---|
| WorkflowID | INT | NULL |
| TransactionID | INT | NULL |
| CostPercentageID | INT | NULL |
| 704cAllocationTypeID | INT | NULL |
| GaapBegCapAccount | FLOAT | NULL |
| ReceivedIncentive | INT | NULL |
| JanEquity | FLOAT | NULL |
| FebEquity | FLOAT | NULL |
| MarEquity | FLOAT | NULL |
| AprEquity | FLOAT | NULL |
| MayEquity | FLOAT | NULL |
| JunEquity | FLOAT | NULL |
| JulEquity | FLOAT | NULL |
| AugEquity | FLOAT | NULL |
| SepEquity | FLOAT | NULL |
| OctEquity | FLOAT | NULL |
| NovEquity | FLOAT | NULL |
| DecEquity | FLOAT | NULL |
| DecEndingEquity | FLOAT | NULL |
| CommitmentPercentage | FLOAT | NULL |
| IncentiveReallocPercent | FLOAT | NULL |
| GaapEndCapAccount | FLOAT | NULL |
| IncPartnerResOverride | INT | NULL |
| BookIncome | FLOAT | NULL |
| TotalMgmtFees | FLOAT | NULL |
| HotIssueGainLoss | FLOAT | NULL |
| 704cGainLoss | FLOAT | NULL |
| GuaranteedPaymentsServices | FLOAT | NULL |
| GuaranteedPaymentsCapital | FLOAT | NULL |
| UsWithholding | FLOAT | NULL |
| IncentiveFee | FLOAT | NULL |
| ForeignTaxes | FLOAT | NULL |
| SpecialAllocation1 | FLOAT | NULL |
| SpecialAllocation2 | FLOAT | NULL |
| BegRevalAccount | FLOAT | NULL |
| RevalAdjustment | FLOAT | NULL |
| TransferCode | VARCHAR(50) | NULL |
| TransferPercent | FLOAT | NULL |
| RevalTransfer | FLOAT | NULL |
| PercentWithdrawal | FLOAT | NULL |
| PYRedemptionPayables | FLOAT | NULL |
| CYRedemptionPayables | FLOAT | NULL |
| ResidualCustomGainPercent | VARCHAR(50) | NULL |
| ResidualCustomLossPercent | VARCHAR(50) | NULL |
| TotalBookCshDist | FLOAT | NULL |
| TotalBookPrDist | FLOAT | NULL |
| TotalBookCshContri | FLOAT | NULL |
| TotalBookPrContri | FLOAT | NULL |
| TaxBegCapAccount | FLOAT | NULL |
| TotalTaxCshContri | FLOAT | NULL |
| TotalTaxPrContri | FLOAT | NULL |
| OtherIncrease | FLOAT | NULL |
| TotalTaxCshDist | FLOAT | NULL |
| TotalTaxPrptDist | FLOAT | NULL |
| OtherDecrease | FLOAT | NULL |
| TransferIn | FLOAT | NULL |
| TransferOut | FLOAT | NULL |
| GPPartnerReceivingCarry | **BIT** (→ BOOLEAN) | NULL |
| BegForward704c | FLOAT | NULL |
| EndingRevalTransferAmt | FLOAT | NULL |
| BookTransferIn | FLOAT | NULL |
| BookTransferOut | FLOAT | NULL |
| BookIncomeQ1 | DECIMAL(25,0) | NULL |
| BookIncomeQ2 | DECIMAL(25,0) | NULL |
| BookIncomeQ3 | DECIMAL(25,0) | NULL |
| BookIncomeQ4 | DECIMAL(25,0) | NULL |
| PercentWithdrawQ1 | FLOAT | NULL |
| PercentWithdrawQ2 | FLOAT | NULL |
| PercentWithdrawQ3 | FLOAT | NULL |
| PercentWithdrawQ4 | FLOAT | NULL |
| DisparityAdjustmentQ1 | DECIMAL(25,0) | NULL |
| DisparityAdjustmentQ2 | DECIMAL(25,0) | NULL |
| DisparityAdjustmentQ3 | DECIMAL(25,0) | NULL |
| DisparityAdjustmentQ4 | DECIMAL(25,0) | NULL |
| SpecialAllocationAdjustmentQ1 | DECIMAL(25,0) | NULL |
| SpecialAllocationAdjustmentQ2 | DECIMAL(25,0) | NULL |
| SpecialAllocationAdjustmentQ3 | DECIMAL(25,0) | NULL |
| SpecialAllocationAdjustmentQ4 | DECIMAL(25,0) | NULL |

### dbo.CostPercentage_Snapshot

| Column | Type | Nullable |
|---|---|---|
| WorkFlowID | INT | NULL |
| TransactionID | INT | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| EntityId | INT | NULL |
| InvestmentID | INT | NULL |
| PartnerNumber | VARCHAR(200) | NULL |
| Quarter | VARCHAR(50) | NULL |
| CommitmentPercent | FLOAT | NULL |
| AllocationTypeId | INT | NULL |
| Tag | VARCHAR(5000) | NULL |
| TrackingKey | VARCHAR(4000) | NULL |
| Underlyingtype | INT | NULL |
| AllocatedAmount | FLOAT | NULL |
| CostPercentageId | INT IDENTITY | NULL |
| DealID | VARCHAR(500) | NULL |

### dbo.CustomFootNotePackage

| Column | Type | Nullable |
|---|---|---|
| CustomFootnoteID | INT IDENTITY | NULL |
| K1PackageID | INT | NOT NULL |
| RowID | INT | NOT NULL |
| RegisterTypeID | INT | NOT NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| TransactionID | INT | NOT NULL |

### dbo.CustomFootnoteFlowup

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| FlowupEntityID | INT | NOT NULL |
| SourceEntityID | INT | NOT NULL |
| CustomFootnoteID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| LineTypeID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| TextValue | VARCHAR(100) | NULL |

### dbo.CustomImportDetail

| Column | Type | Nullable |
|---|---|---|
| CustomImportID | INT IDENTITY | NULL |
| GlobalMenuID | INT | NULL |
| ImportName | VARCHAR(100) | NULL |
| ImportLevel | VARCHAR(20) | NULL |
| ImportAt | VARCHAR(20) | NULL |
| ModifiedBy | VARCHAR(50) | NULL |
| ModifiedDate | DATETIME | NULL |
| EnableMapping | **BIT** (→ BOOLEAN) | NULL |
| IsLookThroughImport | **BIT** (→ BOOLEAN) | NULL |
| IsWindCreditImport | INT | NULL |
| IsCustomFootnote | **BIT** (→ BOOLEAN) | NULL |
| DoNotSuppressBlankRows | **BIT** (→ BOOLEAN) | NULL |
| CustomTemplatePath | VARCHAR(1000) | NULL |
| StandardSiteClientID | INT | NULL |
| StandardSiteIdentifier | INT | NULL |
| IsLinkToXtract | **BIT** (→ BOOLEAN) | NULL |

### dbo.DefaultAllocationRuleSetup

| Column | Type | Nullable |
|---|---|---|
| TransactionID | INT | NOT NULL |
| RuleID | INT | NOT NULL |
| AllocationByID | INT | NOT NULL |
| UnderlyingTypeID | INT | NOT NULL |
| RuleTypeID | INT | NOT NULL |
| RuleGroupID | INT | NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| AllocationPercentageTypeID | INT | NOT NULL |

### dbo.ENU_704cAllocationLogic

| Column | Type | Nullable |
|---|---|---|
| 704cAllocationTypeID | INT IDENTITY | NULL |
| GlobalMenuID | INT | NULL |
| 704cAllocationTypeName | VARCHAR(100) | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |

### dbo.ENU_AllocationBy

| Column | Type | Nullable |
|---|---|---|
| AllocationByID | INT IDENTITY | NULL |
| AllocationBy | VARCHAR(30) | NULL |
| DisplayOrder | INT | NULL |

### dbo.ENU_AllocationLogic

| Column | Type | Nullable |
|---|---|---|
| AllocationTypeID | INT IDENTITY | NULL |
| AllocationTypeName | VARCHAR(100) | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NOT NULL |
| IsDisplay | **BIT** (→ BOOLEAN) | NULL |

### dbo.ENU_AllocationPercentageType

| Column | Type | Nullable |
|---|---|---|
| AllocationPercentageTypeID | INT IDENTITY | NULL |
| AllocationPercentageType | VARCHAR(30) | NULL |
| DisplayOrder | INT | NULL |

### dbo.ENU_AttributeType

| Column | Type | Nullable |
|---|---|---|
| AttributeID | INT IDENTITY | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| AttributeType | VARCHAR(50) | NULL |
| AttributeValue | VARCHAR(200) | NULL |
| DisplayOrder | INT | NULL |
| IsHidden | **BIT** (→ BOOLEAN) | NULL |

### dbo.ENU_DF_DataList

| Column | Type | Nullable |
|---|---|---|
| ID | INT IDENTITY | NULL |
| Category | VARCHAR(100) | NULL |
| LookUpData | VARCHAR(200) | NULL |
| LookUpValue | VARCHAR(500) | NULL |
| DisplayOrder | INT | NULL |
| Comments | VARCHAR(200) | NULL |

### dbo.ENU_EntityType

| Column | Type | Nullable |
|---|---|---|
| EntityTypeID | INT IDENTITY | NULL |
| EntityTypeName | VARCHAR(50) | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NOT NULL |

### dbo.ENU_GlobalMenuGroup

| Column | Type | Nullable |
|---|---|---|
| GlobalMenuGroupID | INT IDENTITY | NULL |
| GroupName | VARCHAR(50) | NOT NULL |
| GroupConfig | VARCHAR(1) | NULL |
| BOEParentId | INT | NULL |

### dbo.ENU_LineType

| Column | Type | Nullable |
|---|---|---|
| LineTypeID | INT IDENTITY | NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| LineType | VARCHAR(50) | NOT NULL |
| DisplayOrder | INT | NULL |

### dbo.ENU_MappingSource

| Column | Type | Nullable |
|---|---|---|
| SourceID | INT IDENTITY | NULL |
| ClientID | BIGINT | NOT NULL |
| SourceName | VARCHAR(100) | NOT NULL |

### dbo.ENU_RuleGroup

| Column | Type | Nullable |
|---|---|---|
| RuleGroupID | INT IDENTITY | NULL |
| RuleGroupName | VARCHAR(100) | NULL |
| DisplayOrder | INT | NULL |

### dbo.ENU_RuleType

| Column | Type | Nullable |
|---|---|---|
| RuleTypeID | INT IDENTITY | NULL |
| RuleType | VARCHAR(30) | NULL |
| DisplayOrder | INT | NULL |

### dbo.ENU_UnderlyingType

| Column | Type | Nullable |
|---|---|---|
| UnderlyingTypeID | INT IDENTITY | NULL |
| UnderlyingType | VARCHAR(50) | NULL |
| DisplayOrder | INT | NULL |

### dbo.Entity

| Column | Type | Nullable |
|---|---|---|
| EntityID | INT IDENTITY | NULL |
| EntityIdentification | VARCHAR(50) | NULL |
| EIN | VARCHAR(20) | NULL |
| DisplayName | VARCHAR(200) | NULL |
| EntityName1 | VARCHAR(100) | NULL |
| EntityName2 | VARCHAR(100) | NULL |
| EntityName3 | VARCHAR(100) | NULL |
| Address1 | VARCHAR(100) | NULL |
| Address2 | VARCHAR(100) | NULL |
| City | VARCHAR(100) | NULL |
| State | VARCHAR(10) | NULL |
| Zip | VARCHAR(50) | NULL |
| Country | VARCHAR(50) | NULL |
| FundOrInvestmentID | INT | NULL |
| ClientID | INT | NULL |
| InvesttransID | VARCHAR(50) | NULL |
| IsIssueK1 | **BIT** (→ BOOLEAN) | NULL |
| UpdateDate | DATETIME | NULL |
| IsPTP | **BIT** (→ BOOLEAN) | NULL |
| IsForeign | **BIT** (→ BOOLEAN) | NULL |
| IsCFC | **BIT** (→ BOOLEAN) | NULL |
| TaxBasisTypeID | INT | NULL |
| IRSServiceCenter | VARCHAR(100) | NULL |
| PrimaryActivity | VARCHAR(20) | NULL |
| CalendarOrFiscalYr | VARCHAR(50) | NULL |
| FiscalTaxBeginning | VARCHAR(50) | NULL |
| FiscalTaxEnding | VARCHAR(50) | NULL |
| DateFormation | VARCHAR(25) | NULL |
| EntityTypeB | VARCHAR(150) | NULL |
| CountryCode | VARCHAR(50) | NULL |
| IsExternal | **BIT** (→ BOOLEAN) | NULL |
| IsInCarry | **BIT** (→ BOOLEAN) | NULL |
| TaxPeriodID | INT | NOT NULL |
| AllocationTypeID | INT | NULL |
| TaxClassID | INT | NULL |
| Province | VARCHAR(50) | NULL |
| TransactionID | INT | NULL |
| ContactName1 | VARCHAR(35) | NULL |
| ContactName2 | VARCHAR(35) | NULL |
| ContactTitle | VARCHAR(35) | NULL |
| ContactCompany | VARCHAR(50) | NULL |
| ContactAddress1 | VARCHAR(35) | NULL |
| ContactAddress2 | VARCHAR(35) | NULL |
| ContactCity | VARCHAR(100) | NULL |
| ContactState | VARCHAR(50) | NULL |
| ContactProvince | VARCHAR(35) | NULL |
| ContactPostalCode | VARCHAR(20) | NULL |
| ContactCountry | VARCHAR(20) | NULL |
| ContactCountryCode | VARCHAR(5) | NULL |
| ContactPhone | VARCHAR(20) | NULL |
| ContactFax | VARCHAR(20) | NULL |
| ContactEmail | VARCHAR(30) | NULL |
| AssetClassID | INT | NULL |
| PrimaryOrTrueUp | VARCHAR(50) | NULL |
| IncludeInDebtAllocation | **BIT** (→ BOOLEAN) | NULL |
| DateK1Expected | DATETIME | NULL |
| CurrencyCode | VARCHAR(50) | NULL |
| GeographyClassID | INT | NULL |
| StrategyClassID | INT | NULL |
| HoldingVehicle | VARCHAR(100) | NULL |
| BusinessUnitId | INT | NULL |
| FundGroup | VARCHAR(100) | NULL |
| GPAdvisor | VARCHAR(100) | NULL |
| LegalAddress | VARCHAR(100) | NULL |
| CountryofEstablishment | VARCHAR(100) | NULL |
| LegalEntityType | VARCHAR(100) | NULL |
| EntityDescription | VARCHAR(100) | NULL |
| UnderlyingFund | VARCHAR(100) | NULL |
| AcceptsInvestorMoneySec1471 | **BIT** (→ BOOLEAN) | NULL |
| ClassificationID | INT | NULL |
| FFIID | VARCHAR(100) | NULL |
| FFIEIN | VARCHAR(100) | NULL |
| ForeignEIN | VARCHAR(100) | NULL |
| WFPAgreement | **BIT** (→ BOOLEAN) | NULL |
| TaxFormTypeID | INT | NULL |
| TaxReturnsTypeID | INT | NULL |
| StateReturns | VARCHAR(100) | NULL |
| DueDates | DATETIME | NULL |
| IsActive | **BIT** (→ BOOLEAN) | NULL |
| Filings5471 | **BIT** (→ BOOLEAN) | NULL |
| Filings8621 | **BIT** (→ BOOLEAN) | NULL |
| Filings8858 | **BIT** (→ BOOLEAN) | NULL |
| Filings8865 | **BIT** (→ BOOLEAN) | NULL |
| FiledTypeID | INT | NULL |
| AOGEntity | VARCHAR(100) | NULL |
| TaxableIncomeWorkbook | VARCHAR(100) | NULL |
| TaxPreperer | VARCHAR(100) | NULL |
| FATCAStatusID | INT | NULL |
| FATCAWithholdingRate | FLOAT | NULL |
| AssetType | VARCHAR(100) | NULL |
| StartTaxPeriodId | VARCHAR(100) | NULL |
| EndTaxPeriodId | VARCHAR(100) | NULL |
| TransferThreshhold | **BIT** (→ BOOLEAN) | NULL |
| FundsOwnershipPercentageBit | **BIT** (→ BOOLEAN) | NULL |
| USInvestorsOwnershipPercentageBit | **BIT** (→ BOOLEAN) | NULL |
| ForeignFundOwnershipBit | **BIT** (→ BOOLEAN) | NULL |
| ClassOfEquityOwnedByUAW | INT | NULL |
| EquityVotingRights | **BIT** (→ BOOLEAN) | NULL |
| Form8832Election | **BIT** (→ BOOLEAN) | NULL |
| DirectOwnership | **BIT** (→ BOOLEAN) | NULL |
| FundsOwnershipPercentage | INT | NULL |
| The926Filings | **BIT** (→ BOOLEAN) | NULL |
| Custom01 | VARCHAR(300) | NULL |
| Custom02 | VARCHAR(300) | NULL |
| Custom03 | VARCHAR(300) | NULL |
| Custom04 | VARCHAR(300) | NULL |
| Custom05 | VARCHAR(300) | NULL |
| Custom06 | VARCHAR(300) | NULL |
| Custom07 | VARCHAR(300) | NULL |
| Custom08 | VARCHAR(50) | NULL |
| Custom09 | VARCHAR(50) | NULL |
| Custom10 | VARCHAR(50) | NULL |
| Custom11 | VARCHAR(50) | NULL |
| Custom12 | VARCHAR(50) | NULL |
| Custom13 | VARCHAR(50) | NULL |
| Custom14 | VARCHAR(50) | NULL |
| Custom15 | VARCHAR(50) | NULL |
| Custom16 | VARCHAR(50) | NULL |
| Custom17 | VARCHAR(50) | NULL |
| Custom18 | VARCHAR(50) | NULL |
| Custom19 | VARCHAR(50) | NULL |
| Custom20 | VARCHAR(50) | NULL |
| ConfigureStateThresholds | INT | NOT NULL |
| ApportionmentFlowUp | INT | NOT NULL |
| LegalInvestmentName | VARCHAR(200) | NULL |
| ReferenceId | VARCHAR(50) | NULL |
| ForeignCharacterization | VARCHAR(50) | NULL |
| PEAttribute | INT | NULL |
| TaxDocument | VARCHAR(200) | NULL |
| EntitySubType | INT | NULL |
| IsPFIC | **BIT** (→ BOOLEAN) | NULL |
| DealStatus | VARCHAR(250) | NULL |
| IsDomesticBlocker | **BIT** (→ BOOLEAN) | NULL |
| IsHolding | **BIT** (→ BOOLEAN) | NULL |
| IsFeeder | **BIT** (→ BOOLEAN) | NULL |
| OldAssetClassID | INT | NULL |
| InvestmentType | INT | NULL |
| ShortName | VARCHAR(300) | NULL |
| IsSuspendedLossDisabled | **BIT** (→ BOOLEAN) | NULL |
| HideTaxCapitalDetails | **BIT** (→ BOOLEAN) | NULL |
| ValueOfShareatYE | FLOAT | NULL |
| EntityCategory | VARCHAR(100) | NULL |
| LowerTierTieringStatus | VARCHAR(100) | NULL |
| DateOfAcquisition | DATETIME | NULL |
| FunctionalCurrencyPerK1 | VARCHAR(100) | NULL |
| FunctionalCurrencyBasisTracking | VARCHAR(100) | NULL |
| IsQualifiedForeignCorporation | **BIT** (→ BOOLEAN) | NULL |
| XtractInvestmentID | VARCHAR(50) | NULL |
| SecondaryInvestment | **BIT** (→ BOOLEAN) | NULL |

### dbo.EntityAllocationRule_Snapshot

| Column | Type | Nullable |
|---|---|---|
| WorkflowID | INT | NULL |
| TransactionID | INT | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| EntityID | INT | NULL |
| LineID | INT | NULL |
| DefaultAllocationRuleID | INT | NULL |
| UpdatedAllocationRuleID | INT | NULL |

### dbo.EntityConfigurations

| Column | Type | Nullable |
|---|---|---|
| ConfigID | INT IDENTITY | NULL |
| EntityId | INT | NOT NULL |
| Is16bchiLineCalcEnabled | **BIT** (→ BOOLEAN) | NULL |
| IsOrdinaryIncomeCalSet | **BIT** (→ BOOLEAN) | NULL |
| ApplyToAllBasis | **BIT** (→ BOOLEAN) | NULL |
| isWacOverride | **BIT** (→ BOOLEAN) | NULL |
| IsUseResidualGain | **BIT** (→ BOOLEAN) | NULL |
| IsGainSameasLoss | **BIT** (→ BOOLEAN) | NULL |
| IsUseResidualLoss | **BIT** (→ BOOLEAN) | NULL |
| ExcessGainsSelected | VARCHAR(100) | NULL |
| ResidualLoss | VARCHAR(100) | NULL |
| isAdjustmentOverride | **BIT** (→ BOOLEAN) | NULL |
| IsAlternativeStuffing | **BIT** (→ BOOLEAN) | NULL |
| IsGain731a | **BIT** (→ BOOLEAN) | NULL |
| IsParallelUpperTierCalcEnabled | **BIT** (→ BOOLEAN) | NULL |
| IsCalcCleanupDisabled | **BIT** (→ BOOLEAN) | NULL |
| UsePYData | **BIT** (→ BOOLEAN) | NULL |
| UsePYFinal | **BIT** (→ BOOLEAN) | NULL |
| EnableLegacyMK1Import | **BIT** (→ BOOLEAN) | NULL |
| IsEntityLevelCustReptToDotNet | **BIT** (→ BOOLEAN) | NULL |
| 704cAllocationTypeID | INT | NULL |
| EnableCustomPeriodsforWAC | **BIT** (→ BOOLEAN) | NULL |

### dbo.EntityRelationShip

| Column | Type | Nullable |
|---|---|---|
| UpperTierEntityID | INT | NOT NULL |
| LowerTierEntityID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| ClientID | INT | NULL |
| TransactionID | INT | NULL |
| PK_ID | INT IDENTITY | NULL |
| UpdateDate | DATETIME | NULL |

### dbo.Enu_AssetClass

| Column | Type | Nullable |
|---|---|---|
| AssetClassID | INT IDENTITY | NULL |
| AssetClass | VARCHAR(100) | NOT NULL |
| ClientID | INT | NULL |
| DisplayOrder | INT | NULL |
| UpdatedBy | VARCHAR(50) | NULL |
| DateUpdated | DATETIME | NULL |

### dbo.Enu_Event

| Column | Type | Nullable |
|---|---|---|
| EventTypeID | INT IDENTITY | NULL |
| EventName | VARCHAR(64) | NOT NULL |
| EventDescription | VARCHAR(150) | NULL |
| IsWorkflowEvent | **BIT** (→ BOOLEAN) | NULL |
| IsDataFeedEvent | **BIT** (→ BOOLEAN) | NOT NULL |

### dbo.Form1042SPackage

| Column | Type | Nullable |
|---|---|---|
| Form1042SID | INT IDENTITY | NULL |
| K1PackageID | INT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |

### dbo.Form199AFlowUp

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| FlowupEntityID | INT | NOT NULL |
| SourceEntityID | INT | NOT NULL |
| Form199AID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| TextValue | VARCHAR(100) | NULL |

### dbo.Form199ALineItem

| Column | Type | Nullable |
|---|---|---|
| LineID | INT IDENTITY | NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| LineDescription | VARCHAR(300) | NOT NULL |
| LineDataType | VARCHAR(20) | NULL |
| ShortName | VARCHAR(50) | NOT NULL |
| DisplayOrder | INT | NOT NULL |
| IsActive | **BIT** (→ BOOLEAN) | NOT NULL |
| IsAllocated | **BIT** (→ BOOLEAN) | NULL |
| CreatedBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| IsSpeciallyAllocated | **BIT** (→ BOOLEAN) | NULL |
| IsConfigurable | **BIT** (→ BOOLEAN) | NOT NULL |
| isActiveADJReclassImport | **BIT** (→ BOOLEAN) | NOT NULL |

### dbo.Form199APackage

| Column | Type | Nullable |
|---|---|---|
| Form199AID | INT IDENTITY | NULL |
| K1PackageID | INT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |

### dbo.Form8865Flowup

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| FlowupEntityID | INT | NOT NULL |
| SourceEntityID | INT | NOT NULL |
| Form8865ID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| TextValue | VARCHAR(128) | NULL |
| SchID | INT | NULL |

### dbo.Form8865LineItem

| Column | Type | Nullable |
|---|---|---|
| LineID | INT IDENTITY | NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| Schedule | VARCHAR(3) | NOT NULL |
| ScheduleDescription | VARCHAR(300) | NOT NULL |
| LineDescription | VARCHAR(300) | NOT NULL |
| LineDataType | VARCHAR(20) | NULL |
| ShortName | VARCHAR(50) | NOT NULL |
| DisplayOrder | INT | NOT NULL |
| IsAllocated | **BIT** (→ BOOLEAN) | NULL |
| CreatedBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| IsConfigurable | **BIT** (→ BOOLEAN) | NOT NULL |
| IsActive | **BIT** (→ BOOLEAN) | NOT NULL |
| IsSpeciallyAllocated | **BIT** (→ BOOLEAN) | NOT NULL |

### dbo.Form8865Package

| Column | Type | Nullable |
|---|---|---|
| Form8865ID | INT IDENTITY | NULL |
| K1PackageID | INT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |

### dbo.Form8886FlowUp

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| FlowupEntityID | INT | NOT NULL |
| SourceEntityID | INT | NOT NULL |
| Form8886ID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| TextValue | VARCHAR(100) | NULL |
| TransactionName | VARCHAR(150) | NULL |
| TransactionEntityID | INT | NULL |
| Comments | VARCHAR(MAX) | NULL |
| SecIIComments | VARCHAR(MAX) | NULL |

### dbo.Form8886LineItem

| Column | Type | Nullable |
|---|---|---|
| LineID | INT IDENTITY | NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| LineDescription | VARCHAR(2000) | NULL |
| LineDataType | VARCHAR(20) | NULL |
| ShortName | VARCHAR(250) | NULL |
| DisplayOrder | INT | NOT NULL |
| IsActive | **BIT** (→ BOOLEAN) | NOT NULL |
| IsAllocated | **BIT** (→ BOOLEAN) | NULL |
| CreatedBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| IsSpeciallyAllocated | **BIT** (→ BOOLEAN) | NULL |
| IsConfigurable | **BIT** (→ BOOLEAN) | NOT NULL |
| isActiveADJReclassImport | **BIT** (→ BOOLEAN) | NOT NULL |

### dbo.Form8886Package

| Column | Type | Nullable |
|---|---|---|
| Form8886ID | INT IDENTITY | NULL |
| K1PackageID | INT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| Comments | VARCHAR(MAX) | NULL |

### dbo.Form926Flowup

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| FlowupEntityID | INT | NOT NULL |
| SourceEntityID | INT | NOT NULL |
| Form926ID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| TextValue | VARCHAR(100) | NULL |

### dbo.Form926LineItem

| Column | Type | Nullable |
|---|---|---|
| LineID | INT IDENTITY | NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| PartNumber | INT | NOT NULL |
| LineDescription | VARCHAR(500) | NULL |
| LineDataType | VARCHAR(20) | NULL |
| ShortName | VARCHAR(50) | NOT NULL |
| DisplayOrder | INT | NOT NULL |
| IsActive | **BIT** (→ BOOLEAN) | NOT NULL |
| IsAllocated | **BIT** (→ BOOLEAN) | NULL |
| CreatedBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| IsSpeciallyAllocated | **BIT** (→ BOOLEAN) | NULL |
| IsConfigurable | **BIT** (→ BOOLEAN) | NOT NULL |
| isActiveADJReclassImport | **BIT** (→ BOOLEAN) | NOT NULL |
| IsXtractOverride | **BIT** (→ BOOLEAN) | NOT NULL |

### dbo.Form926Package

| Column | Type | Nullable |
|---|---|---|
| Form926ID | INT IDENTITY | NULL |
| K1PackageID | INT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |

### dbo.GlobalMenu

| Column | Type | Nullable |
|---|---|---|
| GlobalMenuID | INT IDENTITY | NULL |
| GlobalMenuGroupID | INT | NULL |
| MenuName | VARCHAR(400) | NULL |
| URL | VARCHAR(400) | NULL |
| State | VARCHAR(10) | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| IsHedge | **BIT** (→ BOOLEAN) | NULL |
| IsTechConfig | **BIT** (→ BOOLEAN) | NULL |
| UserID | NVARCHAR(128) | NULL |
| ValidFrom | DATETIME2 | NOT NULL |
| ValidTo | DATETIME2 | NOT NULL |
| IsStandardizationEnabled | **BIT** (→ BOOLEAN) | NULL |
| AllowStandardizationUpdate | **BIT** (→ BOOLEAN) | NULL |

### dbo.K1GPartnerTypes

| Column | Type | Nullable |
|---|---|---|
| PartnerTypeID | INT IDENTITY | NULL |
| PartnerTypeDesc | VARCHAR(200) | NULL |
| StatePartnerTypeDesc | VARCHAR(200) | NULL |

### dbo.K1LineItem

| Column | Type | Nullable |
|---|---|---|
| LineID | INT IDENTITY | NULL |
| LineNumber | VARCHAR(10) | NULL |
| Box | VARCHAR(20) | NULL |
| LineDescription | VARCHAR(250) | NULL |
| TaxableIncomeRule | VARCHAR(20) | NULL |
| TaxPeriodID | INT | NULL |
| Comment | VARCHAR(MAX) | NULL |
| AllocationType | VARCHAR(50) | NULL |
| Classification | VARCHAR(32) | NULL |
| DisplayOrder | INT | NOT NULL |
| LineDataType | VARCHAR(20) | NULL |
| ClientID | INT | NOT NULL |
| CreateBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| IsActive | **BIT** (→ BOOLEAN) | NOT NULL |
| IsGPFeeAllocated | **BIT** (→ BOOLEAN) | NULL |
| IsReadOnly | **BIT** (→ BOOLEAN) | NULL |
| ApplicableToStates | VARCHAR(50) | NULL |
| TaxableIncomeRuleForStates | VARCHAR(20) | NULL |
| TRC | NVARCHAR(40) | NULL |
| TCC | NVARCHAR(40) | NULL |
| MRC | NVARCHAR(40) | NULL |
| MCC | NVARCHAR(40) | NULL |
| TCCDescription | NVARCHAR(400) | NULL |
| TransactionDate | DATETIME | NULL |
| IsTransactionDate | **BIT** (→ BOOLEAN) | NOT NULL |
| IsTransfersAdjusted | **BIT** (→ BOOLEAN) | NULL |
| K1CategoryID | INT | NULL |
| IsVisible | **BIT** (→ BOOLEAN) | NULL |
| IsM1Adjustment | **BIT** (→ BOOLEAN) | NULL |
| Is16BCalc | **BIT** (→ BOOLEAN) | NULL |
| Is16CCalc | **BIT** (→ BOOLEAN) | NULL |
| Is16HCalc | **BIT** (→ BOOLEAN) | NULL |
| BookOrdinaryIncomeRule | VARCHAR(20) | NULL |
| IncludeLookthroughData | **BIT** (→ BOOLEAN) | NULL |
| AdditionalDetail1 | VARCHAR(MAX) | NULL |
| AdditionalDetail2 | VARCHAR(MAX) | NULL |
| AdditionalDetail3 | VARCHAR(MAX) | NULL |
| MappedToPFIC | VARCHAR(20) | NULL |
| DisplayInGAAPtoTAX | INT | NOT NULL |
| isGrossOverride | **BIT** (→ BOOLEAN) | NULL |
| PFICClassType | VARCHAR(20) | NULL |
| TransferBasisAdjTypeID | INT | NULL |
| IsFDAPLineClassification | **BIT** (→ BOOLEAN) | NULL |
| IsTaxHoldback | **BIT** (→ BOOLEAN) | NULL |
| AllocationTypeRuleId | INT | NULL |
| IsRestrictOverrideXtract | **BIT** (→ BOOLEAN) | NULL |
| IsGain731a | **BIT** (→ BOOLEAN) | NULL |
| SaleOrDispositionGainLoss | VARCHAR(100) | NULL |
| DefaultDispositionGainLoss | **BIT** (→ BOOLEAN) | NULL |
| SystemLineDescription | VARCHAR(250) | NULL |
| SICCode | VARCHAR(20) | NULL |
| TaxTypeId | INT | NULL |
| K3AttributeTypeID | INT | NULL |
| IsK3PYLineItem | **BIT** (→ BOOLEAN) | NULL |
| CapitalGainLoss | VARCHAR(50) | NULL |
| K1K3ValidationRule | INT | NULL |
| TypeK1K3VldImport | VARCHAR(50) | NULL |
| TypeK1K3VldCalc | VARCHAR(50) | NULL |
| SchK3Part2 | VARCHAR(50) | NULL |
| SchK3Part3 | VARCHAR(50) | NULL |
| SchK3Part4 | VARCHAR(50) | NULL |
| SchK3Part9 | VARCHAR(50) | NULL |
| SchK3Part10 | VARCHAR(50) | NULL |
| SchK3Part13 | VARCHAR(50) | NULL |
| Custom1 | VARCHAR(50) | NULL |
| Custom2 | VARCHAR(50) | NULL |
| Custom3 | VARCHAR(50) | NULL |
| Custom4 | VARCHAR(50) | NULL |
| Custom5 | VARCHAR(50) | NULL |
| StandardVsCustom | VARCHAR(25) | NULL |
| ECIWithholding | VARCHAR(10) | NULL |
| ECISubTotal | VARCHAR(10) | NULL |
| IsRestrictDataFeedbyEntInvOverride | **BIT** (→ BOOLEAN) | NULL |
| IsVisiblePriorYear | **BIT** (→ BOOLEAN) | NULL |
| ECILineClassification | VARCHAR(500) | NULL |
| FDAPLineClassification | VARCHAR(500) | NULL |
| Gain731aPeriod | VARCHAR(100) | NULL |
| DisparityLineClassification | VARCHAR(100) | NULL |

### dbo.K1Package

| Column | Type | Nullable |
|---|---|---|
| K1PackageID | INT IDENTITY | NULL |
| UpperTierEntityID | INT | NOT NULL |
| LowerTierEntityID | INT | NULL |
| TaxPeriodID | INT | NOT NULL |
| IsAnnualized | **BIT** (→ BOOLEAN) | NULL |
| IsFullRedemption | **BIT** (→ BOOLEAN) | NULL |
| K1StatusTypeID | INT | NULL |
| MonthsOfIncome | INT | NULL |
| MonthsAnnualizingFor | INT | NULL |
| UpdatedBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| ClientID | INT | NULL |
| WorkflowStatusID | INT | NULL |
| ReceivedDate | DATETIME | NULL |
| DateK1Expected | DATETIME | NULL |
| K1InputStatusId | INT | NULL |
| FinalK1 | VARCHAR(20) | NULL |
| AmendedK1 | VARCHAR(20) | NULL |
| PhaseID | INT | NULL |
| K1ATaxBasisStatus | VARCHAR(20) | NULL |
| StateK1TypeID | INT | NULL |
| StateK1StatusID | INT | NULL |
| DateOfDisposition | DATETIME | NULL |
| PartnershipTypeID | INT | NULL |
| K1inputTeamSignoff | VARCHAR(1000) | NULL |
| FundTeamSignoff | VARCHAR(1000) | NULL |

### dbo.LookThroughAllocationInput

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| EntityID | INT | NOT NULL |
| LineTypeID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| QuicklinkID | INT | NULL |
| Amount704b | FLOAT | NULL |
| CategoryID | INT | NULL |
| ParentEntityID | INT | NULL |
| PeriodID | INT | NULL |
| LineCode | VARCHAR(100) | NULL |
| SuperParentEntityID | INT | NULL |
| AdjustmentTypeID | INT | NULL |
| TrackingKey | VARCHAR(4000) | NULL |
| Tag | VARCHAR(5000) | NULL |
| OriginalParentEntityID | INT | NULL |
| FlowUpPartner | VARCHAR(50) | NULL |

### dbo.LookThroughAllocationOutput

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| EntityID | INT | NOT NULL |
| ShareClass | VARCHAR(200) | NULL |
| PartnerNumber | VARCHAR(50) | NOT NULL |
| LineTypeID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| AllocationType | VARCHAR(150) | NULL |
| QuicklinkID | INT | NULL |
| Amount704b | FLOAT | NULL |
| CategoryID | INT | NULL |
| ParentEntityID | INT | NULL |
| PeriodID | INT | NULL |
| LineCode | VARCHAR(100) | NULL |
| SuperParentEntityID | INT | NULL |
| AdjustmentTypeID | INT | NULL |
| TrackingKey | VARCHAR(4000) | NULL |
| Tag | VARCHAR(5000) | NULL |
| AllocationTypeID | INT | NULL |
| OriginalParentEntityID | INT | NULL |
| FlowUpPartner | VARCHAR(50) | NULL |

### dbo.LookThroughTaxableIncome

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| ShareClass | VARCHAR(200) | NULL |
| PartnerNumber | VARCHAR(50) | NOT NULL |
| TaxableIncome | FLOAT | NULL |
| ParentEntityID | INT | NULL |
| Tag | VARCHAR(5000) | NULL |

### dbo.MAP_DerivedLines

| Column | Type | Nullable |
|---|---|---|
| MapID | INT IDENTITY | NULL |
| BaseLineID | INT | NULL |
| DerivedLineID | INT | NULL |
| AttributeID | INT | NULL |
| ParentK1GLineID | INT | NULL |
| Level | INT | NULL |

### dbo.MAP_K1LineItemLineType

| Column | Type | Nullable |
|---|---|---|
| MapID | INT IDENTITY | NULL |
| K1LineItemID | INT | NOT NULL |
| LineTypeID | INT | NOT NULL |

### dbo.MapDataRegister

| Column | Type | Nullable |
|---|---|---|
| MapRegisterID | INT IDENTITY | NULL |
| EntityID | INT | NOT NULL |
| LineDescription | VARCHAR(MAX) | NULL |
| RegisterTypeID | INT | NOT NULL |
| SourceTypeID | INT | NOT NULL |
| MapLineID | INT | NOT NULL |
| RegisterLineID | INT | NOT NULL |
| OperationType | VARCHAR(20) | NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NULL |
| CreateBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| CategoryID | INT | NULL |
| MapLineSubType | VARCHAR(100) | NULL |
| StateID | INT | NULL |
| PeriodID | INT | NULL |
| LevelTypeID | INT | NULL |
| IsEntityLevel | INT | NULL |
| CurrencyCode | VARCHAR(10) | NULL |
| AdjustmentTypeID | INT | NULL |
| AdjustmentSourceID | INT | NULL |
| FieldSourceID | INT | NULL |
| Quarter | INT | NULL |
| TaxTreatment | VARCHAR(200) | NULL |
| Waterfall | VARCHAR(200) | NULL |
| OffsetType | VARCHAR(200) | NULL |
| TransactionDate | VARCHAR(100) | NULL |
| LineType | VARCHAR(200) | NULL |
| GainOrLoss | VARCHAR(25) | NULL |
| AllocationTypeID | INT | NULL |
| ContributionLineClassification | VARCHAR(20) | NULL |
| ForceDisplayinReports | VARCHAR(5) | NULL |
| iFormsOutputFormat | VARCHAR(20) | NULL |

### dbo.MapDefaultAllocRuleToLineItem

| Column | Type | Nullable |
|---|---|---|
| TransactionID | INT | NOT NULL |
| SourceID | INT | NOT NULL |
| SelectedMappingID | INT | NOT NULL |
| RuleID | INT | NOT NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| StateID | INT | NOT NULL |
| ExcludeFromTransfers | INT | NOT NULL |

### dbo.MapRulesToUnderlyings

| Column | Type | Nullable |
|---|---|---|
| TransactionID | INT | NOT NULL |
| RuleID | INT | NOT NULL |
| UnderlyingID | INT | NOT NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| 704cAllocationTypeID | INT | NULL |

### dbo.MappingLineItem

| Column | Type | Nullable |
|---|---|---|
| LineID | INT IDENTITY | NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| SourceID | INT | NOT NULL |
| LineDescription | VARCHAR(100) | NOT NULL |
| DatabaseName | VARCHAR(50) | NOT NULL |
| DisplayOrder | INT | NOT NULL |
| IsActive | **BIT** (→ BOOLEAN) | NOT NULL |
| CreateBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| RegisterTypeID | INT | NULL |

### dbo.PE_AllocationInput

| Column | Type | Nullable |
|---|---|---|
| PEFundRunID | BIGINT | NOT NULL |
| ClientID | INT | NOT NULL |
| InvestmentID | INT | NOT NULL |
| LineTypeID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| QuicklinkID | INT | NULL |
| InitialAmount | FLOAT | NULL |

### dbo.PE_ENU_DataList

| Column | Type | Nullable |
|---|---|---|
| ID | INT IDENTITY | NULL |
| Category | VARCHAR(100) | NULL |
| Value | VARCHAR(200) | NULL |
| DisplayOrder | INT | NULL |
| Comments | VARCHAR(200) | NULL |

### dbo.PE_SM_AllocationInput

| Column | Type | Nullable |
|---|---|---|
| PEFundRunID | BIGINT | NOT NULL |
| ClientID | INT | NOT NULL |
| InvestmentID | INT | NOT NULL |
| LineTypeID | INT | NOT NULL |
| StateID | INT | NOT NULL |
| StateLineID | INT | NOT NULL |
| InitialAmount | FLOAT | NULL |

### dbo.PFICFootnoteFlowup

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| FlowupEntityID | INT | NOT NULL |
| SourceEntityID | INT | NOT NULL |
| PFICFootnoteID | INT | NOT NULL |
| LineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| TextValue | VARCHAR(100) | NULL |
| PK_ID | BIGINT IDENTITY | NULL |

### dbo.PFICFootnoteLineItem

| Column | Type | Nullable |
|---|---|---|
| LineID | INT IDENTITY | NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| LineDescription | VARCHAR(300) | NOT NULL |
| LineDataType | VARCHAR(20) | NOT NULL |
| ShortName | VARCHAR(50) | NOT NULL |
| DisplayOrder | INT | NOT NULL |
| IsActive | **BIT** (→ BOOLEAN) | NOT NULL |
| IsAllocated | **BIT** (→ BOOLEAN) | NULL |
| CreatedBy | VARCHAR(50) | NULL |
| UpdateDate | DATETIME | NULL |
| IsSpeciallyAllocated | **BIT** (→ BOOLEAN) | NULL |
| Classification | VARCHAR(50) | NULL |
| IsConfigurable | **BIT** (→ BOOLEAN) | NOT NULL |
| DefaultAllocationRule | VARCHAR(200) | NULL |
| IsPartVAllocated | **BIT** (→ BOOLEAN) | NOT NULL |
| ValidateRule | VARCHAR(50) | NULL |
| ValidateType | VARCHAR(50) | NULL |
| isActiveADJReclassImport | **BIT** (→ BOOLEAN) | NOT NULL |
| Unique_Identifier | **BIT** (→ BOOLEAN) | NOT NULL |
| RoundingTypeID | INT | NULL |
| IsXtractOverride | **BIT** (→ BOOLEAN) | NOT NULL |

### dbo.PFICFootnotePackage

| Column | Type | Nullable |
|---|---|---|
| PFICFootnoteID | INT IDENTITY | NULL |
| K1PackageID | INT | NOT NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| IsDerivedPFIC | **BIT** (→ BOOLEAN) | NULL |

### dbo.Partner_Snapshot

| Column | Type | Nullable |
|---|---|---|
| WorkFlowID | INT | NULL |
| PartnerID | INT | NULL |
| Transactionid | INT | NULL |
| Clientid | INT | NULL |
| TaxperiodID | INT | NULL |
| MasterID | VARCHAR(50) | NULL |
| PartnerNumber | VARCHAR(50) | NULL |
| Name1 | VARCHAR(250) | NULL |
| Name2 | VARCHAR(250) | NULL |
| Name3 | VARCHAR(250) | NULL |
| IndividualNameLast | VARCHAR(150) | NULL |
| IndividualNameFirst | VARCHAR(150) | NULL |
| IndividualNameMiddle | VARCHAR(150) | NULL |
| Address1 | VARCHAR(250) | NULL |
| Address2 | VARCHAR(250) | NULL |
| City | VARCHAR(250) | NULL |
| DomState | VARCHAR(50) | NULL |
| DomZIP | VARCHAR(50) | NULL |
| Province | VARCHAR(50) | NULL |
| ForeignCountry | VARCHAR(50) | NULL |
| GoSystemsOrProSystems | VARCHAR(50) | NULL |
| RefForeignCountryName | VARCHAR(50) | NULL |
| ForeignZIP | VARCHAR(50) | NULL |
| EIN | VARCHAR(50) | NULL |
| GPorLP | VARCHAR(50) | NULL |
| DomOrForeign | VARCHAR(50) | NULL |
| EntityType | INT | NULL |
| IsGeneralPartnership | **BIT** (→ BOOLEAN) | NULL |
| Residency | VARCHAR(50) | NULL |
| FinalK1 | **BIT** (→ BOOLEAN) | NULL |
| AmendedK1 | **BIT** (→ BOOLEAN) | NULL |
| ShareClass | VARCHAR(50) | NULL |
| 3rdPartyClassification | VARCHAR(50) | NULL |
| 3rdPartySystemID | VARCHAR(50) | NULL |
| PEPDistChannel | VARCHAR(50) | NULL |
| DefaultDemographic | VARCHAR(50) | NULL |
| EntityName | VARCHAR(200) | NULL |
| EntityID | INT | NULL |
| EntityIdentification | VARCHAR(150) | NULL |
| isTaxExempt | **BIT** (→ BOOLEAN) | NULL |
| isComposite | **BIT** (→ BOOLEAN) | NULL |
| Custom1 | VARCHAR(50) | NULL |
| Custom2 | VARCHAR(50) | NULL |
| Custom3 | VARCHAR(50) | NULL |
| Custom4 | VARCHAR(50) | NULL |
| Custom5 | VARCHAR(50) | NULL |
| Custom6 | VARCHAR(50) | NULL |
| Custom7 | VARCHAR(50) | NULL |
| Custom8 | VARCHAR(50) | NULL |
| Custom9 | VARCHAR(50) | NULL |
| Custom10 | VARCHAR(50) | NULL |
| Custom11 | VARCHAR(50) | NULL |
| Custom12 | VARCHAR(50) | NULL |
| Custom13 | VARCHAR(50) | NULL |
| Custom14 | VARCHAR(50) | NULL |
| Custom15 | VARCHAR(50) | NULL |
| Custom16 | VARCHAR(50) | NULL |
| Custom17 | VARCHAR(50) | NULL |
| Custom18 | VARCHAR(50) | NULL |
| Custom19 | VARCHAR(50) | NULL |
| Custom20 | VARCHAR(50) | NULL |
| DisplayOrder | INT | NULL |
| Suffix | VARCHAR(50) | NULL |
| ActiveorPassive | VARCHAR(50) | NULL |
| FullName | VARCHAR(1000) | NULL |
| FullAddress | VARCHAR(1000) | NULL |
| UpperTierEntityIdentification | VARCHAR(200) | NULL |
| Commitment | NUMERIC(20,10) | NULL |
| TaxFormType | INT | NULL |
| Address1042 | VARCHAR(100) | NULL |
| CountryofOrganization | VARCHAR(100) | NULL |
| ResidentCountry | VARCHAR(100) | NULL |
| ForeignEIN | VARCHAR(100) | NULL |
| RecipientCode | VARCHAR(100) | NULL |
| WhRateInterest | FLOAT | NULL |
| WhRateDividend | FLOAT | NULL |
| WhRateOther | FLOAT | NULL |
| FATCAStatus | INT | NULL |
| FATCAWithholdingRate | FLOAT | NULL |
| FFIIDNumber | VARCHAR(100) | NULL |
| FFIEIN | VARCHAR(100) | NULL |
| TaxFormSigned | **BIT** (→ BOOLEAN) | NULL |
| TaxFormExpirationDate | DATETIME | NULL |
| SpecialProvisionforComp | **BIT** (→ BOOLEAN) | NULL |
| SSNorEIN | VARCHAR(10) | NULL |
| StatePartnerType | VARCHAR(150) | NULL |
| CarryPaying | **BIT** (→ BOOLEAN) | NULL |
| PayStuffing | **BIT** (→ BOOLEAN) | NULL |
| IsSidePocketFlowUpPartner | **BIT** (→ BOOLEAN) | NULL |
| eDeliveryCode | VARCHAR(100) | NULL |
| ForceFileComposite | **BIT** (→ BOOLEAN) | NULL |
| PK_ID | INT IDENTITY | NULL |
| ExemptOrgStatePartnerType | VARCHAR(150) | NULL |
| OwnedthroughDisregardedEntity | **BIT** (→ BOOLEAN) | NULL |
| DisregardedEntityTIN | VARCHAR(20) | NULL |
| DisregardedEntityName | VARCHAR(100) | NULL |
| FundID | INT | NULL |
| K1GPartnerNumber | BIGINT | NULL |
| MergeCode | VARCHAR(250) | NULL |
| SurvivingPartner | VARCHAR(50) | NULL |
| OwnerThroughDRE | CHAR(1) | NULL |
| DRETIN | VARCHAR(50) | NULL |
| DRENAME | VARCHAR(200) | NULL |
| LockPartner | **BIT** (→ BOOLEAN) | NULL |
| CustomA | VARCHAR(250) | NULL |
| CustomB | VARCHAR(250) | NULL |
| CustomC | VARCHAR(250) | NULL |
| Custom21 | VARCHAR(250) | NULL |
| Custom22 | VARCHAR(250) | NULL |
| Custom23 | VARCHAR(250) | NULL |
| ForOrDomAddressType | VARCHAR(50) | NULL |

### dbo.Phase

| Column | Type | Nullable |
|---|---|---|
| PhaseID | INT | NOT NULL |
| PhaseName | VARCHAR(100) | NULL |
| ShortName | VARCHAR(3) | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| StartDate | DATETIME | NULL |
| EndDate | DATETIME | NULL |
| ID | INT IDENTITY | NULL |

### dbo.QuarterDates

| Column | Type | Nullable |
|---|---|---|
| ID | INT IDENTITY | NULL |
| Quarter | VARCHAR(10) | NULL |
| StartDate | DATETIME | NULL |
| EndDate | DATETIME | NULL |
| Preference | INT | NULL |

### dbo.SM_FederaltoStatePartnerTypeMapping

| Column | Type | Nullable |
|---|---|---|
| MapPartnerTypeID | INT IDENTITY | NULL |
| FederalPartnerType | VARCHAR(50) | NOT NULL |
| StatePartnerType | VARCHAR(50) | NOT NULL |

### dbo.SM_LookThroughAllocationInput

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| EntityID | INT | NOT NULL |
| LineTypeID | INT | NOT NULL |
| StateID | INT | NOT NULL |
| StateLineID | INT | NOT NULL |
| Amount | FLOAT | NULL |
| QuicklinkID | INT | NULL |
| Amount704b | FLOAT | NULL |
| CategoryID | INT | NULL |
| ParentEntityID | INT | NULL |
| PeriodID | INT | NULL |
| LineCode | VARCHAR(100) | NULL |
| SuperParentEntityID | INT | NULL |
| AdjustmentTypeID | INT | NULL |
| TrackingKey | VARCHAR(4000) | NULL |
| Tag | VARCHAR(5000) | NULL |
| OriginalParentEntityID | INT | NULL |
| FlowUpPartner | VARCHAR(50) | NULL |

### dbo.SM_StateLineAllocationRule_Snapshot

| Column | Type | Nullable |
|---|---|---|
| WorkflowID | INT | NULL |
| TransactionID | INT | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
| EntityID | INT | NULL |
| UnderlyingEntityID | INT | NULL |
| SourceID | INT | NULL |
| StateID | INT | NULL |
| StateLineID | INT | NULL |
| AllocationTypeID | INT | NULL |
| AdjustmentAllocationTypeID | INT | NULL |
| Tag | VARCHAR(500) | NULL |
| TrackingKey | VARCHAR(4000) | NULL |
| DealID | VARCHAR(500) | NULL |

### dbo.SM_StateLines

| Column | Type | Nullable |
|---|---|---|
| StateFieldID | INT | NOT NULL |
| Description | VARCHAR(150) | NOT NULL |
| ShortName | VARCHAR(200) | NULL |
| IsStateSpecificLine | **BIT** (→ BOOLEAN) | NOT NULL |
| HasWithholding | **BIT** (→ BOOLEAN) | NULL |
| HasComposite | **BIT** (→ BOOLEAN) | NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| StateID | INT | NOT NULL |
| LineNumber | VARCHAR(20) | NULL |
| Box | VARCHAR(20) | NULL |
| DisplayOrder | INT | NOT NULL |
| CategoryID | INT | NULL |
| IsInterestIncome | **BIT** (→ BOOLEAN) | NOT NULL |
| TransactionDate | DATETIME | NULL |
| IsTaxHoldback | **BIT** (→ BOOLEAN) | NULL |
| AllocationTypeRuleId | INT | NULL |
| IsRestrictDataOverride | **BIT** (→ BOOLEAN) | NULL |
| StandardCode | VARCHAR(35) | NULL |
| IsStateLineThreshold | **BIT** (→ BOOLEAN) | NULL |
| UsePYData | **BIT** (→ BOOLEAN) | NULL |
| IsPETaxCredit | **BIT** (→ BOOLEAN) | NULL |
| IsNYCNOLDeduction | **BIT** (→ BOOLEAN) | NULL |
| IsRestrictDataFeedbyEntInvOverride | **BIT** (→ BOOLEAN) | NULL |
| StandardSiteClientID | INT | NULL |
| StandardSiteIdentifier | INT | NULL |
| UpdateDate | DATETIME | NULL |

### dbo.TransactionLog

| Column | Type | Nullable |
|---|---|---|
| TransactionID | INT IDENTITY | NULL |
| EventTypeID | INT | NOT NULL |
| ClientID | INT | NULL |
| EntityID | INT | NULL |
| TaxPeriodID | INT | NOT NULL |
| TransactionDate | DATETIME | NOT NULL |
| UserLoginName | VARCHAR(226) | NULL |
| StatusID | INT | NULL |
| PhaseID | INT | NULL |
| CustomImportId | INT | NULL |
| WorkflowImportId | INT | NULL |

### dbo.TransfersAdjCostDefaultPercentage

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | INT | NOT NULL |
| EntityID | INT | NOT NULL |
| InvestmentID | INT | NOT NULL |
| PartnerNumber | VARCHAR(50) | NOT NULL |
| TransferPartnerNumber | VARCHAR(50) | NULL |
| TransferAdjPercent | FLOAT | NULL |
| EndingCostPercent | FLOAT | NULL |
| TransferDate | DATETIME | NULL |
| TransferDirection | VARCHAR(5) | NULL |
| BeginningPercentUsage | FLOAT | NULL |
| EffectivePercent | FLOAT | NULL |
| AllocationComplete | VARCHAR(3) | NULL |
| AllocationTypeID | INT | NULL |
| TrackingKey | VARCHAR(5000) | NULL |
| Tag | VARCHAR(5000) | NULL |
| Underlyingtype | INT | NULL |
| IsEODTransfer | **BIT** (→ BOOLEAN) | NULL |

### dbo.TransfersAdjDefaultPercentage

| Column | Type | Nullable |
|---|---|---|
| RunID | BIGINT | NOT NULL |
| ClientID | BIGINT | NOT NULL |
| EntityID | INT | NOT NULL |
| PartnerNumber | VARCHAR(50) | NOT NULL |
| ShareClass | VARCHAR(200) | NULL |
| TransferPartnerNumber | VARCHAR(50) | NULL |
| TransferAdjPercent | FLOAT | NULL |
| EndingCommitmentPercent | FLOAT | NULL |
| TransferDate | DATETIME | NULL |
| TransferDirection | VARCHAR(5) | NULL |
| BeginningPercentUsage | FLOAT | NULL |
| EffectivePercent | FLOAT | NULL |
| AllocationComplete | VARCHAR(5) | NULL |
| IsEODTransfer | **BIT** (→ BOOLEAN) | NULL |

### dbo.WorkFlow

| Column | Type | Nullable |
|---|---|---|
| WorkflowID | INT IDENTITY | NULL |
| K1PackageID | INT | NULL |
| TransactionID | INT | NULL |
| SubmitByID | VARCHAR(100) | NULL |
| SubmitDate | DATETIME | NULL |
| ClientID | INT | NOT NULL |
| TaxPeriodID | INT | NOT NULL |
| PhaseID | INT | NULL |
| IsLastStep | **BIT** (→ BOOLEAN) | NULL |
| UnderReviewLevelID | INT | NULL |

### dbo.WorkFlowChain

| Column | Type | Nullable |
|---|---|---|
| WorkflowChainID | INT IDENTITY | NULL |
| StepNumber | SMALLINT | NOT NULL |
| ClientID | INT | NOT NULL |
| RoleID | UNIQUEIDENTIFIER | NULL |
| WorkflowStatusID | INT | NULL |
| TaxPeriodID | INT | NOT NULL |
| IncludeInCalc | **BIT** (→ BOOLEAN) | NULL |

### dbo.WorkflowStatus

| Column | Type | Nullable |
|---|---|---|
| StatusID | INT IDENTITY | NULL |
| DisplayName | VARCHAR(128) | NOT NULL |
| EnumerationName | VARCHAR(128) | NOT NULL |
| ImagePath | VARCHAR(256) | NULL |
| Priority | INT | NOT NULL |

### dbo.Yearly_Snapshot

| Column | Type | Nullable |
|---|---|---|
| WorkflowID | INT | NULL |
| TransactionID | INT | NULL |
| ClientID | INT | NULL |
| EntityID | INT | NULL |
| TaxPeriodID | INT | NULL |
| PartnerNumber | VARCHAR(50) | NULL |
| ShareClass | VARCHAR(200) | NULL |
| TaxBegCapAccount | FLOAT | NULL |
| GaapBegCapAccount | FLOAT | NULL |
| 704bBegCapAccount | FLOAT | NULL |
| BookCashContriQ1 | FLOAT | NULL |
| BookCashContriQ2 | FLOAT | NULL |
| BookCashContriQ3 | FLOAT | NULL |
| BookCashContriQ4 | FLOAT | NULL |
| TotalBookCashContri | FLOAT | NULL |
| BookPrContriQ1 | FLOAT | NULL |
| BookPrContriQ2 | FLOAT | NULL |
| BookPrContriQ3 | FLOAT | NULL |
| BookPrContriQ4 | FLOAT | NULL |
| TotalBookPrContri | FLOAT | NULL |
| GaapIncDec | FLOAT | NULL |
| 704bIncDec | FLOAT | NULL |
| BookCashDistQ1 | FLOAT | NULL |
| BookCashDistQ2 | FLOAT | NULL |
| BookCashDistQ3 | FLOAT | NULL |
| BookCashDistQ4 | FLOAT | NULL |
| TotalBookCshDist | FLOAT | NULL |
| BookPrDistQ1 | FLOAT | NULL |
| BookPrDistQ2 | FLOAT | NULL |
| BookPrDistQ3 | FLOAT | NULL |
| BookPrDistQ4 | FLOAT | NULL |
| TotalBookPrDist | FLOAT | NULL |
| GaapEndCapAccount | FLOAT | NULL |
| 704bEndingCapAccount | FLOAT | NULL |
| BegProfitPercent | FLOAT | NULL |
| BegLossPercent | FLOAT | NULL |
| BegCapitalPercent | FLOAT | NULL |
| EndProfitPercent | FLOAT | NULL |
| EndLossPercent | FLOAT | NULL |
| EndCapitalPercent | FLOAT | NULL |
| TaxCshContriQ1 | FLOAT | NULL |
| TaxCshContriQ2 | FLOAT | NULL |
| TaxCshContriQ3 | FLOAT | NULL |
| TaxCshContriQ4 | FLOAT | NULL |
| TotalTaxCshContri | FLOAT | NULL |
| TaxPrContriQ1 | FLOAT | NULL |
| TaxPrContriQ2 | FLOAT | NULL |
| TaxPrContriQ3 | FLOAT | NULL |
| TaxPrContriQ4 | FLOAT | NULL |
| TotalTaxPrContri | FLOAT | NULL |
| TaxCashDistQ1 | FLOAT | NULL |
| TaxCashDistQ2 | FLOAT | NULL |
| TaxCashDistQ3 | FLOAT | NULL |
| TaxCashDistQ4 | FLOAT | NULL |
| TotalTaxCshDist | FLOAT | NULL |
| TaxPrptDistQ1 | FLOAT | NULL |
| TaxPrptDistQ2 | FLOAT | NULL |
| TaxPrptDistQ3 | FLOAT | NULL |
| TaxPrptDistQ4 | FLOAT | NULL |
| TotalTaxPrptDist | FLOAT | NULL |
| GaapIncomeQ1 | FLOAT | NULL |
| GaapIncomeQ2 | FLOAT | NULL |
| GaapIncomeQ3 | FLOAT | NULL |
| GaapIncomeQ4 | FLOAT | NULL |
| TotalGaapIncome | FLOAT | NULL |
| 704bIncomeQ1 | FLOAT | NULL |
| 704bIncomeQ2 | FLOAT | NULL |
| 704bIncomeQ3 | FLOAT | NULL |
| 704bIncomeQ4 | FLOAT | NULL |
| Total704bIncome | FLOAT | NULL |
| TotalContriToTaxAlloc | FLOAT | NULL |
| TotalDistriToTaxAlloc | FLOAT | NULL |
| IncentiveReallocPercent | FLOAT | NULL |
| SyndicationCosts | FLOAT | NULL |
| MgmtFeesQ1 | FLOAT | NULL |
| MgmtFeesQ2 | FLOAT | NULL |
| MgmtFeesQ3 | FLOAT | NULL |
| MgmtFeesQ4 | FLOAT | NULL |
| TotalMgmtFees | FLOAT | NULL |
| NonRecLiabilities | FLOAT | NULL |
| QualNonRecLiabilities | FLOAT | NULL |
| RecLiabilities | FLOAT | NULL |
| CommitmentQ1 | FLOAT | NULL |
| CommitmentQ2 | FLOAT | NULL |
| CommitmentQ3 | FLOAT | NULL |
| CommitmentQ4 | FLOAT | NULL |
| TotalCommitmentAmt | FLOAT | NULL |
| CommitmentPercentQ1 | FLOAT | NULL |
| CommitmentPercentQ2 | FLOAT | NULL |
| CommitmentPercentQ3 | FLOAT | NULL |
| CommitmentPercentQ4 | FLOAT | NULL |
| TotalCommitmentPercent | FLOAT | NULL |
| SubIntRcvdQ1 | FLOAT | NULL |
| SubIntRcvdQ2 | FLOAT | NULL |
| SubIntRcvdQ3 | FLOAT | NULL |
| SubIntRcvdQ4 | FLOAT | NULL |
| TotalSubIntRcvd | FLOAT | NULL |
| SubIntCollctQ1 | FLOAT | NULL |
| SubIntCollctQ2 | FLOAT | NULL |
| SubIntCollctQ3 | FLOAT | NULL |
| SubIntCollctQ4 | FLOAT | NULL |
| TotalSubIntCollct | FLOAT | NULL |
| PartnerDefault | VARCHAR(1) | NULL |
| ProRataEffOwnPercent | FLOAT | NULL |
| PartnerContriPrpGnOrLs | VARCHAR(1) | NULL |
| JanBegEquity | FLOAT | NULL |
| FebBegEquity | FLOAT | NULL |
| MarBegEquity | FLOAT | NULL |
| AprBegEquity | FLOAT | NULL |
| MayBegEquity | FLOAT | NULL |
| JunBegEquity | FLOAT | NULL |
| JulBegEquity | FLOAT | NULL |
| AugBegEquity | FLOAT | NULL |
| SepBegEquity | FLOAT | NULL |
| OctBegEquity | FLOAT | NULL |
| NovBegEquity | FLOAT | NULL |
| DecBegEquity | FLOAT | NULL |
| Custom1 | VARCHAR(50) | NULL |
| Custom2 | VARCHAR(50) | NULL |
| Custom3 | VARCHAR(50) | NULL |
| Custom4 | VARCHAR(50) | NULL |
| Custom5 | VARCHAR(50) | NULL |
| Custom6 | VARCHAR(50) | NULL |
| Custom7 | VARCHAR(50) | NULL |
| Custom8 | VARCHAR(50) | NULL |
| Custom9 | VARCHAR(50) | NULL |
| Custom10 | VARCHAR(50) | NULL |
| Custom11 | VARCHAR(50) | NULL |
| Custom12 | VARCHAR(50) | NULL |
| Custom13 | VARCHAR(50) | NULL |
| Custom14 | VARCHAR(50) | NULL |
| Custom15 | VARCHAR(50) | NULL |
| Custom16 | VARCHAR(50) | NULL |
| Custom17 | VARCHAR(50) | NULL |
| Custom18 | VARCHAR(50) | NULL |
| Custom19 | VARCHAR(50) | NULL |
| Custom20 | VARCHAR(50) | NULL |
| BegRevalAccount | FLOAT | NULL |
| RevalAdjustment | FLOAT | NULL |
| BookIncome | FLOAT | NULL |
| PartnerName | VARCHAR(200) | NULL |
| ActualEndProfitPercent | FLOAT | NULL |
| ActualEndLossPercent | FLOAT | NULL |
| ActualEndCapitalPercent | FLOAT | NULL |
| TransferofInterestPlus | FLOAT | NULL |
| TransferofInterestMinus | FLOAT | NULL |
| ExcludeFromTransfers | VARCHAR(1) | NULL |
| ReceivedIncentive | VARCHAR(1) | NULL |
| IncludeInResidualOverride | VARCHAR(1) | NULL |
| HotIssueGainLoss | FLOAT | NULL |
| 704CGainLoss | FLOAT | NULL |
| PK_ID | INT IDENTITY | NULL |
| BegNonRecLiabilities | FLOAT | NULL |
| BegQualNonRecLiabilities | FLOAT | NULL |
| BegRecLiabilities | FLOAT | NULL |
| Fed_CurrentYrIncomeLoss | FLOAT | NULL |
| FedIncludesLiabfromlowertier | VARCHAR(1) | NULL |
| FedBegPartShNetUnrecsec704c | FLOAT | NULL |
| FedEndPartShNetUnrecsec704c | FLOAT | NULL |
| Fed21 | VARCHAR(1) | NULL |
| Fed22 | VARCHAR(1) | NULL |
| GrossreceiptsFirstPrecedingYear | FLOAT | NULL |
| GrossreceiptsSecondPrecedingYear | FLOAT | NULL |
| GrossreceiptsThirdPrecedingYear | FLOAT | NULL |
| TotalECIGrossReceiptFirstYear | FLOAT | NULL |
| TotalECIGrossReceiptSecondYear | FLOAT | NULL |
| TotalECIGrossReceiptThirdYear | FLOAT | NULL |
| GuaranteedPaymentsServices | FLOAT | NULL |
| GuaranteedPaymentsCapital | FLOAT | NULL |
| UsWithholding | FLOAT | NULL |
| IncentiveFee | FLOAT | NULL |
| ForeignTaxes | FLOAT | NULL |
| SpecialAllocation1 | FLOAT | NULL |
| SpecialAllocation2 | FLOAT | NULL |
| OtherIncreases | FLOAT | NULL |
| OtherDecreases | FLOAT | NULL |
| RevalTransfer | FLOAT | NULL |
| TransferCode | VARCHAR(50) | NULL |
| TransferPercentage | FLOAT | NULL |
| PercentWithdrawal | FLOAT | NULL |
| PYRedemptionPayables | FLOAT | NULL |
| CYRedemptionPayables | FLOAT | NULL |
| CheckIfDesDueToSale | VARCHAR(50) | NULL |
| CheckIfDesDueToExchange | VARCHAR(50) | NULL |
| CheckIfLiabSubjectToObligation | VARCHAR(50) | NULL |
| CashMktSec19AAndItemL | FLOAT | NULL |
| CashMktSec19AOnly | FLOAT | NULL |
| DistSubj73719BAndItemL | FLOAT | NULL |
| DistSubj73719BOnly | FLOAT | NULL |
| OtherPropDistFmv19COnly | FLOAT | NULL |
| OtherPropDistBasis19CAndItemL | FLOAT | NULL |
| DeemedMoneyLiabDec19DAndItemL | FLOAT | NULL |
| DeemedMoneyLiabDec19DOnly | FLOAT | NULL |
| SvcDistCashMktSec19FAndItemL | FLOAT | NULL |
| SvcDistCashMktSec19FOnly | FLOAT | NULL |
| SvcDistPropFmv19GOnly | FLOAT | NULL |
| SvcDistPropBasis19GAndItemL | FLOAT | NULL |
| PshipAssumePartnerLiab | FLOAT | NULL |
| PartnerAssumePshipLiab | FLOAT | NULL |

### dbo.enu_customallocations

| Column | Type | Nullable |
|---|---|---|
| AllocationTypeID | INT IDENTITY | NULL |
| AllocationType | VARCHAR(100) | NULL |
| ClientID | INT | NULL |
| TaxPeriodID | INT | NULL |
