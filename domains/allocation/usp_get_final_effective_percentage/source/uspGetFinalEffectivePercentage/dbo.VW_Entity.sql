CREATE VIEW [dbo].VW_Entity  
WITH SCHEMABINDING  
AS  
SELECT [EntityID]
      ,[EntityIdentification]
      ,[EIN]
      ,[DisplayName]
      ,[EntityName1]
      ,[EntityName2]
      ,[EntityName3]
      ,[Address1]
      ,[Address2]
      ,[City]
      ,[State]
      ,[Zip]
      ,[Country]
      ,[FundOrInvestmentID]
      ,[ClientID]
      ,[InvesttransID]
      ,[IsIssueK1]
      ,[UpdateDate]
      ,[IsPTP]
      ,[IsForeign]
      ,[IsCFC]
      ,[TaxBasisTypeID]
      ,[IRSServiceCenter]
      ,[PrimaryActivity]
      ,[CalendarOrFiscalYr]
      ,[FiscalTaxBeginning]
      ,[FiscalTaxEnding]
      ,[DateFormation]
      ,[EntityTypeB]
      ,[CountryCode]
      ,[IsExternal]
      ,[IsInCarry]
      ,[TaxPeriodID]
      ,[AllocationTypeID]
      ,[TaxClassID]
      ,[Province]
      ,[TransactionID]
      ,[ContactName1]
      ,[ContactName2]
      ,[ContactTitle]
      ,[ContactCompany]
      ,[ContactAddress1]
      ,[ContactAddress2]
      ,[ContactCity]
      ,[ContactState]
      ,[ContactProvince]
      ,[ContactPostalCode]
      ,[ContactCountry]
      ,[ContactCountryCode]
      ,[ContactPhone]
      ,[ContactFax]
      ,[ContactEmail]
      ,[AssetClassID]
      ,[PrimaryOrTrueUp]
      ,[IncludeInDebtAllocation]
      ,[DateK1Expected]
      ,[CurrencyCode]
      ,[GeographyClassID]
      ,[StrategyClassID]
      ,[HoldingVehicle]
      ,[BusinessUnitId]
      ,[FundGroup]
      ,[GPAdvisor]
      ,[LegalAddress]
      ,[CountryofEstablishment]
      ,[LegalEntityType]
      ,[EntityDescription]
      ,[UnderlyingFund]
      ,[AcceptsInvestorMoneySec1471]
      ,[ClassificationID]
      ,[FFIID]
      ,[FFIEIN]
      ,[ForeignEIN]
      ,[WFPAgreement]
      ,[TaxFormTypeID]
      ,[TaxReturnsTypeID]
      ,[StateReturns]
      ,[DueDates]
      ,[IsActive]
      ,[Filings5471]
      ,[Filings8621]
      ,[Filings8858]
      ,[Filings8865]
      ,[FiledTypeID]
      ,[AOGEntity]
      ,[TaxableIncomeWorkbook]
      ,[TaxPreperer]
      ,[FATCAStatusID]
      ,[FATCAWithholdingRate]
      ,[AssetType]
      ,[StartTaxPeriodId]
      ,[EndTaxPeriodId]
      ,[TransferThreshhold]
      ,[FundsOwnershipPercentageBit]
      ,[USInvestorsOwnershipPercentageBit] 
      ,[ForeignFundOwnershipBit]
      ,[ClassOfEquityOwnedByUAW]
      ,[EquityVotingRights]
      ,[Form8832Election]
      ,[DirectOwnership]
      ,[FundsOwnershipPercentage]
      ,[The926Filings]
      ,[Custom01]
      ,[Custom02]
      ,[Custom03]
      ,[Custom04]
      ,[Custom05]
      ,[Custom06]
      ,[Custom07]
      ,[Custom08]
      ,[Custom09]
      ,[Custom10]
      ,[Custom11]
      ,[Custom12]
      ,[Custom13]
      ,[Custom14]
      ,[Custom15]
      ,[Custom16]
      ,[Custom17]
      ,[Custom18]
      ,[Custom19]
      ,[Custom20]
      ,[ConfigureStateThresholds]
      ,[ApportionmentFlowUp]
      ,[LegalInvestmentName]
      ,[ReferenceId]
      ,[ForeignCharacterization]
      ,[PEAttribute]
      ,[TaxDocument]
      ,[EntitySubType]
      ,[IsPFIC]
      ,[DealStatus]
      ,[IsDomesticBlocker]
      ,[IsHolding]
      ,[IsFeeder]
      ,[OldAssetClassID]
      ,[InvestmentType]
      ,[ShortName]
      ,[IsSuspendedLossDisabled]
      ,[HideTaxCapitalDetails]
	  ,[ValueOfShareatYE]
	  ,[EntityCategory]
	  ,[LowerTierTieringStatus]
	  ,[DateOfAcquisition]
      ,[SecondaryInvestment]
	  ,[FunctionalCurrencyPerK1]
	  ,[FunctionalCurrencyBasisTracking] 
	  ,[IsQualifiedForeignCorporation]
      ,[XtractInvestmentID]
  FROM [dbo].[Entity];

