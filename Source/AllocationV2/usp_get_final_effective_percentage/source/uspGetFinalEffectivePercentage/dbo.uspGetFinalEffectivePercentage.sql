CREATE PROCEDURE [dbo].[uspGetFinalEffectivePercentage](
@EntityID INT, 
@ClientID BIGINT, 
@TaxPeriodID INT,
@RunID INT,
@Mode INT,
@IsPEModel BIT=0
) 
WITH RECOMPILE
AS
BEGIN  
/* ==============================================================================================================   
Author		Date		Comment  
Dilip       10/10/2018  Initial Creation.TPG Cost Allocation Redesign Changes.
Satish P    11/26/2018  Modified for State allocations
Kirti K		12/01/2018	TFS#2596081: Added distinct to pick lower tiers correctly for pfics
Dilip       12/04/2018  TFS #2599513 TPG Cost allocation Changes to handle -1 deal id for all underlying case. 
Shiv		12/20/2018  PBI#2606790:Custom Rule Allocation Methodology.  
Dilip		01/13/2019   Custom Allocation redesign changes. 
Dilip      01/28/2019   Custom Allocation redesign changes. Adding TrackingKey and Tag. 
Anshu M		01/31/2019	TFS #2650321 - Fixed issue in calculating cost percentages.
Pavan R		01/31/2019	Included Form199A
Guru        03/06/2019  TFS# 2691351 - TPG - 199A Effective Allocations 
Guru        03/06/2019  TFS# 2691351 - TPG - 199A Effective Allocations - Rules changes
Guru        03/13/2019  TFS# 2691351 - TPG - 199A Effective Allocations - Rule 2 fix
Pavan		03/20/2019	Adding linetype join for 199A effective calculations
Guru        03/20/2019  TFS# 2717359 - TPG - 199A Effective Allocations - Rule 2 - Removed the unwanted code and added Lineid join
Satish P    03/28/2019  TFS# 2725338 Added a filter on cost allocation type for 199A logic
Kirti K		04/10/2019	TFS#2732455: TPG-199A pick max quarter regarless of partner.
Dilip/KK	06/04/2019	TFS# 2782096: fix for Form 199a to have custom allocations work .
Davinder	06/04/2019	TFS 2750079: Optimization of expensive queries
Sarath      07/02/2019  TFS#2782265:TPG - Clean Up Footnote IDs
Davinder	07/12/2019	TFS 2782266: Calculate Cost Effective percentage excluding Transfer Effective percentage based on Exclude from transfer flag
Dilip/Sarath 8/072019   TFS#2782265:TPG - Clean Up Footnote IDs
Dilip        08/28/2019 TFS#2782266: Fixed Isexclude from transfer input load for Footnote lines. 
Dilip        08/28/2019 TFS#2782266: Fixed the join between footnote lines input and custom allocation table to pick up the correct allocation type. 
Dilip        09/27/2019 TFS#2805122: Changes to move offset allocation as a part of custom allocation logic. 
Dilip G     11/24/2019  ADO#172465 - Adding cost percentage total underlying changes. 
Kirti K		01/21/2020	ADO#179618 -Underlying entity value was failing on temp table hence created and then insert data.
Kirti K		01/22/2020	ADO#170984/179816: Correcting the join condition to avoid syntex errors.
Dilip       01/24/2020  ADO#180182 - Fixing underlying type issue while loading cost percentage. 
Dilip       03/05/2020  PBI#183818: Change to use waterfall model for allocation rule. 
Raja		04/20/2020	PBI#187111 ? GSAM | Default Allocation Rule | Allocation Changes 
Jacob M		05/20/2020 ADO#191977 - Updating AllocationType text to accommodate more user input characters
Guru        05/29/2020 ADO#192771 - Add Lookupvalue condition to check 199A config
Guru        05/31/2020 ADO#192771 - Exclude 199A lines which is configured in Cost Percentage import
Guru        05/31/2020 ADO#191977 - Add "Exclude Transfer" flag for 199A Lines
Ankit G		05/19/2020	ADO#191414: Replaced the default 0 value with actual for IsExcludefromTransfer.
Raja		06/04/2020 ADO#192782 - PBI 192782: GSAM | Default Allocation Rule | Override Asset Class Configuration for Partnership level & Look through investments	
Kirti K		06/09/20202 ADO#191414/192771 Merge the existing code changes to final effective SP
Raja		06/30/2020 ADO#194876 - Adding Tracking key for asset class population when lookthrough configuration for asset class in unconfigured  
Subbu S     07/09/2020  Modified to include custom allocation rules in PE Model PBI 189072
Subbu S     07/24/2020  Modified to correct merge issues
Dilip G		07/27/2020	ADO# 191611: Manage State Default Rules id update
Davinder	07/28/2020	ADO 196885: Issue with IdentityColumn of table #TempCostPercentage_Snapshot
Raja		07/19/2020 Modifying Asset class population as asset class varies when flowing to different uppertiers as per new requirement
Raja		07/27/2020 ADO#196518 - Including Box JKL lines in Allocation By Amount logic
Subbu S     07/30/2020  Corrected the tracking key merge issue for footnotes in PE Model
Kunal		08/11/2020 PBI#189877: Added logic to include transfer by dates
Raja		08/12/2020	selecting distinct percentages as amounts are double allocating when mode = 1
Subbu S     09/15/2020  Modified to fix the bug 200226
Vipin G		09/18/2020  Reverted State DAR and state allocation data import changes.
Raja		09/29/2020	ADO#201228 : Correcting logic to pickup non dated state lines 
Shiva/Raja	10/08/2020	ADO#201737: Allocation not working based on the tracking key scenario
Utsav D		10/14/2020	ADO#202366: Deleting only K1Lines from #TempBookEffectiveData from mode = 1 and PE Model = 1.
Pavan R		10/29/2020	Corrected duplicate insert of percetnages correcponding to investmentid = -1
Raja		11/04/2020	ADO#203744 : selecting distinct lines as incorrect percentages are calcualted fro Transfers because of lineid inclusion
Pramod K	11/05/2020	ADO#203301: Addressing duplicate data issue to resolve issue with Cost Percentage > 100%.
Raja		11/11/2020	ADO#172971 - Federal to Box JKL Mapping Allocation Changes
Anshu M		11/26/2020	PBI 197535 - TPG Capital | Footnotes | Update related to quarterly allocation 
Shampa D	12/7/2020	ADO#205782 - Selecting distinct lines while inserting into #tmpunderlingdated table to prevent -ve values getting inserted in finaleffective
Aditya V	12/10/2020	ADO#205979 - Changed int to varchar type of tracking key and tag when @IsDatedTransfersConfigured is 'C'
Raja		12/16/2020	ADO:206202: Integrating Tracking Key changes for footnotes
Abhinav S   1/4/2021    ADO:2066880 - Fixed #TempBookEffectiveData to take null for Trackingkey as it is checked later for null check
Raja		01/05/2021	ADO#208101: Issue in Box JKL allocation - Correcting LineTypeId join when mode = 1
Abhinav S   01/07/2021  Bug 204690: CAR | Footnote lines duplicating in CAR import
Raja		01/29/2021	Bug 211120: KKR | Transfer : Populating LineTypeID in Transfers
Raja		01/29/2021	Bug 211467: AGM | | PE Model | Adding missing LineTypeID Join to prevent double allocation
Pramod K    01/22/2020  ADO 210258TPG AtRisk and Passive Activity Reporting TY 2020 Allocation Changes
Manohar A	02/03/2021	ADO#211616: avoiding footnote duplicate lines issue while inserting into fnfinal as lineid will be -1
Raja		02/09/2021	Product Backlog Item 209621: GSAM | Request for Fund Level Residual allocation
Shiv	    02/11/2021  PBI#211786:Form 8865 - Tiering & Bi Reporting - Allocation  changes
Abhinav S   02/22/2021  Bug 212720 - At-Risk line not following fed effective allocation when corresponding federal line is following CAR 
Kirti K		02/22/2021	ADO#212794 - At risk fix for k1line when amount is zero.
Manohar A	02/24/2021	ADO#213529 - Footnotes are not following cost
Raja		02/17/2021	Bug 212884: KKR | Allocation data import | Correcting logic for CAR import when underlyingtype is "Entity total" 
Guru        03/15/2021  Bug 215309: Add Null check to Excludefrom transfer column
Subbu S     04/12/2021  Modified to add At risk lines for DAR import ADO 210740
Guru        04/22/2021  Bug 217052: NON 7216 | BDT Capital Partners | Allocations | Some of the rules are not working with entity total
Subbu S     04/22/2021  Modified to fix the bug ADO 217787
Guru        04/30/2021  ADO#217052: Handle Transfer Cost Adj Percent for -1 Underlying Scenerio
Abhinav     05/21/2021  Bug 219503: QA Regression June | At Risk | Entity total rules are not getting picked up for allocations via CAR import
Kirti K		05/06/2021	ADO#219330/218835 - Calc Slowness fix for Underlyings numbers.
Kirti K     05/21/2021  ADO#220290 checkin for asset class updates.
Satish/KK   06/16/2021  ADO#221833 926 Allocation not going through DAR fix by not applying rounding when DAR when > or < 100
Rakesh/Anubhav   07/16/2021  ADO223685: Added a JOIN condition to avoid cross join
KK/Guru		08/11/2021  ADO#225895: Handle entity total scenarios when there is diamond structure and allocation data trackign keys are blank.
/Satish
Kirti K		08/23/2021	ADO#223252: Fix the footnote allocation issue for entity and total and rule scenarios and fix ranking condition.
Vivek       08/26/2021  Build Issue : Removed 1 DROP statement on #TempAllUnderlyingsStatesOrdered
Davinder    07/25/2021  Bug 224861: Rollback of change made where we made LineID -1 for Mode 2. LineID is required for Form 199a
Guru        09/7/2021   ADO#225904: Fix Rounding and Pluggin logic
Aditya      09/14/2021  ADO 227856 : Modified join on Tracking key for Entity Total
Guru        09/17/2021  ADO#228173 : Revert IFF Investment ID condition for K1 Only 
Kirti K		09/20/2021	ADO# 228261/228262/228287 :handle the null transactiondate condition by adding 0 as Q0.
Guru        09/20/2021  ADO#228273 : Handle -ve Cost Percentage issue by spilting the Tracking key match query
Pramod K	09/27/2021	ADO#215218: Handle scenario when Rounding Logic = Plugged to GP and GP partner has no percentages provided in allocation data.
Guru        09/28/2021  ADO#228605: Fix negative allcoation with K1 Only and Entity Total combination 
Guru        09/29/2021  ADO#228877 : Fix precedence of Underlying type when there are both ET and K1 Only rules 
Guru        09/30/2021  ADO#228872 : Fix k1 Only scenerio by adding a null check and a left join  
Guru        10/03/2021  ADO#228730 : Fix -ve state alloaction . Delete data from temp tables which is already picked for appying percentage 
Kirti K/    10/05/2021  ADO#228693/228696/228907 : Fixing transfers by adding trackeingkey. also fixing Footnote for default rules with CAR 
Satish
Aditya      10/06/2021  ADO229178   : Modified join on Entity Hierarchy query for Entity Total & k-1 only
Kirti K    10/05/2021  ADO#228693/228696 : Fixing transfers by removing cost percentage additional logic
Guru       10/8/2021   ADO# 229372 : Pick max HLevel then there are more than 1 HLevels
Kirti K		10/13/2021	ADO#228590 : Adding index to table #TempAllUnderlyingsCombined
Shraddha    10/08/2021  Reverting changes for ADO#215218
Praneeth B  11/30/2021 TPG Capital | KKR | Imports/Allocations | PROD 2020 | Show CAR breaking out of footnote by stream Pt.3
Aditya V    03/29/2021  PBI#239988: Custom footnote Import: Allocation Changes
Aditya      03/05/2022 ADO241993 Adding default parameter to udfGetLatestCustomFootnoteTransactionIDs 
Shiv        04/27/2021  Bug 242881: KKR | 926 | Transfer logic is not working for 16(a) and 16(b)
Kirti K		05/20/2021	ADO#245150: KKR | 926| Fixed transfer logic if the date doesnt lies in the transfer date it and should flow yearly.  
Anupama S   05/25/2022  PBI 222968: Partners Group - IPC/IPC2 Sync | Asset Class Override Import
Subbu S     06/08/2022  Modified to fix the Bug 246160: Precedence logic is working incorrectly when we have entity total and asset class
Subbu S     06/17/2022  Modified to fix the Bug 235988: QA2 | Regression | Negative allocations for Fed and States in Cost and CAR allocations

Kirti K		06/03/2022	ADO#246300: KKR | 926| Adding the Quarterdate condition to handle this logic for dated transfer.


Anshu M     06/30/2022  PBI 241688: Optimization for 'UspGetFinalEffectivePercentage' SP. Removed PartnerNumber column from #TempAllUnderlyingsCombined
                        as it was not being used anywhere. Added cosetpercentage_snapshot in a temp table.
Rishu G     07/13/2022 Bug 249405: Energy Capital Partners | Data | Entity is erroring out due to incorrect data types on data
Siva Balaji 07/19/2022  Bug 248991: Negative allocations for Fed and States in Cost and CAR allocations | corrected join condition to include TypeID
Guru/Siva Balaji 07/22/2022  Bug 247692: Transfers are not working as expected with ranking logic for Cost Allocations | implemented ranking logic for transfers
Anshu M     08/05/2022  Reapplying fix which got overwritten during optimization-  Bug 246160: Precedence logic is working incorrectly when we have entity total and asset class
Rakesh N    08/04/2022 Added NOLOCK & Removed unused column Partner Number
Satish/KK		09/08/2022	ADO 255148: GSAM - Calc slowness - Removed Partner Number from #TempCostUnderlyingTypes
Raaghav M   09/14/2022  Task 248196 - Multiple Clients | Set 'Form199AEffectivePercentageLogic' Config Value To 0 for Clients Other Than TPG
Anubhav G   09/15/2022 Task 252063: corrected the position of #CostPercentage_Snapshot table
Aditya      09/19/2022  PBI252140: TPG Capital | PFICs | PFIC Allocations
Aditya      10/03/2022  ADO257338 : Ignoring blank date of distribution values along with various for part v lines
Raaghav M/  09/29/2022  Bug 256686 - Global Infrastructure Partners - Final | Reports | Allocation method is not correct for some of the lines
Guru
Rishu/Yash   10/06/2022 Bug 257253: Partners Group | Asset Class Override Import | Issue with override for investment with no predefined Asset Class in ER
Rakesh N    10/12/2022  Bug 255008: Farallon | Stuck Calculation | Workflow is taking longer time than expected
Vivek       11/22/2022  PBI259383: All Clients | Tax Capital Transfer Logic Updates - Part 2
Raaghav M/  12/05/2022  Bug 261567 - Apollo Global Management | Imports | Monthly Tagging Not Working Correctly
Guru
Pavan R     02/02/2023  Including custom footnote line types for CAR allocation entities
Rahul S/ 
Guru        03/13/2023  Bug 273377: TPG Capital | Allocations/Imports | 199a Allocations not following Cost percentage
Shraddha    04/03/2023  Bug 276013: Realterm | Schedule K Equivalent Report | Liabilities section needs correct amounts to tier
Nikhil P    04/10/2023  Product Backlog Item 274989: Sprint11 Performance Fixes
Arpitha 	05/01/2023	Code CleanUp : Redundant WorkflowIDs Calculation removed by fetching from AllocationRun.
Pavan R     05/12/2023  Transfers with DAR
Shiv        07/12/2023  Product Backlog Item 260081: Apax Partners | Foreign Corp | Update Foreign Corp Part V logic
Shiv        07/19/2023  Bug 287974: Amounts is 6A,7A and 7A long term lines are getting doubled with Config ON and even with Config OFF some extra amount is getting added
Shiv        07/28/2023  Bug 288979: Apax Partners | Foreign Corporation Report | Allocations for PFICs are not working as per dates and Transfer Set Up.
Shiv        08/01/2023  Bug 289282: Apax Partners | Foreign Corporation Report | Allocations for PFICs are not working as per Transfer Logic.
Ankush U    07/18/2023  Bug 287467: Fixed Quarter comparison condition.
Xiaoping    08/07/2023  Task 286919: Blocking: uspGetFinalEffectivePercentage
Rishu G		09/05/2023  Bug 293527: QA1 - TY23 | Calc is failing
Davinder	 09/06/2023	PBI 286514: Exclude deleted investments which are present in Allocation Data import
Aditya V    09/28/2023  Bug 297059: TY23 QA2 | Transfer | PE Book Entity is not working for Cost allocation and Dated Line Transfer.
Saptarshi/ 10/17/2023 Bug 298971: Partners Group | Asset Class Override | Footnotes Data not Overriding
Keerthi
Anshu 	    10/25/2023  PBI 285877: Master Standard Site - 704c | Make PE book compatible with 704c allocation logic
Pavan R     10/30/2023	Filtering the matching asset class and then removing unmatched one with left join. Handling this for diverging-converging structures
/Rakesh N
Abhinav S   11/29/2023  Removing entity join for investment as it fails for asset class condition
Raaghav M   12/12/2023  Bug 306669 - Multiple Clients | Reports | Incorrectly Allocating Amounts by Pro Rata
Santosh/Guru 12/19/2023 Bug 307581: Vista Equity Partners | Investment Tagging and Allocations | Flow Up Amounts Applying Correct Tag But Incorrect Allocations
Meera J     12/20/2023  Bug 307417: Regression_Asset Class percentages are ignored in the Reports
Anshu M     12/21/2023  Bug 307862: Fixed issue with immediatelowertier in case of K-1 only at local entity.
Arpitha     11/30/2023 Task 303112: KKR | Calc Optimization
Aditya V    01/17/2024  Bug 310229: Apollo Global Management | Workflow | Calcs are taking longer than normal
Ankush U    01/24/2024  Bug 311167: Add config for getfinaleffective with tag.
Rahul S/Nagesh  01/24/2024 Bug 311167: Performance issue fix
Aditya/Anupama    01/25/2024  BUG 311167 REMOVED DISTINCT
Nagesh         01/25/2024 BUG 311167 Removal of tag and populating tag from cost percentage snapshot
Raaghav M   01/15/2024  Product Backlog Item 300796 - Allocation changes related to 704c to K1 mappings. Added logic to create custom rules for 'By Amount' allocations
Anupama/ Prabandha  03/18/2024  Bug 318012: Francisco Partners | Workflow Error | Unable to Run Calculation Due to Workflow Error "Conversion failed when converting the varchar value"
Raaghav M   03/21/2024  Product Backlog Item 314661: Hedge Standardization | Make PE book compatible with 704c allocation logic - 704c to K1 line mapping- Entity Total
Prudhvi     05/21/2024  Product Backlog Item 317699: Vista | Dynamic Master K-1 Enhancement | Ability to show/hide columns in import
Raja		06/20/2024	Product Backlog Item 316403: Apollo Global Management | Other | 704c Carry Allocations to Feed Directly into Offset Import
Siva Balaji/Ankush 06/27/2024  Bug 331798/324261: Adding ISNULL() check while comparing tracking key from Costpercent_Snapshot table.   
Meera J		07/11/2024	Bug 332945: TPG Capital | Calcs | Slowness & Unclear Error Reports
Pavan R     08/07/2024  Enabling capability of monthly allocations
Hemant J	08/29/2024  Task 333633: Tech Task | Allocation | Run End Date not getting populated
Utsav D     09/07/2024  Bug 340444: Removing logic to exclude small amounts for BoxJKL lines while fetching from LookthroughAllocationInput.
Anubhav G   09/11/2024  ADO 340846: Correcting LEFT JOIN condition while calculating Entity Hierarchy in #EntityHierarchy table
Pavan R     10/15/2024  Fixing the comparision of Quarters for quarterly transfers logic, as there are both Quarters and Months in QuarterMonth category
Rishu G     11/20/2024  Bug 345866: GSAM - PEG | Workflow Status Report | Update Nonexistent Upper-Tiers Validations message
Raaghav M   12/04/2024  Bug 347522 - Added plugging logic for '704c with PE Book' allocations
Karthikeyan A   01/20/2025  Bug 352831 - Added null check for Comments column from ENU_DF_DataList
Anupama S   02/24/2025      Bug 356608: QA1 TY24 | Calc is failing with deadlock error
Nagesh      05/19/2025      Bug 331593:Foreign Corp Allocations | Foreign Corp Report not Following DAR Custom Allocations - PF
Nagesh      06/22/2025      Product Backlog Item 336024: Default Allocation Import | Exclude from Transfer functionality in DAR to work for All Allocations - Hotfix 
Raja		07/04/2025		Bug 368717: QA1 TY24 | Populating ExcludeFromTransfer column correctly for amounts
Nagesh      07/08/2025      Bug 368837: QA1 TY24 | 'Exclude from transfer' is not working for States Amount Rule
Ankush      07/31/2025      Bug 371428: To fix issue of effective percenatges getting calculated wrong when the tracking key is different from the flow up investments and Override Indirect Look through Asset class is OFF
Shraddha    08/07/2025  PBI 371807: Allocation: Calculation optimization - Deadlock Fixes
Rahul S     09/05/2025  Bug 376020: To fix the issue with State lines not following DAR rule percentages
Karthikeyan 09/13/2025  Bug 377227: Transfers Report | BOD and EOD logic not working as expected for Dated Lines
Ankush U    11/26/2025  Bug 383761: Fixed Quarter comparison condition when it is string type.
Anubhav G   12/04/2025  ADO 384955: Correcting Quarter comparison condition when it is string type.
Co N        01/16/2025  Bug 387074: Workflow error on setting up Monthly allocations
Rohan R     02/12/2026 Product Backlog Item 377585: Tech | Optimize Long running Import, Reports
 =================================================================================================================*/     

SET NOCOUNT ON    
  
DECLARE @LocalEntityID INT = @EntityID    
   , @LocalClientID INT = @ClientID    
   , @LocalTaxPeriodID INT = @TaxPeriodID    
   , @LocalRunID INT = @RunID
   , @LocalMode INT = @Mode
   , @LocalIsPEModel BIT = @IsPEModel
   , @StartDate DATETime = GetDate()    
   , @RunStatus VARCHAR(50)             
   , @EndDate DATETIME    
   , @InvEntityTypeID INT       
   , @AllocationTypeID INT    
   , @YearlyWorkflowID INT    
   , @K1LineTypeID INT    
   , @AdjustmentLineTypeID INT       
   , @CostPercentageWorkflowID INT    
   , @PhaseID INT    
   , @LogID INT       
   , @SM_CustomAllocationEventTypeID INT    
   , @CustomAllocationWorkFlowID INT    
   , @SM_CustomAllocationWorkFlowID INT    
   , @CostAllocationTypeID INT    
   , @BookAllocationTypeID INT     
   , @OffsetAllocationTypeID INT, @LPOffsetAllocationTypeID INT, @GPOffsetAllocationTypeID INT    
   , @YearlyAllocationTypeID INT     
   , @BoxJKLLineTypeID INT    
   , @AtRiskLineTypeID INT  
   , @IsDatedTransfersConfigured CHAR(1)    
   , @AllocationTypeName VARCHAR(100)      
   , @IsCustomAllocationRuleEnabled CHAR(1)  
   , @IsForm199AEffectivePercentageLogic INT = 0
   , @IsPFICAllocationbyQuarter CHAR(1)
   , @704cAllocationTypeID INT
   , @704cAllocationTypeName VARCHAR(100) = ''
   , @IsDARSetup INT = 0
   , @StateInput INT

DECLARE @PartVAllocated BIT
DECLARE @allocationTypeIDfor704c INT



select @allocationTypeIDfor704c=AllocationTypeID from enu_customallocations where AllocationType='704c'

SELECT @PartVAllocated = CASE WHEN GM.State ='C' THEN 1 ELSE 0 END
FROM GlobalMenu GM (NOLOCK) JOIN ENU_GlobalMenuGroup EG ON EG.GlobalMenuGroupID = GM.GlobalMenuGroupID
WHERE EG.GroupName='K3 Part V Configuration' AND GM.MenuName='Allocate Part V using distribution date percentage'  


SELECT @K1LineTypeID = LineTypeID     
FROM ENU_LineType    
WHERE ClientID = @LocalClientID     
 AND TaxPeriodID = @LocalTaxPeriodID    
 AND LineType = 'K1'  
  
SELECT @AdjustmentLineTypeID = LineTypeID  
FROM ENU_LineType    
WHERE ClientID = @LocalClientID     
 AND TaxPeriodID = @LocalTaxPeriodID    
 AND LineType = 'Book K-1 Adjustments'
 
SELECT @StateInput = LineTypeID     
FROM ENU_LineType    
WHERE ClientID = @LocalClientID     
 AND TaxPeriodID = @LocalTaxPeriodID    
 AND LineType = 'State Input' 
 
SELECT @IsDARSetup = CASE WHEN ISNULL([State], 'U') in ('C','CG') THEN 1 ELSE 0 END   
FROM GlobalMenu GM  
INNER JOIN ENU_GlobalMenuGroup EN ON EN.GlobalMenuGroupID = GM.GlobalMenuGroupID  
WHERE GM.ClientID = @LocalClientID    
AND GM.TaxPeriodID = @LocalTaxPeriodID    
AND GM.MenuName = 'Default Allocation Rule'    
AND EN.GroupName = 'Other Logic/Imports'

  
CREATE TABLE #TempAllocationInput(RunID INT,ClientID INT,EntityID INT,LineTypeID INT,LineID INT,Amount FLOAT,QuicklinkID INT,Amount704b FLOAT,Tag VARCHAR(5000), TrackingKey  Varchar(5000))  
  
CREATE TABLE #TempSMLookThroughAllocationInput(RunID INT,ClientID INT,EntityID INT,LineTypeID INT,StateID INT,StateLineID INT,Amount FLOAT,QuicklinkID INT, TrackingKey VARCHAR(4000),Tag VARCHAR(5000))  

CREATE TABLE #tmpPartVQuarters(PFICFootnoteID INT,QUARTER VARCHAR(6), TextValue VARCHAR(100))

CREATE TABLE #DefaultAllocationRuleSetup(TransactionID INT, RuleID INT, AllocationPercentageTypeID INT, AllocationByID INT, UnderlyingTypeID INT, RuleTypeID INT, RuleGroupID INT, ClientID	INT, TaxPeriodID INT, EntityID INT)

CREATE TABLE #MapDefaultAllocRuleToLineItem(TransactionID INT, SourceID INT, StateID INT, SelectedMappingID INT, RuleID	INT, ExcludeFromTransfers INT, ClientID INT, TaxPeriodID INT, EntityID INT)

CREATE TABLE #Mappings(EntityID INT, MapLineID INT, DatabaseName VARCHAR(50), RegisterLineID INT, Formula VARCHAR(20), FieldSourceID INT)

EXEC uspAddAllocationLog @LocalClientID, @LocalTaxPeriodID, @LocalRunID,   
  'Get Final Effective Percentages', 'UspGetFinalEffectivePercentage', @StartDate, NULL, @LocalMode, @LogID output;  

IF(@LocalMode = 2 OR (@LocalIsPEModel=1 AND @LocalMode=1) OR @LocalMode = 4)  
-------------------------------------------------PFIC LINES----------------------------------------------------------------  
BEGIN   
  

IF (@LocalIsPEModel = 0 )  
BEGIN  
 INSERT INTO #TempAllocationInput (RunID,ClientID,EntityID,LineTypeID,LineID,Amount,QuicklinkID,Amount704b,Tag, TrackingKey)  
 SELECT RunID,ClientID,EntityID,LineTypeID,LineID,Amount,QuicklinkID,Amount704b,Tag,TrackingKey  
 FROM AllocationInput AI (NOLOCK)     
 WHERE RunID = @LocalRunID  AND CASE WHEN ISNULL(Amount,0) = ISNULL(Amount704b,0) THEN Amount ELSE Round(ISNULL(Amount,0),0) END <> 0  AND LineTypeID NOT IN (@K1LineTypeID, @AdjustmentLineTypeID)    
 AND ClientID = @LocalClientID   
   
END  
ELSE  
BEGIN  
  
 INSERT INTO #TempAllocationInput (RunID,ClientID,EntityID,LineTypeID,LineID,Amount,QuicklinkID,TrackingKey)  
 SELECT PEFundRunID,ClientID,InvestmentID,LineTypeID,LineID, InitialAmount ,QuicklinkID, InvestmentID  
 FROM PE_AllocationInput (NOLOCK)     
 WHERE PEFundRunID = @LocalRunID  AND LineTypeID NOT IN (@K1LineTypeID, @AdjustmentLineTypeID)    
 AND ClientID = @LocalClientID   
  
  
END   


IF (@LocalMode != 4) AND NOT EXISTS (SELECT TOP(1) 1 FROM #TempAllocationInput)
BEGIN

SET @EndDate = GETDATE()  
EXEC [dbo].[uspUpdateAllocationLog] @LogID, @EndDate  

RETURN
END

END


IF(@LocalMode = 3)  
BEGIN  
  
  
IF (@LocalIsPEModel=0)  
BEGIN  
INSERT INTO #TempSMLookThroughAllocationInput(RunID,ClientID,EntityID,LineTypeID,StateID,StateLineID,Amount,QuicklinkID,Trackingkey,Tag)  
SELECT RunID,ClientID,EntityID,LineTypeID,StateID,StateLineID,Amount,QuicklinkID,Trackingkey,Tag  
FROM SM_LookThroughAllocationInput (NOLOCK)  
WHERE  RunID = @LocalRunID   AND Round(ISNULL(Amount,0),0) <> 0    
AND ClientID = @LocalClientID   
END  
ELSE  
BEGIN  
INSERT INTO #TempSMLookThroughAllocationInput(RunID,ClientID,EntityID,LineTypeID,StateID,StateLineID,Amount,TrackingKey)  
SELECT PEFundRunID,ClientID,InvestmentID,LineTypeID,StateID,StateLineID,InitialAmount,InvestmentID  
FROM PE_SM_AllocationInput (NOLOCK)
WHERE  PEFundRunID = @LocalRunID   AND Round(ISNULL(InitialAmount,0),0) <> 0    
AND ClientID = @LocalClientID   

END

IF NOT EXISTS (SELECT TOP(1) 1 FROM #TempSMLookThroughAllocationInput)
BEGIN

SET @EndDate = GETDATE()  
EXEC [dbo].[uspUpdateAllocationLog] @LogID, @EndDate  

RETURN
END

END    

CREATE TABLE #FinalCostPercentage (    
DealId INT,    
Partnernumber VARCHAR(50),    
Quarter VARCHAR(50),    
CommitmentPercent float,    
TypeId INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000) ,
[704cAllocationTypeID] INT,[704cPercentageType] VARCHAR(100), GPPartnerReceivingCarry BIT
)    
    
CREATE TABLE #TempCostPercentageMinQuarter (    
DealId INT,    
Quarter VARCHAR(50),    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),
Preference INT    
)    
    
    
CREATE TABLE #TempFinalEffectivePercentageNonDated(    
InvestmentID INT,    
PartnerNumber Varchar(50),    
EffPercentage float,    
AllocationType Varchar(255),    
Quarter Varchar(50),    
PickUpOrder INT,    
TypeId INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
LineTypeID INT,    
LineID INT,    
IsExcludefromTransfer BIT   ,
[704cAllocationTypeId] INT,[704cPercentageType] VARCHAR(100), GPPartnerReceivingCarry BIT,StateID INT
)    
    
    
CREATE TABLE #TempFinalEffectivePercentageDated(    
InvestmentID INT,    
PartnerNumber Varchar(50),    
EffPercentage float,    
AllocationType Varchar(255),    
Quarter Varchar(50),    
PickUpOrder INT,    
TypeId INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
LineTypeID INT,    
IsExcludefromTransfer BIT,
GPPartnerReceivingCarry BIT
)    
    
    
CREATE TABLE #TempUnderlyingsPickUpOrderDated(    
InvestmentID INT,    
LineTypeID INT,    
Quarter Varchar(50),    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
PickupOrder INT,    
IsExcludefromTransfer BIT)    
    
CREATE TABLE #TempSelectedNonDatedLines(    
InvestmentID INT,    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
LineTypeID INT    
)    
    
CREATE TABLE #TempInputLines(    
UnderlyingEntityID INT,     
LineID INT,    
LineTypeID INT,    
QuickLinkID INT,    
StateID INT,    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
IsExcludefromTransfer BIT    
)    
    
CREATE TABLE #TempNonDatedEntities(    
UnderlyingEntityID INT,    
LineTypeID INT,    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
IsExcludefromTransfer BIT ,
StateID INT      
)    
    
CREATE TABLE #TempDatedEntities(  
UnderlyingEntityID INT,  
LineTypeID INT,  
Quarter Varchar(50),  
TypeID INT,  
TrackingKey Varchar(5000),  
Tag Varchar(5000),  
IsExcludefromTransfer BIT,  
LineID INT ,
Transferdate Datetime,
Preference INT 
)    
    
    
CREATE TABLE #TempNonDatedEntitiesCost(    
UnderlyingEntityID INT,    
LineTypeID INT,    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
IsExcludefromTransfer BIT    
)    
    
CREATE TABLE #TempDatedEntitiesCost(    
UnderlyingEntityID INT,    
LineTypeID INT,    
Quarter Varchar(50),    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
IsExcludefromTransfer BIT    
)    
    
CREATE TABLE #TempAllEntities(    
UnderlyingEntityID INT, TypeID INT, TrackingKey VARCHAR(5000), Tag VARCHAR(5000)    
)    
    
CREATE TABLE #TempMinimumQuarter(    
DealID INT,    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
MinQuarter Varchar(50),
QuarterType VARCHAR(10)    
)    
    
CREATE TABLE #TempErrorUnderlyings(DealID INT)    
    
    
CREATE TABLE #TempCostPercentage (    
DealId INT,    
Partnernumber VARCHAR(50),    
Quarter VARCHAR(50),    
CommitmentPercent float,    
TypeId INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000)  ,  
TrackingKeyMatch Varchar(5000)  ,  
UnderlyingType INT,
[704cAllocationTypeID] INT,[704cPercentageType] VARCHAR(100), GPPartnerReceivingCarry BIT
)    
    
CREATE TABLE #Temp199ACostPercentage (    
DealId INT,    
Partnernumber VARCHAR(50),    
Quarter VARCHAR(50),    
CommitmentPercent float,    
TypeId INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
RuleNumber INT    
)    
    
CREATE TABLE #TempCostPercentageDeals (    
DealId INT,    
Quarter VARCHAR(50),    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000)    
)    
    
CREATE TABLE #EntityPartners(    
partnernumber varchar(50)        
)    
    
    
CREATE TABLE #TempTransfersAdjCostDefaultPercentage(    
InvestmentID INT,    
TransferPartnerNumber Varchar(50),    
TransferDate datetime,     
EndingCostPercent float,     
PartnerNumber Varchar(50),    
EffectivePercent float,    
TypeID INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),  
TrackingKeyMatch Varchar(5000),
Underlyingtype int
)    
    
CREATE TABLE #TempEntityUnderlying (              
UnderlyingEntityId INT,    
AssetClassId INT,    
TrackingKey Varchar(5000)    
)    
    
--CREATE TABLE #TempEntityUnderlyingWithTrackingkey (              
--UnderlyingEntityId INT,    
--Trackingkey varchar(4000),    
--TrackingkeyMatch varchar(4000)    
--)    
    
CREATE TABLE #TempBookEffectiveData(    
UnderlyingEntityID INT, LineID INT, FootNoteID INT, SourceID INT, AllocationTypeid INT,     
AdjustmentAllocationTypeID INT, TrackingKey VARCHAR(5000), Tag VARCHAR(5000), IsExcludefromTransfer BIT    
)    
    
CREATE TABLE #SM_TempBookEffective(    
UnderlyingEntityID INT, StateLineID INT, StateID INT, AllocationTypeid INT,     
AdjustmentAllocationTypeID INT, TrackingKey VARCHAR(5000), Tag VARCHAR(5000)    
)    
    
CREATE TABLE #TempTransferDate(    
UnderlyingEntityID INT, LineTypeID INT, QUARTER varchar(10), UnderlyingTypeID INT, UnderlyingTrackingKey VARCHAR(5000),     
UnderlyingTag VARCHAR(5000),TypeID INT, TrackingKey VARCHAR(5000), Tag VARCHAR(5000),InvestmentID INT,TransferPartnerNumber varchar(100),TransferDate datetime    
)    
    
CREATE TABLE #TempEnitityAllocationRule(LineId INT, UpdatedAllocationRuleID INT)    
    
CREATE TABLE #TempLookThroughAllocationInput(RunID INT,ClientID INT,EntityID INT,LineTypeID INT,LineID INT,Amount FLOAT,QuicklinkID FLOAT,Amount704b FLOAT,TrackingKey VARCHAR(400), Tag VARCHAR(5000))    

CREATE NONCLUSTERED INDEX IX_TempLookThroughAllocationInput ON #TempLookThroughAllocationInput(LineID)

CREATE TABLE #TempDefaultAllocationRule(LineId INT, AllocationRuleID INT , EntityID INT)    
    
CREATE TABLE #TotalUnderlyingAmounts (    
LineId INT,    
Partnernumber VARCHAR(50),    
TotalAmount float,    
CostEntityID INT,    
AllocationTypeId INT,TrackingKey VARCHAR(200),  Tag VARCHAR(200)  , LineTypeID INT , IsExcludefromTransfer INT  
)    
    
CREATE TABLE #FinalEffectiveAmounts (    
UnderlyingEntityID INT,    
LineId INT,    
Partnernumber VARCHAR(50),    
Quarter VARCHAR(50),    
TypeId INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
EffectiveAmount FLOAT,    
UnderlyingTypeId INT,    
LineTypeID INT,
GPPartnerReceivingCarry BIT,
IsExcludefromTransfer INT

)    
    
CREATE TABLE #FinalAmounts (    
InvestmentID INT,    
PartnerNumber Varchar(50),    
AllocationType Varchar(255),    
Quarter Varchar(50),    
PickUpOrder INT,    
TypeId INT,    
TrackingKey Varchar(5000),    
Tag Varchar(5000),    
LineTypeID INT,    
LineID INT,    
IsExcludefromTransfer BIT,     
EffectiveAmount FLOAT,    
UnderlyingTypeId INT,
GPPartnerReceivingCarry BIT
)    
    
CREATE TABLE #TempAllUnderlyings (    
Underlyingtype INT,    
UnderlyingEntityId INT,    
EntityId INT,    
TrackingKey  Varchar(5000),    
TrackingMatch  Varchar(5000),    
AllocationTypeId INT,    
LineID INT,    
RankForUnderlyingPickup INT,    
LineTypeID INT,    
AllocationBy Varchar(10),    
StateID INT ,    
IsExcludefromTransfer BIT    
)    
    
CREATE TABLE #TempAllUnderlyingsOrdered (    
Underlyingtype INT,    
UnderlyingEntityId INT,    
EntityId INT,    
TrackingKey  Varchar(5000),    
TrackingMatch  Varchar(5000),    
AllocationTypeId INT,    
LineID INT,    
RankForUnderlyingPickup INT,    
LineTypeID INT,    
AllocationBy Varchar(10),    
StateID INT  ,    
IsExcludefromTransfer BIT  
)    
    
CREATE TABLE #TempAllUnderlyingsFNOrdered (    
Underlyingtype INT,    
UnderlyingEntityId INT,    
EntityId INT,    
TrackingKey  Varchar(5000),    
TrackingMatch  Varchar(5000),    
AllocationTypeId INT,    
LineID INT,    
RankForUnderlyingPickup INT,    
LineTypeID INT,    
AllocationBy Varchar(10),    
StateID INT ,
IsExcludefromTransfer INT
)    
  
CREATE TABLE #TempAllUnderlyingsStatesOrdered (    
Underlyingtype INT,    
UnderlyingEntityId INT,    
EntityId INT,    
TrackingKey  Varchar(5000),    
TrackingMatch  Varchar(5000),    
AllocationTypeId INT,    
StateLineID INT,    
RankForUnderlyingPickup INT,    
LineTypeID INT,    
AllocationBy Varchar(10),    
StateID INT ,
IsExcludefromTransfer INT
)   
  
  
CREATE TABLE #LineItem    
(    
LineID INT,    
AllocationTypeRuleID INT,    
LineTypeID INT,    
TransactionDate DATETIME,    
IsTransactionDate BIT,    
IsTransfersAdjusted BIT    
)

CREATE TABLE #TempFilteredTransfersAdjCostDefaultPercentage(
	RunID bigint,
	ClientID int,
	EntityID int,
	InvestmentID int,
	PartnerNumber varchar(50),
	TransferPartnerNumber varchar(50),
	TransferAdjPercent float,
	EndingCostPercent float,
	TransferDate datetime,
	TransferDirection varchar(5),
	BeginningPercentUsage float,
	EffectivePercent float,
	AllocationComplete varchar(3),
	AllocationTypeID int,
	TrackingKey varchar(5000),
	Tag varchar(5000),
	Underlyingtype int,
  FormattedTrackingKey varchar(5000),
  FormattedEntityID int
)

CREATE TABLE #EntityTotalAmounts(UnderlyingEntityID INT, PartnerNumber VARCHAR(50), Quarter VARCHAR(50), CommitmentPercent FLOAT, AllocationTypeID INT, TrackingKey VARCHAR(5000), Tag VARCHAR(5000), LineID INT, InputAmount FLOAT, AllocatedAmount FLOAT, CostEntityID INT, UnderlyingTypeID INT, LineTypeID INT, GPPartnerReceivingCarry BIT)
    
SELECT @BoxJKLLineTypeID = LineTypeID       
FROM ENU_LineType      
WHERE ClientID = @LocalClientID       
AND TaxPeriodID = @LocalTaxPeriodID      
AND LineType = 'BoxJKL'    
    
SELECT @AtRiskLineTypeID = LineTypeID       
FROM ENU_LineType      
WHERE ClientID = @LocalClientID       
AND TaxPeriodID = @LocalTaxPeriodID      
AND LineType = 'At Risk'  
  
SELECT @InvEntityTypeID = EntityTypeID      
FROM ENU_EntityType      
WHERE ClientID = @LocalClientID      
 AND TaxPeriodID = @LocalTaxPeriodID      
 AND EntityTypeName = 'Investment'     
    
Select @CostAllocationTypeID = AllocationTypeID    
  From ENU_CustomAllocations (NOLOCK)    
  WHERE AllocationType = 'Cost'    
    
    
Select @BookAllocationTypeID = AllocationTypeID    
  From ENU_CustomAllocations (NOLOCK)   
  WHERE AllocationType = 'Book'    
    
Select @OffsetAllocationTypeID = AllocationTypeID    
From ENU_CustomAllocations (NOLOCK)    
WHERE AllocationType = 'Offset'    
    
Select  @GPOffsetAllocationTypeID =  AllocationTypeID    
From ENU_CustomAllocations (NOLOCK)   
WHERE AllocationType = 'GP Offset'    
    
Select  @LPOffsetAllocationTypeID =  AllocationTypeID    
From ENU_CustomAllocations (NOLOCK)    
WHERE AllocationType = 'LP Offset'    
     
      
Select @YearlyAllocationTypeID = AllocationTypeID    
  From ENU_CustomAllocations (NOLOCK)   
  WHERE AllocationType = 'Yearly'   
  
   SELECT @704cAllocationTypeID = AllocationTypeID FROM ENU_CustomAllocations (NOLOCK)
 WHERE AllocationType = '704c'

    
SELECT @YearlyWorkflowID = YearlyWorkflowID,
@CustomAllocationWorkFlowID=CARWorkflowID,
@CostPercentageWorkflowID=CostWorkflowID
FROM AllocationRun(NOLOCK)     
WHERE RunID = @LocalRunID    
    
SELECT @IsDatedTransfersConfigured = [State]      
FROM GlobalMenu GM (NOLOCK) INNER JOIN ENU_GlobalMenuGroup EG      
ON EG.GlobalMenuGroupID = GM.GlobalMenuGroupID      
AND EG.GroupName = 'Other Configuration'      
WHERE MenuName = 'Transfer by date'      
AND ClientID = @LocalClientID      
AND TaxPeriodID = @LocalTaxPeriodID      
    
SELECT @AllocationTypeName = AL.AllocationTypeName      
FROM ENU_AllocationLogic AL INNER JOIN VW_Entity E with(nolock)     
ON AL.AllocationTypeID = E.AllocationTypeID      
WHERE E.EntityID = @LocalEntityID       
AND E.ClientID = @LocalClientID    
  
SELECT @IsCustomAllocationRuleEnabled = [State]      
FROM GlobalMenu GM  (NOLOCK)    
WHERE MenuName = 'Custom Allocation Rule'      
AND ClientID = @LocalClientID      
AND TaxPeriodID = @LocalTaxPeriodID

SELECT @IsForm199AEffectivePercentageLogic = CASE WHEN ISNULL([State], 'U') = 'C' THEN 1 ELSE 0 END  
FROM GlobalMenu GM (NOLOCK)
INNER JOIN ENU_GlobalMenuGroup EN ON EN.GlobalMenuGroupID = GM.GlobalMenuGroupID
WHERE GM.ClientID = @LocalClientID  
AND GM.TaxPeriodID = @LocalTaxPeriodID  
AND GM.MenuName = 'Form 199A Effective Percentage Logic'  
AND EN.GroupName = 'Other Configuration'

SELECT @IsPFICAllocationbyQuarter = [State]        
FROM GlobalMenu GM (NOLOCK) INNER JOIN ENU_GlobalMenuGroup EG        
ON EG.GlobalMenuGroupID = GM.GlobalMenuGroupID        
AND EG.GroupName = 'ALLOCATION DATA'        
WHERE MenuName = 'PFIC Allocation Logic by Quarter'        
AND ClientID = @LocalClientID        
AND TaxPeriodID = @LocalTaxPeriodID

SELECT @704cAllocationTypeName = EL.[704cAllocationTypeName]
FROM EntityConfigurations EC (NOLOCK)
INNER JOIN ENU_704cAllocationLogic EL ON EL.[704cAllocationTypeID] = EC.[704cAllocationTypeID]
WHERE EC.EntityID = @LocalEntityID
    
Declare @EntityAllocationRuleEventTypeID INT, @EntityAllocationRuleWorkflowID INT    
     
Select @EntityAllocationRuleEventTypeID = EventTypeID From ENU_Event WHERE EventName = 'Import_EntityDefaultRuleOverride'    
    
SET @EntityAllocationRuleWorkflowID = dbo.udfGetApprovedWorkflow(@LocalClientID,@LocalTaxPeriodID, @EntityAllocationRuleEventTypeID, @LocalEntityID)    
    
Declare @DefaultAllocationRuleEventTypeID INT, @DefaultAllocationRuleTransactionID INT, @GlobalDefaultAllocationRuleTransactionID INT    
    
    
Select @DefaultAllocationRuleEventTypeID = EventTypeID From ENU_Event WHERE EventName = 'Import_DefaultAllocationRule'    
    
SET @DefaultAllocationRuleTransactionID = dbo.udfGetLatestTransactionID(@LocalClientID,@LocalTaxPeriodID,0,@DefaultAllocationRuleEventTypeID, @LocalEntityID)    
    
SET @GlobalDefaultAllocationRuleTransactionID = dbo.udfGetLatestTransactionID(@LocalClientID, @LocalTaxPeriodID, 0, @DefaultAllocationRuleEventTypeID, -1)    

INSERT INTO #DefaultAllocationRuleSetup(TransactionID, RuleID, AllocationPercentageTypeID, AllocationByID, UnderlyingTypeID, RuleTypeID, RuleGroupID, ClientID, TaxPeriodID, EntityID)
SELECT TransactionID, RuleID, AllocationPercentageTypeID, AllocationByID, UnderlyingTypeID, RuleTypeID, RuleGroupID, ClientID, TaxPeriodID, EntityID
FROM DefaultAllocationRuleSetup (NOLOCK)
WHERE TransactionID IN (@DefaultAllocationRuleTransactionID, @GlobalDefaultAllocationRuleTransactionID)

INSERT INTO #MapDefaultAllocRuleToLineItem(TransactionID, SourceID, StateID, SelectedMappingID, RuleID, ExcludeFromTransfers, ClientID, TaxPeriodID, EntityID)
SELECT TransactionID, SourceID, StateID, SelectedMappingID, RuleID, ExcludeFromTransfers, ClientID, TaxPeriodID, EntityID
FROM MapDefaultAllocRuleToLineItem (NOLOCK)
WHERE TransactionID IN (@DefaultAllocationRuleTransactionID, @GlobalDefaultAllocationRuleTransactionID)
    
INSERT INTO #TempEnitityAllocationRule(LineId, UpdatedAllocationRuleID)    
Select LineID, UpdatedAllocationRuleID    
FROM EntityAllocationRule_Snapshot with(nolock)    
WHERE WorkflowID = @EntityAllocationRuleWorkflowID    
    
INSERT INTO #LineItem WITH (TABLOCK) (LineID,AllocationTypeRuleId,LineTypeID, TransactionDate, IsTransactionDate, IsTransfersAdjusted)    
SELECT LineID,AllocationTypeRuleId,@K1LineTypeID,TransactionDate, IsTransactionDate, IsTransfersAdjusted FROM K1LineItem with(nolock)   
UNION ALL    
SELECT LineID,@YearlyAllocationTypeID,@BoxJKLLineTypeID, NULL, 0, 1 FROM BoxjklLineItem (NOLOCK)     
  
    
INSERT INTO #TempBookEffectiveData WITH (TABLOCK) (UnderlyingEntityID, LineID, FootNoteID, SourceID, AllocationTypeid, AdjustmentAllocationTypeID, TrackingKey, Tag, IsExcludefromTransfer)    
SELECT UnderlyingEntityID, LineID, FootNoteID, SourceID, AllocationTypeid, AdjustmentAllocationTypeID, TrackingKey, Tag, ISNULL(IsExcludefromTransfer, 0)     
FROM BookEffective_Snapshot with(nolock)    
WHERE WorkflowID = @CustomAllocationWorkFlowID    
AND ClientID = @LocalClientID AND TaxPeriodID = @LocalTaxPeriodID    
    
Update #TempBookEffectiveData     
SET AdjustmentAllocationTypeID = @CostAllocationTypeID    
WHERE AdjustmentAllocationTypeID = @BookAllocationTypeID    
    
SELECT DISTINCT UnderlyingEntityID,AllocationTypeid,AdjustmentAllocationTypeID,TrackingKey,Tag INTO #TempYearlyLines FROM #TempBookEffectiveData     
WHERE AdjustmentAllocationTypeID = @YearlyAllocationTypeID    
    
CREATE TABLE #Quarters (Quarter VARCHAR(10))    
    
IF (@AllocationTypeName = 'PE Book Allocation' AND @IsDatedTransfersConfigured = 'C')    
BEGIN    
 INSERT INTO #Quarters(Quarter)    
 SELECT Quarter FROM QuarterDates (NOLOCK)   
END    
ELSE    
BEGIN    
 INSERT INTO #Quarters(Quarter)    
 SELECT LookupDATA as Quarters FROM ENU_DF_DataList WHERE Category = 'Quarters'    
END    
    
SELECT * INTO #YearlyData FROM Yearly_Snapshot YS with(nolock) WHERE YS.WorkflowId = @YearlyWorkflowID    
    
SELECT    
    @PhaseID = dbo.[udfGetPhaseID](@LocalClientID, @LocalTaxPeriodID)    
    
    
INSERT INTO #EntityPartners (partnernumber)            
SELECT    
partnernumber     
FROM dbo.udf_PE_GetPartnersListForReports(@LocalClientID, @LocalTaxPeriodID,    
CONVERT(varchar(50), ISNULL(@LocalEntityID,0))    
,    
@PhaseID)    
    
DECLARE @IgnoreAssetclassForPartnershipLevel VARCHAR(1),@OverrideIndirectLookthroughAssetClass VARCHAR(1)    
    
SELECT @IgnoreAssetclassForPartnershipLevel = [State] FROM GlobalMenu      
WHERE MenuName = 'Ignore Asset class For Partnership level'      
    
SELECT @OverrideIndirectLookthroughAssetClass = [State] FROM GlobalMenu      
WHERE MenuName = 'Override Indirect Look through Asset class'      
    
Declare @EntityUnderlyingtype INT,  @UnderlyingOnlyUnderlyingType INT, @EntityTotalUnderlyingType INT, @AssetClassUnderlyingType INT    
    
Select @EntityUnderlyingtype = UnderlyingTypeID From ENU_UnderlyingType WHERE UnderlyingType = 'K-1 ONLY'    
Select @UnderlyingOnlyUnderlyingType = UnderlyingTypeID From ENU_UnderlyingType WHERE UnderlyingType = 'Underlying Only'    
Select @EntityTotalUnderlyingType = UnderlyingTypeID From ENU_UnderlyingType WHERE UnderlyingType = 'Entity Total'    
Select @AssetClassUnderlyingType = UnderlyingTypeID From ENU_UnderlyingType WHERE UnderlyingType = 'Asset Class'    

CREATE TABLE #CostPercentage_Function(WorkFlowID INT, TransactionID INT, ClientID INT, TaxPeriodID INT, EntityID INT, InvestmentID INT, 
PartnerNumber VARCHAR(50), Quarter VARCHAR(10), CommitmentPercent FLOAT, AllocationTypeID INT, Tag VARCHAR(4000), TrackingKey VARCHAR(4000),
UnderlyingType INT, AllocatedAmount FLOAT, CostPercentageID INT, [704cAllocationTypeID] INT, [704cPercentageType] VARCHAR(100), EntityUnderlyingType VARCHAR(100), GPPartnerReceivingCarry BIT)

CREATE TABLE #CostPercentage_Snapshot(WorkFlowID INT, TransactionID INT, ClientID INT, TaxPeriodID INT, EntityID INT, InvestmentID INT, 
PartnerNumber VARCHAR(50), Quarter VARCHAR(10), CommitmentPercent FLOAT, AllocationTypeID INT, Tag VARCHAR(4000), TrackingKey VARCHAR(4000),
UnderlyingType INT, AllocatedAmount FLOAT, CostPercentageID INT, [704cAllocationTypeID] INT, [704cPercentageType] VARCHAR(100), EntityUnderlyingType VARCHAR(100),
TotalMgmtFees FLOAT, HotIssueGainLoss FLOAT, [704cGainLoss] FLOAT, GuaranteedPaymentsServices FLOAT, GuaranteedPaymentsCapital FLOAT,
UsWithholding FLOAT, IncentiveFee FLOAT, ForeignTaxes FLOAT, SpecialAllocation1 FLOAT, SpecialAllocation2 FLOAT, GPPartnerReceivingCarry BIT)

CREATE TABLE #CostPercentage704cValues(WorkFlowID INT, TransactionID INT, ClientID INT, TaxPeriodID INT, EntityID INT, InvestmentID INT, 
PartnerNumber VARCHAR(50), Quarter VARCHAR(10), CommitmentPercent FLOAT, AllocationTypeID INT, Tag VARCHAR(4000), TrackingKey VARCHAR(4000),
UnderlyingType INT, AllocatedAmount FLOAT, CostPercentageID INT, [704cAllocationTypeID] INT, [704cPercentageType] VARCHAR(100), EntityUnderlyingType VARCHAR(100),
TotalMgmtFees FLOAT, HotIssueGainLoss FLOAT, [704cGainLoss] FLOAT, GuaranteedPaymentsServices FLOAT, GuaranteedPaymentsCapital FLOAT,
UsWithholding FLOAT, IncentiveFee FLOAT, ForeignTaxes FLOAT, SpecialAllocation1 FLOAT, SpecialAllocation2 FLOAT, GPPartnerReceivingCarry BIT)

CREATE TABLE #CostPercentage_Snapshot_UnPivoted(WorkFlowID INT, TransactionID INT, ClientID INT, TaxPeriodID INT, EntityID INT, InvestmentID INT, 
PartnerNumber VARCHAR(50), Quarter VARCHAR(10), CommitmentPercent FLOAT, AllocationTypeID INT, Tag VARCHAR(4000), TrackingKey VARCHAR(4000),
UnderlyingType INT, AllocatedAmount FLOAT, CostPercentageID INT, [704cAllocationTypeID] INT, [704cPercentageType] VARCHAR(100), EntityUnderlyingType VARCHAR(100),
Mapped704cField VARCHAR(50), Mapped704cFieldRuleGroupID INT, K1LineID INT, GPPartnerReceivingCarry BIT)

CREATE TABLE #CostPercentage_Snapshot_UnPivotedMerged(WorkFlowID INT, TransactionID INT, ClientID INT, TaxPeriodID INT, EntityID INT, InvestmentID INT, 
PartnerNumber VARCHAR(50), Quarter VARCHAR(10), CommitmentPercent FLOAT, AllocationTypeID INT, Tag VARCHAR(4000), TrackingKey VARCHAR(4000),
UnderlyingType INT, AllocatedAmount FLOAT, CostPercentageID INT, [704cAllocationTypeID] INT, [704cPercentageType] VARCHAR(100), EntityUnderlyingType VARCHAR(100),
Mapped704cField VARCHAR(50), Mapped704cFieldRuleGroupID INT, K1LineID INT, GPPartnerReceivingCarry BIT)

IF(@LocalMode != 4)
BEGIN
    INSERT INTO #CostPercentage_Function(WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, AllocationTypeID, Tag, TrackingKey,
	UnderlyingType, AllocatedAmount, CostPercentageID)
    SELECT C.WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, C.EntityID, C.InvestmentID, C.PartnerNumber, C.Quarter, C.CommitmentPercent, C.AllocationTypeID, C.Tag, C.TrackingKey,
	C.UnderlyingType, C.AllocatedAmount, C.CostPercentageID
	FROM dbo.udfGetCostPercentageDetails(@CostPercentageWorkflowID) C

	INSERT INTO #CostPercentage_Snapshot(WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, AllocationTypeID, Tag, TrackingKey,
	UnderlyingType, AllocatedAmount, CostPercentageID, EntityUnderlyingType)
	SELECT C.WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, C.EntityID, C.InvestmentID, C.PartnerNumber, C.Quarter, C.CommitmentPercent, C.AllocationTypeID, C.Tag, C.TrackingKey,
	C.UnderlyingType, C.AllocatedAmount, C.CostPercentageID, U.UnderlyingType AS EntityUnderlyingType 
	FROM #CostPercentage_Function C
	INNER JOIN ENU_UnderlyingType U ON C.UnderlyingType = U.UnderlyingTypeID
	LEFT JOIN CostPercentage_704c_Snapshot CP (NOLOCK) ON C.WorkFlowID = CP.WorkFlowID AND C.CostPercentageID = CP.CostPercentageID
	WHERE C.WorkFlowID = @CostPercentageWorkflowID AND CP.CostPercentageID IS NULL
END

IF(@LocalMode = 1 AND ISNULL(@704cAllocationTypeName, '') <> '')
BEGIN
    DECLARE @RegisterTypeID INT, @704cSourceID INT
    SELECT @RegisterTypeID = GlobalMenuID FROM GlobalMenu WHERE MenuName = '704c To K1 Line Mapping'
    SELECT @704cSourceID = SourceID FROM ENU_MappingSource WHERE SourceName = 'Tax Allocation Report - 704c'

    -- Pick entity level mappings first. Entity level mappings will override -1/ALL mappings
	INSERT INTO #Mappings(EntityID, MapLineID, DatabaseName, RegisterLineID, Formula, FieldSourceID)
	SELECT E.EntityID, MD.MapLineID, ML.DatabaseName, MD.RegisterLineID,
	CASE WHEN OperationType = '-' THEN 'SUBTRACT' ELSE 'ADD' END, MD.FieldSourceID
	FROM MapDataRegister MD (NOLOCK)
	INNER JOIN MappingLineItem ML (NOLOCK) ON ML.LineID = MD.MapLineID AND ML.ClientID = MD.ClientID AND ML.TaxPeriodID = MD.TaxPeriodID
	INNER JOIN K1LineItem KL (NOLOCK) ON KL.LineID = MD.RegisterLineID AND KL.ClientID = MD.ClientID AND KL.TaxPeriodID = MD.TaxPeriodID
	INNER JOIN VW_Entity E (NOLOCK) ON E.EntityID = MD.EntityID AND E.ClientID = MD.ClientID AND E.TaxPeriodID = MD.TaxPeriodID  
	WHERE MD.EntityID = @LocalEntityID
	AND MD.SourceTypeID = @704cSourceID
	AND MD.ClientID = @LocalClientID
	AND MD.TaxPeriodID = @LocalTaxPeriodID
	AND MD.RegisterTypeID = @RegisterTypeID
	AND KL.LineDataType = 'Number'
	AND KL.IsActive = 1
	AND KL.IsVisible = 1

    -- Pick other mappings which are not entity specific
	INSERT INTO #Mappings(EntityID, MapLineID, DatabaseName, RegisterLineID, Formula, FieldSourceID)
	SELECT MD.EntityID, MD.MapLineID, ML.DatabaseName, MD.RegisterLineID,
	CASE WHEN OperationType = '-' THEN 'SUBTRACT' ELSE 'ADD' END, MD.FieldSourceID
	FROM MapDataRegister MD (NOLOCK)
	INNER JOIN MappingLineItem ML (NOLOCK) ON ML.LineID = MD.MapLineID AND ML.ClientID = MD.ClientID AND ML.TaxPeriodID = MD.TaxPeriodID
	INNER JOIN K1LineItem KL (NOLOCK) ON KL.LineID = MD.RegisterLineID AND KL.ClientID = MD.ClientID AND KL.TaxPeriodID = MD.TaxPeriodID
	LEFT JOIN #Mappings MS ON MS.MapLineID = MD.MapLineID
	WHERE MD.EntityID = -1
	AND MD.SourceTypeID = @704cSourceID
	AND MD.ClientID = @LocalClientID
	AND MD.TaxPeriodID = @LocalTaxPeriodID
	AND MD.RegisterTypeID = @RegisterTypeID
	AND KL.LineDataType = 'Number'
	AND KL.IsActive = 1
	AND KL.IsVisible = 1
	AND MS.MapLineID IS NULL

    -- Pick values for 704c fields. Only those lines will be picked in below query which have 704c values in allocation data import
    INSERT INTO #CostPercentage704cValues(WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber,Quarter, CommitmentPercent, AllocationTypeID, Tag, TrackingKey,
    UnderlyingType, AllocatedAmount, CostPercentageID, TotalMgmtFees, HotIssueGainLoss, [704cGainLoss], GuaranteedPaymentsServices, GuaranteedPaymentsCapital,
    UsWithholding, IncentiveFee, ForeignTaxes, SpecialAllocation1, SpecialAllocation2, EntityUnderlyingType, GPPartnerReceivingCarry)
    SELECT C.WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, C.EntityID, C.InvestmentID, C.PartnerNumber, C.Quarter, C.CommitmentPercent, C.AllocationTypeID, C.Tag, C.TrackingKey,
    C.UnderlyingType, C.AllocatedAmount, C.CostPercentageID, CP.TotalMgmtFees, CP.HotIssueGainLoss, CP.[704cGainLoss], CP.GuaranteedPaymentsServices, CP.GuaranteedPaymentsCapital,
    CP.UsWithholding, CP.IncentiveFee, CP.ForeignTaxes, CP.SpecialAllocation1, CP.SpecialAllocation2, U.UnderlyingType, CP.GPPartnerReceivingCarry
    FROM #CostPercentage_Function C
    INNER JOIN ENU_UnderlyingType U ON C.UnderlyingType = U.UnderlyingTypeID
	INNER JOIN CostPercentage_704c_Snapshot CP (NOLOCK) ON C.WorkFlowID = CP.WorkFlowID AND C.CostPercentageID = CP.CostPercentageID
    WHERE C.WorkFlowID = @CostPercentageWorkflowID AND U.UnderlyingType != 'ASSET CLASS'

    -- Run below logic only when we have mappings and 704c values given in allocation data import
    IF EXISTS (SELECT TOP 1 1 FROM #Mappings) AND EXISTS (SELECT TOP 1 1 FROM #CostPercentage704cValues)
    BEGIN
        DECLARE @Cols VARCHAR(MAX), @Query VARCHAR(MAX)
    
        SELECT @Cols = STUFF(( SELECT TOP 100 PERCENT '],[' + M.DatabaseName
        FROM #Mappings AS M
        FOR XML PATH('')                 
        ), 1, 2, '') + ']'

        -- Unpivot on basis of mapped columns in below query. We need 704c fields as rows to create rules later on
        SELECT @Query = N'INSERT INTO #CostPercentage_Snapshot_UnPivoted(WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, Tag, TrackingKey,
        UnderlyingType, AllocatedAmount, CostPercentageID, EntityUnderlyingType, Mapped704cField, GPPartnerReceivingCarry)
        SELECT WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, Tag, TrackingKey,
        UnderlyingType, AllocatedAmount, CostPercentageID, EntityUnderlyingType, Mapped704cField, GPPartnerReceivingCarry
        FROM               
        (              
        SELECT WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, Tag, TrackingKey,
        UnderlyingType, CostPercentageID, EntityUnderlyingType, TotalMgmtFees, HotIssueGainLoss, [704cGainLoss], GuaranteedPaymentsServices, GuaranteedPaymentsCapital,
        UsWithholding, IncentiveFee, ForeignTaxes, SpecialAllocation1, SpecialAllocation2, GPPartnerReceivingCarry
        FROM #CostPercentage704cValues
        ) p              
        UNPIVOT                                
        (                 
        AllocatedAmount FOR Mapped704cField IN
        ( '+ @Cols +' )                                
        ) AS unpvt
        '

        EXEC (@Query)

        UPDATE CS
        SET CS.K1LineID = MS.RegisterLineID
        FROM #CostPercentage_Snapshot_UnPivoted CS
        INNER JOIN #Mappings MS ON MS.DatabaseName = CS.Mapped704cField

        UPDATE CS
        SET CS.AllocatedAmount = -CS.AllocatedAmount
        FROM #CostPercentage_Snapshot_UnPivoted CS
        INNER JOIN #Mappings MS ON MS.DatabaseName = CS.Mapped704cField
		WHERE MS.Formula = 'SUBTRACT'

        -- Multiple 704c fields can be mapped to single k1 line
        -- In that case we will merge the amounts in a single field and create rule for that only
        INSERT INTO #CostPercentage_Snapshot_UnPivotedMerged(WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, AllocationTypeID, Tag, TrackingKey,
        UnderlyingType, AllocatedAmount, CostPercentageID, EntityUnderlyingType, Mapped704cField, K1LineID, GPPartnerReceivingCarry)
        SELECT WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, AllocationTypeID, Tag, TrackingKey,
        UnderlyingType, SUM(AllocatedAmount), CostPercentageID, EntityUnderlyingType, MAX(Mapped704cField), K1LineID, ISNULL(GPPartnerReceivingCarry,0)
        FROM #CostPercentage_Snapshot_UnPivoted
        GROUP BY WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, AllocationTypeID, Tag, TrackingKey,
        UnderlyingType, CostPercentageID, EntityUnderlyingType, K1LineID, ISNULL(GPPartnerReceivingCarry,0)
        
        SELECT DISTINCT Mapped704cField INTO #Mapped704cFields
        FROM #CostPercentage_Snapshot_UnPivotedMerged
        
        -- Create custom rules if they do not exist
        INSERT INTO ENU_CustomAllocations(AllocationType, ClientID, TaxPeriodID)
        SELECT 'Special ' + MD.Mapped704cField, @LocalClientID, @LocalTaxPeriodID
        FROM #Mapped704cFields MD
        LEFT JOIN ENU_CustomAllocations ET (NOLOCK)  ON 'Special ' + MD.Mapped704cField = ET.AllocationType
        WHERE ET.AllocationType IS NULL

        INSERT INTO ENU_RuleGroup(RuleGroupName, DisplayOrder)
        SELECT 'Special ' + MD.Mapped704cField, NULL
        FROM #Mapped704cFields MD
        LEFT JOIN ENU_RuleGroup ET ON 'Special ' + MD.Mapped704cField = ET.RuleGroupName
        WHERE ET.RuleGroupName IS NULL
        
        UPDATE CS
        SET CS.AllocationTypeID = ET.AllocationTypeID
        FROM #CostPercentage_Snapshot_UnPivotedMerged CS
        INNER JOIN ENU_CustomAllocations ET (NOLOCK) ON 'Special ' + CS.Mapped704cField = ET.AllocationType

        UPDATE CS
        SET CS.Mapped704cFieldRuleGroupID = ET.RuleGroupID
        FROM #CostPercentage_Snapshot_UnPivotedMerged CS
        INNER JOIN ENU_RuleGroup ET ON 'Special ' + CS.Mapped704cField = ET.RuleGroupName

        DECLARE @AllocationPercentageTypeID INT, @AllocationByID INT, @RuleTypeID INT
        SELECT @AllocationPercentageTypeID = AllocationPercentageTypeID FROM ENU_AllocationPercentageType WHERE AllocationPercentageType = 'N/A'
        SELECT @AllocationByID = AllocationByID FROM ENU_AllocationBy WHERE AllocationBy = 'AMOUNT'
        SELECT @RuleTypeID = RuleTypeID FROM ENU_RuleType (NOLOCK) WHERE RuleType = 'ENTITY'

        -- Create rules like we create in DAR import
        -- Inserting transactionid as -2 because @DefaultAllocationRuleTransactionID and @GlobalDefaultAllocationRuleTransactionID will be null for CAR import
        INSERT INTO #MapDefaultAllocRuleToLineItem(TransactionID, SourceID, StateID, SelectedMappingID, RuleID, ExcludeFromTransfers, ClientID, TaxPeriodID, EntityID)
        SELECT DISTINCT -2, @K1LineTypeID, 0, MS.RegisterLineID, CS.AllocationTypeID, 0, @LocalClientID, @LocalTaxPeriodID, @LocalEntityID
        FROM #Mappings MS
        INNER JOIN #CostPercentage_Snapshot_UnPivotedMerged CS ON MS.DatabaseName = CS.Mapped704cField

        INSERT INTO #DefaultAllocationRuleSetup(TransactionID, RuleID, AllocationPercentageTypeID, AllocationByID, UnderlyingTypeID, RuleTypeID, RuleGroupID, ClientID, TaxPeriodID, EntityID)
        SELECT DISTINCT -2, CS.AllocationTypeID, @AllocationPercentageTypeID, @AllocationByID, CS.UnderlyingType, @RuleTypeID, CS.Mapped704cFieldRuleGroupID, @LocalClientID, @LocalTaxPeriodID, @LocalEntityID
        FROM #Mappings MS
        INNER JOIN #CostPercentage_Snapshot_UnPivotedMerged CS ON MS.DatabaseName = CS.Mapped704cField

        INSERT INTO #CostPercentage_Snapshot(WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityID, InvestmentID, PartnerNumber, Quarter, CommitmentPercent, AllocationTypeID, Tag, TrackingKey,
        UnderlyingType, AllocatedAmount, CostPercentageID, EntityUnderlyingType, GPPartnerReceivingCarry)
        SELECT C.WorkFlowID, C.TransactionID, C.ClientID, C.TaxPeriodID, C.EntityID, C.InvestmentID, C.PartnerNumber, C.Quarter, C.CommitmentPercent, C.AllocationTypeID, C.Tag, C.TrackingKey,
        C.UnderlyingType, C.AllocatedAmount, C.CostPercentageID, C.EntityUnderlyingType, GPPartnerReceivingCarry
        FROM #CostPercentage_Snapshot_UnPivotedMerged C
        
        DROP TABLE IF EXISTS #Mapped704cFields
    END
END

IF(@LocalMode = 4)
BEGIN
SELECT ClientID, TaxPeriodID, EntityId, InvestmentID, PartnerNumber, Quarter,AllocationTypeId,Underlyingtype, TrackingKey, 
CommitmentPercent ,[704cAllocationTypeID],[704cPercentageType] INTO #AllocationPercentage704c FROM AllocationPercentage704c (NOLOCK) 
  UNPIVOT(CommitmentPercent FOR [704cPercentageType] IN (OrdinaryPercentage,CapitalPercentage,CapitalGainPercentage,CapitalLossPercentage)) b
WHERE RunID = @RunID

INSERT INTO #CostPercentage_Snapshot(WorkFlowID, TransactionID, ClientID, TaxPeriodID, EntityId, InvestmentID, PartnerNumber, Quarter, AC.CommitmentPercent, AllocationTypeId, Tag, TrackingKey,
		Underlyingtype, AllocatedAmount, CostPercentageId, [704cAllocationTypeID], [704cPercentageType],EntityUnderlyingtype, GPPartnerReceivingCarry)
SELECT CS.WorkFlowID, CS.TransactionID, CS.ClientID, CS.TaxPeriodID, CS.EntityId, CS.InvestmentID, CS.PartnerNumber, CS.Quarter, AC.CommitmentPercent, CS.AllocationTypeId,'', CS.TrackingKey,
		CS.Underlyingtype, CS.AllocatedAmount, CS.CostPercentageId, CP.[704cAllocationTypeID],  AC.[704cPercentageType],U.Underlyingtype, CP.GPPartnerReceivingCarry
         FROM CostPercentage_Snapshot CS (NOLOCK)
INNER JOIN VW_Entity E1 ON CS.EntityID = E1.EntityID
INNER JOIN CostPercentage_704c_Snapshot (NOLOCK) CP ON CS.WorkFlowID = CP.WorkFlowID and CS.CostPercentageId = CP.CostPercentageId
INNER JOIN #AllocationPercentage704c AC ON CS.EntityID = AC.EntityID
        AND ISNULL(CS.InvestmentID,0) =ISNULL(AC.InvestmentID,0)  
		AND ISNULL(CS.PartnerNumber,'') =ISNULL(AC.PartnerNumber,'') 
		AND ISNULL(CS.Quarter,'') =ISNULL(AC.Quarter,'')  
        AND ISNULL(CS.Underlyingtype,0) =ISNULL(AC.Underlyingtype,0)  
		AND ISNULL(CS.AllocationTypeId,0) =ISNULL(AC.AllocationTypeId,0) 
		AND ISNULL(CS.TrackingKey,'') =ISNULL(AC.TrackingKey,'')
        AND ISNULL(CP.[704cAllocationTypeID],0) =ISNULL(AC.[704cAllocationTypeID],0)
		INNER JOIN Enu_Underlyingtype U on ISNULL(CS.Underlyingtype,0) = ISNULL(U.UnderlyingTypeId,0)   
       WHERE ISNULL(CS.WorkFlowID,0) = @CostPercentageWorkflowID 

END
    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag,[704cAllocationTypeID] ,[704cPercentageType], GPPartnerReceivingCarry )    
Select C.InvestmentID, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0),ISNULL(AllocationTypeId, @CostAllocationTypeID), ISNULL(C.TrackingKey, ''), ISNULL(C.Tag, '')  
,[704cAllocationTypeID] ,[704cPercentageType], C.GPPartnerReceivingCarry
FROM #CostPercentage_Snapshot C (NOLOCK)    
WHERE  C.InvestmentID <> -1  AND ISNULL(C.Underlyingtype, @EntityUnderlyingtype) = @EntityUnderlyingtype    
    
    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag)    
SELECT DISTINCT Y.UnderlyingEntityID,YS.PartnerNumber, Q.Quarter, ISNULL(YS.ProRataEffOwnPercent,0),Y.AdjustmentAllocationTypeID, '','' FROM  #TempYearlyLines Y     
CROSS JOIN #YearlyData YS Cross Join #Quarters Q    
    
    
DROP TABLE #YearlyData    
DROP TABLE #TempYearlyLines    
DROP TABLE #Quarters    
    
    
select DISTINCT C.EntityId, IIF(c.InvestmentID = -1, c.EntityId, c.InvestmentID) InvestmentID, c.Quarter, c.AllocationTypeId, TrackingKey, c.Underlyingtype,C.EntityUnderlyingtype, ISNULL(C.Tag,'-1') AS TAG,C.InvestmentID Cost_InvestmentID           
into #TempCostUnderlyingTypes from #CostPercentage_Snapshot C    
WHERE  (C.EntityUnderlyingtype <> 'K-1 ONLY'  OR (C.EntityUnderlyingtype = 'K-1 ONLY' and c.InvestmentID = -1) )

CREATE TABLE #EntityAssetClassRelationShip ( LowerTierEntityID INT, AssetClassID INT, TrackingKey VARCHAR(4000))
INSERT INTO #EntityAssetClassRelationShip
SELECT LowerTierEntityID, AssetClassID, TrackingKey FROM [udfGetAssetClassRelationship] (@LocalClientID, @LocalTaxPeriodID, @LocalEntityID)

CREATE TABLE #TempAllUnderlyingsCombined(UnderlyingEntityID INT, EntityID INT, HLevel INT, UnderlyingType INT, AllocationTypeID INT,
                                         TrackingKey VARCHAR(4000), AssetClassID INT, ImmediateLowerTierEntityID INT, Cost_Entity INT,
										 Cost_InvestmentID INT, Cost_Quarter VARCHAR(50), Cost_AllocationTypeID INT, Cost_TrackingKey VARCHAR(4000),
										 Cost_UnderlyingType INT)
              
CREATE NONCLUSTERED INDEX IX_TempAllUnderlyingsCombined ON #TempAllUnderlyingsCombined(UnderlyingentityID) INCLUDE (UnderlyingType)

CREATE NONCLUSTERED INDEX IX_TempAllUnderlyingsCombinedFull ON #TempAllUnderlyingsCombined(Cost_Entity, Cost_InvestmentID, Cost_AllocationTypeID)  
  
  
SELECT DISTINCT ER.LowerTierEntityID, ER.UpperTierEntityID, ER.UpperTierEntityID CurrentEntityId ,  CASE WHEN ER.UpperTierEntityID = @LocalEntityID THEN 10001 ELSE 2 END HLevel,TC.AllocationTypeId, '~'+      
 CASE WHEN TC.EntityUnderlyingtype = 'Asset Class' THEN CONVERT(VARCHAR(4000), ER.LowerTierEntityID) + '~' ELSE      
 CASE WHEN ISNULL(TC.TrackingKey,'') = '' THEN CONVERT(VARCHAR(4000), TC.InvestmentID) ELSE TC.TrackingKey END  +'~' END TrackingKey  
 ,TC.InvestmentID AssetClassId, ER.LowerTierEntityID ImmediateLowerTierEntityID
 INTO #EntityHierarchy
 FROM #TempCostUnderlyingTypes TC  
 INNER JOIN EntityRelationship ER (NOLOCK) ON ER.UpperTierEntityID = CASE WHEN TC.EntityUnderlyingtype = 'Asset Class' THEN TC.EntityId ELSE tc.InvestmentID END    
 AND ER.ClientID = @LocalClientID AND ER.TaxPeriodID = @LocalTaxPeriodID   
  
WHILE (1=1)
BEGIN

INSERT INTO #EntityHierarchy (LowerTierEntityID,UpperTierEntityID,CurrentEntityId,HLevel, AllocationTypeId,  TrackingKey,  AssetClassId, ImmediateLowerTierEntityID)
SELECT ER.LowerTierEntityID,
	   ER.UpperTierEntityID,
	   EH.CurrentEntityId,
       CASE WHEN EH.CurrentEntityId = @LocalEntityID THEN 10001 ELSE EH.HLevel + 1 END,
       EH.AllocationTypeId,
       EH.TrackingKey,
       Eh.AssetClassId,
       EH.ImmediateLowerTierEntityID
       FROM EntityRelationship ER (NOLOCK)
       INNER JOIN #EntityHierarchy EH
       ON ER.UpperTierEntityID = EH.LowerTierEntityID
	   LEFT JOIN #EntityHierarchy T ON ER.LowerTierEntityID = T.LowerTierEntityID AND ER.UpperTierEntityID = T.UpperTierEntityID 
	   AND EH.CurrentEntityId = T.CurrentEntityId AND CASE WHEN EH.CurrentEntityId = @LocalEntityID THEN 10001 ELSE EH.HLevel + 1 END = T.HLevel
       AND EH.AllocationTypeId = T.AllocationTypeId AND ISNUll(T.TrackingKey,'') = ISNULL(EH.TrackingKey,'')
	   AND T.AssetClassId = Eh.AssetClassId AND T.ImmediateLowerTierEntityID = EH.ImmediateLowerTierEntityID
       WHERE T.LowerTierEntityID IS NULL AND ER.ClientID = @LocalClientID AND ER.TaxPeriodID = @LocalTaxPeriodID

IF(@@ROWCOUNT = 0) BREAK;
END      

 INSERT INTO #TempAllUnderlyingsCombined(UnderlyingEntityID, EntityID, HLevel, UnderlyingType, AllocationTypeID, TrackingKey, AssetClassID,
 ImmediateLowerTierEntityID, Cost_Entity, Cost_InvestmentID, Cost_Quarter, Cost_AllocationTypeID, Cost_TrackingKey, Cost_UnderlyingType)
 SELECT DISTINCT EH.LowerTierEntityID, EH.CurrentEntityID, EH.HLevel, TC.UnderlyingType, TC.AllocationTypeID, EH.TrackingKey, EH.AssetClassID,    
 EH.ImmediateLowerTierEntityID, TC.EntityID, TC.Cost_InvestmentID, TC.Quarter, TC.AllocationTypeID, TC.TrackingKey, TC.UnderlyingType																																												   
 FROM #TempCostUnderlyingTypes TC 
  JOIN #EntityHierarchy EH  ON EH.CurrentEntityId = CASE WHEN TC.EntityUnderlyingtype = 'Asset Class' THEN TC.EntityId ELSE TC.InvestmentID END   
 AND TC.AllocationTypeId=EH.AllocationTypeId AND TC.InvestmentID=Eh.AssetClassId    
 UNION    
 --K1 Only     
 select InvestmentID UnderlyingEntityId,InvestmentID EntityId, CASE WHEN InvestmentID = @LocalEntityID THEN 10001 ELSE 1 END,C.Underlyingtype,C.AllocationTypeId,CASE WHEN ISNULL(C.TrackingKey,'') = '' THEN +'~' +  CONVERT(VARCHAR(4000), C.InvestmentID) +'~'  ELSE C.TrackingKey    END TrackingKey,
 C.InvestmentID, 0, C.EntityID, C.InvestmentID, C.Quarter, C.AllocationTypeID, C.TrackingKey, C.UnderlyingType																																																			  
 FROM #CostPercentage_Snapshot C  
 WHERE  C.EntityUnderlyingtype = 'K-1 ONLY'  
 UNION    
 --K1 Only     
 select @LocalEntityID UnderlyingEntityId,@LocalEntityID EntityId,10001,C.Underlyingtype,C.AllocationTypeId,'~' + CONVERT(VARCHAR(4000),@LocalEntityID) + '~',@LocalEntityID, @LocalEntityID
 ,C.entityid Cost_Entity,C.Investmentid Cost_InvestmentID,C.Quarter Cost_Quarter,C.AllocationTypeID Cost_AllocationTypeID,C.TrackingKey Cost_TrackingKey,C.UnderlyingType Cost_UnderlyingType
 FROM #CostPercentage_Snapshot C 							  
 WHERE C.EntityUnderlyingtype = 'K-1 ONLY'   AND InvestmentID=-1  
 UNION    
 --Applying Entity Total and asset class to the cost entity where they are defined    
  SELECT TC.EntityId  UnderlyingEntityId,TC.EntityId EntityId,CASE WHEN TC.EntityId = @LocalEntityID THEN 10001 ELSE 1 END,TC.Underlyingtype,TC.AllocationTypeId,'~' + CONVERT(VARCHAR(4000),TC.EntityId) + '~',TC.InvestmentID, TC.EntityId  
 ,TC.entityid Cost_Entity,TC.Cost_InvestmentID Cost_InvestmentID,TC.Quarter Cost_Quarter,TC.AllocationTypeID Cost_AllocationTypeID,TC.TrackingKey Cost_TrackingKey,TC.UnderlyingType Cost_UnderlyingType
 FROM #TempCostUnderlyingTypes TC     
 WHERE TC.EntityUnderlyingtype IN ( 'Asset Class')     
 UNION    
SELECT  TC.InvestmentID UnderlyingEntityId,TC.InvestmentID EntityId,CASE WHEN TC.InvestmentID = @LocalEntityID THEN 10001 ELSE 1 END,TC.Underlyingtype,TC.AllocationTypeId, 
 CASE WHEN ISNULL(TC.TrackingKey,'') = '' THEN +'~' +  CONVERT(VARCHAR(4000), TC.InvestmentID) +'~'  ELSE TC.TrackingKey    END TrackingKey ,TC.InvestmentID, 0  
 ,TC.entityid Cost_Entity,TC.Cost_InvestmentID Cost_InvestmentID,TC.Quarter Cost_Quarter,TC.AllocationTypeID Cost_AllocationTypeID,TC.TrackingKey Cost_TrackingKey,TC.UnderlyingType Cost_UnderlyingType
 FROM #TempCostUnderlyingTypes TC    
 WHERE  TC.EntityUnderlyingtype IN ( 'Entity Total')  
 
CREATE NONCLUSTERED INDEX IX_CostPercentage_Snapshot ON #CostPercentage_Snapshot(EntityId,InvestmentID,AllocationTypeID)

 IF (@OverrideIndirectLookthroughAssetClass <> 'C')    
 BEGIN    
  CREATE TABLE #MatchingAssetClass(UnderlyingEntityID INT, EntityID INT, HLevel INT, UnderlyingType INT, AllocationTypeID INT, TrackingKey VARCHAR(MAX), AssetClassID INT
	, ImmediateLowerTierEntityID INT)

	--For Handling Asset Class, Deleting Entities which do not have matching asset class 
	IF EXISTS (SELECT TOP 1 1 FROM #EntityAssetClassRelationShip WHERE TrackingKey IS NULL)
		INSERT INTO #MatchingAssetClass(UnderlyingEntityID, EntityID, HLevel, UnderlyingType, AllocationTypeID, TrackingKey, AssetClassID, ImmediateLowerTierEntityID)
		SELECT DISTINCT AI.UnderlyingEntityID, AI.EntityID, AI.HLevel, AI.UnderlyingType, AI.AllocationTypeID, AI.TrackingKey, AI.AssetClassID, AI.ImmediateLowerTierEntityID
		FROM #TempAllUnderlyingsCombined AI   
		INNER JOIN #EntityAssetClassRelationShip EAR ON AI.UnderlyingEntityID = EAR.LowerTierEntityID and EAR.TrackingKey IS NULL  
			--AND '~' + EAR.TrackingKey + '~'  LIKE  '%'+ AI.TrackingKey + '%'
		JOIN VW_Entity E ON E.EntityID = AI.UnderlyingEntityID      
		JOIN ENU_Underlyingtype U on AI.Underlyingtype = U.UnderlyingTypeId    
		WHERE U.UnderlyingType = 'Asset Class' AND  CASE WHEN ISNULL(EAR.AssetClassID,0) = 0 THEN E.AssetClassID ELSE EAR.AssetClassID END = AI.AssetClassId 
				
	IF EXISTS (SELECT TOP 1 1 FROM #EntityAssetClassRelationShip WHERE TrackingKey IS NOT NULL)
        INSERT INTO #MatchingAssetClass(UnderlyingEntityID, EntityID, HLevel, UnderlyingType, AllocationTypeID, TrackingKey, AssetClassID, ImmediateLowerTierEntityID)
		SELECT DISTINCT AI.UnderlyingEntityID, AI.EntityID, AI.HLevel, AI.UnderlyingType, AI.AllocationTypeID, AI.TrackingKey, AI.AssetClassID, AI.ImmediateLowerTierEntityID
		FROM #TempAllUnderlyingsCombined AI   
		INNER JOIN #EntityAssetClassRelationShip EAR ON AI.UnderlyingEntityID = EAR.LowerTierEntityID and EAR.TrackingKey IS NOT NULL  
			AND '~' + EAR.TrackingKey + '~'  LIKE  '%'+ AI.TrackingKey + '%'
		JOIN VW_Entity E ON E.EntityID = AI.UnderlyingEntityID      
		JOIN ENU_Underlyingtype U on AI.Underlyingtype = U.UnderlyingTypeId    
		WHERE U.UnderlyingType = 'Asset Class' AND  CASE WHEN ISNULL(EAR.AssetClassID,0) = 0 THEN E.AssetClassID ELSE EAR.AssetClassID END =AI.AssetClassId


    INSERT INTO #MatchingAssetClass(UnderlyingEntityID, EntityID, HLevel, UnderlyingType, AllocationTypeID, TrackingKey, AssetClassID, ImmediateLowerTierEntityID)
		SELECT DISTINCT AI.UnderlyingEntityID, AI.EntityID, AI.HLevel, AI.UnderlyingType, AI.AllocationTypeID, AI.TrackingKey, AI.AssetClassID, AI.ImmediateLowerTierEntityID
		FROM #TempAllUnderlyingsCombined AI
        JOIN VW_Entity E ON E.EntityID = AI.UnderlyingEntityID AND AI.AssetClassID = E.AssetClassID     
		JOIN ENU_Underlyingtype U on AI.Underlyingtype = U.UnderlyingTypeId
        LEFT JOIN #MatchingAssetClass M
		ON M.UnderlyingEntityId = ai.UnderlyingEntityId
		AND AI.TrackingKey = M.TrackingKey
		AND AI.ImmediateLowerTierEntityID = M.ImmediateLowerTierEntityID
        WHERE U.UnderlyingType = 'Asset Class' AND M.UnderlyingEntityId IS NULL



  IF EXISTS (SELECT TOP 1 1 FROM #EntityAssetClassRelationShip)            
		DELETE AI
		FROM #TempAllUnderlyingsCombined AI
		JOIN ENU_Underlyingtype U on AI.Underlyingtype = U.UnderlyingTypeId
		LEFT JOIN #MatchingAssetClass M
		ON M.UnderlyingEntityId = ai.UnderlyingEntityId
		AND AI.TrackingKey = M.TrackingKey
		AND AI.AssetClassId = M.AssetClassId
		AND AI.ImmediateLowerTierEntityID = M.ImmediateLowerTierEntityID
		WHERE M.UnderlyingEntityId IS NULL AND U.UnderlyingType = 'Asset Class'

		DROP TABLE IF EXISTS #MatchingAssetClass
 END    
 ELSE   
 BEGIN  
    DELETE AI FROM #TempAllUnderlyingsCombined AI   
    JOIN #EntityAssetClassRelationShip EAR ON AI.ImmediateLowerTierEntityID = EAR.LowerTierEntityID  
    JOIN VW_Entity E with(nolock) ON E.EntityID = AI.ImmediateLowerTierEntityID      
    JOIN ENU_Underlyingtype U on AI.Underlyingtype = U.UnderlyingTypeId    
    WHERE U.UnderlyingType = 'Asset Class' AND CASE WHEN ISNULL(EAR.AssetClassID,0) = 0 THEN E.AssetClassID ELSE EAR.AssetClassID END !=AI.AssetClassId    
 END    
      
 IF (@IgnoreAssetclassForPartnershipLevel  = 'C')      
 BEGIN      
  DELETE AI FROM #TempAllUnderlyingsCombined AI JOIN VW_Entity E with(nolock) ON E.EntityID =  AI.UnderlyingEntityID        
  JOIN ENU_Underlyingtype U on AI.Underlyingtype = U.UnderlyingTypeId      
  WHERE AI.UnderlyingEntityId = @LocalEntityID AND U.UnderlyingType = 'Asset Class'      
 END      
    

--update #TempAllUnderlyingsCombined set hlevel=10001 where entityid=@LocalEntityID    

Select UnderlyingEntityId,Entityid,Underlyingtype,AllocationTypeId,TrackingKey,MAX(HLevel) AS HLevel   INTO  #TempAllUnderlyingsCombinedOrdered from #TempAllUnderlyingsCombined  
GROUP BY UnderlyingEntityId,Entityid,Underlyingtype,AllocationTypeId,TrackingKey  
  
update C  
SET C.HLevel = O.HLevel  
FROM #TempAllUnderlyingsCombined C   
INNER JOIN #TempAllUnderlyingsCombinedOrdered O   
ON C.UnderlyingEntityId = O.UnderlyingEntityId   
AND C.Entityid = O.Entityid   
AND C.Underlyingtype = O.Underlyingtype  
AND  C.AllocationTypeId = O.AllocationTypeId   
AND C.TrackingKey = O.TrackingKey

CREATE TABLE #tempunderlyingMod(UnderlyingType INT, UnderlyingEntityID INT, EntityID INT, TrackingKey VARCHAR(4000), AllocationTypeID INT,
                                hlevel INT, Tag VARCHAR(5000))

CREATE NONCLUSTERED INDEX IX_tempunderlyingMod ON #tempunderlyingMod(AllocationTypeID)

INSERT INTO #tempunderlyingMod(UnderlyingType, UnderlyingEntityID, EntityID, TrackingKey, AllocationTypeId, hlevel, Tag)
SELECT DISTINCT AI.UnderlyingType, AI.UnderlyingEntityID, AI.EntityID, AI.TrackingKey, AI.AllocationTypeID, hlevel, C.Tag
FROM #TempAllUnderlyingsCombined AI
JOIN #CostPercentage_Snapshot C ON AI.Cost_Entity = C.EntityID AND AI.Cost_InvestmentID = C.InvestmentID AND AI.Cost_Quarter = C.Quarter
AND AI.Cost_AllocationTypeID = C.AllocationTypeID AND ISNULL(AI.Cost_TrackingKey,'') = ISNULL(C.TrackingKey,'') AND AI.Cost_UnderlyingType = C.UnderlyingType								   

IF(@LocalMode IN (1,4))    
BEGIN    
    
IF (@LocalIsPEModel = 0)    
BEGIN    
 INSERT INTO #TempLookThroughAllocationInput(RunID,ClientID,EntityID,LineTypeID,LineID,Amount,QuicklinkID,Amount704b,TrackingKey,Tag)    
 SELECT RunID,ClientID,EntityID,LineTypeID,LineID,Amount,QuicklinkID,Amount704b,TrackingKey,Tag    
 FROM LookThroughAllocationInput  (NOLOCK)    
 WHERE RunID = @LocalRunID    
 AND (LineTypeID = @BoxJKLLineTypeID OR (LineTypeID IN (@K1LineTypeID, @AdjustmentLineTypeID) AND Round(ISNULL(Amount,0),0) <> 0))   
 AND ClientID = @LocalClientID  AND LineTypeID IN (@K1LineTypeID, @AdjustmentLineTypeID, @BoxJKLLineTypeID)    
END    
ELSE    
BEGIN    
    INSERT INTO #TempLookThroughAllocationInput(RunID,ClientID,EntityID,LineTypeID,LineID,Amount,QuicklinkID,TrackingKey)    
 SELECT PEFundRunID,ClientID,InvestmentID,LineTypeID,LineID,InitialAmount,QuicklinkID,InvestmentID    
 FROM PE_AllocationInput  (NOLOCK)    
 WHERE PEFundRunID = @LocalRunID    
 AND Round(ISNULL( InitialAmount,0),0) <> 0      
 AND ClientID = @LocalClientID  AND LineTypeID IN (@K1LineTypeID, @AdjustmentLineTypeID, @BoxJKLLineTypeID)    
    
END    

    
Select DISTINCT M.BaseLineID FedLine, M.DerivedLineID FootnoteLine INTO #TempFootnoteLines    
From MAP_DerivedLines M with(nolock) INNER JOIN ENU_AttributeType EA    
ON M.AttributeID = EA.AttributeID    
WHERE EA.AttributeType = 'FN' AND M.DerivedLineID IS NOT NULL AND M.BaseLineID IS NOT NULL  
AND ISNULL(EA.IsHidden,0)=0
    
---mark the footnote lines to follow fed lines custom allocation.     
INSERT INTO #TempBookEffectiveData(UnderlyingEntityID, LineID, FootNoteID, SourceID, AllocationTypeid, AdjustmentAllocationTypeID, TrackingKey, Tag, IsExcludefromTransfer)    
Select DISTINCT I.EntityID, I.LineID, B.FootNoteID, B.SourceID, B.AllocationTypeid, B.AdjustmentAllocationTypeID, B.TrackingKey, B.Tag, ISNULL(B.IsExcludefromTransfer, 0)     
FROM #TempLookThroughAllocationInput I INNER JOIN #TempFootnoteLines M     
ON I.LineID = M.FootnoteLine    
INNER JOIN #TempBookEffectiveData B  ON B.LineID = M.FedLine AND B.SourceID = @K1LineTypeID    
AND B.UnderlyingEntityID = I.EntityID    
AND  CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
LEFT JOIN #TempBookEffectiveData B2  ON B2.LineID = M.FootnoteLine AND B2.SourceID = @K1LineTypeID    
AND B2.UnderlyingEntityID = I.EntityID     
AND  CASE WHEN ISNULL(B2.TrackingKey, '') = '' THEN '-1' ELSE B2.TrackingKey  END =     
    CASE WHEN ISNULL(B2.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B2.Tag, '') = '' THEN '-1' ELSE B2.Tag  END =     
    CASE WHEN ISNULL(B2.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
WHERE B2.UnderlyingEntityID IS NULL    
    
---------------------------------------------Get Allocation K1 and adjustment Input Data---------------------------------------------------    
IF(ISNULL(@IsCustomAllocationRuleEnabled, 'U') = 'C')  
BEGIN
    CREATE TABLE #K1LineItem(LineID INT, AllocationTypeRuleId INT)

	CREATE NONCLUSTERED INDEX IX_K1LineItem ON #K1LineItem(LineID, AllocationTypeRuleID)

	INSERT INTO #K1LineItem(LineID, AllocationTypeRuleID)
    SELECT DISTINCT K.LineID, CASE WHEN @LocalMode = 4 AND K.AllocationTypeRuleID = @CostAllocationTypeID
    THEN @allocationTypeIDfor704c ELSE K.AllocationTypeRuleID END AS AllocationTypeRuleID
    FROM K1LineItem K (NOLOCK)
    INNER JOIN #TempLookThroughAllocationInput I ON K.LineID = I.LineID

    INSERT INTO #TempAllUnderlyingsOrdered (Underlyingtype,UnderlyingEntityId ,EntityId ,TrackingKey ,TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup,LineTypeID, AllocationBy)     
    SELECT  Ai.Underlyingtype, AI.UnderlyingEntityId, AI.EntityId, L.TrackingKey,AI.TrackingKey TrackingMatch, ISNULL(B.AdjustmentAllocationTypeID,ISNULL(K.AllocationTypeRuleId, @CostAllocationTypeID)),L.LineID,    
    ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId, L.TrackingKey, L.LineID, CASE WHEN L.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE L.LineTypeID END ,AI.AllocationTypeId ORDER BY hlevel,U.DisplayOrder, AI.TrackingKey) RankForUnderlyingPickup     
    , CASE WHEN L.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE L.LineTypeID END LineTypeID  , 'PERCENT'   
    From #tempunderlyingMod AI     
    JOIN ENU_UnderlyingType U ON AI.Underlyingtype=U.UnderlyingTypeID    
    JOIN #TempLookThroughAllocationInput L ON L.EntityID = AI.UnderlyingEntityId AND    
    CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class') 
              OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '~' + L.TrackingKey + '~' END  LIKE    
    CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class') 
              OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '%'+Ai.TrackingKey+'%'END   
    AND  CASE WHEN ISNULL(AI.Tag, '') = '' THEN '-1' ELSE AI.Tag  END =       
    CASE WHEN ISNULL(AI.Tag, '') = '' THEN '-1' ELSE L.Tag  END 
    LEFT JOIN #TempBookEffectiveData B ON L.EntityID = B.UnderlyingEntityID AND L.LineID = B.LineId AND B.LineID <> -1    
    AND   B.SourceID = L.LineTypeID    
    AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE L.TrackingKey  END     
    AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE L.Tag  END    
    INNER JOIN #K1LineItem K ON L.LineID = K.LineID AND    
    ISNULL(B.AdjustmentAllocationTypeID,ISNULL(K.AllocationTypeRuleId, @CostAllocationTypeID)) = AI.AllocationTypeId   

    DROP TABLE IF EXISTS #K1LineItem
END  

-- Run DAR logic in CAR if we have mappings
-- This is for 'By Amount' allocations
IF(ISNULL(@IsCustomAllocationRuleEnabled, 'U') != 'C' OR (EXISTS (SELECT TOP 1 1 FROM #CostPercentage704cValues) AND EXISTS(SELECT TOP 1 1 FROM #Mappings)))
BEGIN  
 INSERT INTO #TempAllUnderlyingsOrdered (Underlyingtype,UnderlyingEntityId ,EntityId ,TrackingKey ,TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup,LineTypeID,AllocationBy,IsExcludefromTransfer)     
 SELECT  Ai.Underlyingtype, AI.UnderlyingEntityId, AI.EntityId, L.TrackingKey,AI.TrackingKey TrackingMatch, AI.AllocationTypeId,L.LineID,    
 ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId, L.TrackingKey, L.LineID, 
 CASE WHEN L.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE L.LineTypeID END, EA.DisplayOrder  
 ORDER BY hlevel, R.DisplayOrder DESC,U.DisplayOrder,M.SelectedMappingID DESC, EA.DisplayOrder, AI.TrackingKey) RankForUnderlyingPickup     
 , CASE WHEN L.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE L.LineTypeID END LineTypeID    
 ,EA.AllocationBy, ISNULL(M.ExcludeFromTransfers,0)    
 From #tempunderlyingMod AI     
 JOIN ENU_UnderlyingType U ON AI.Underlyingtype=U.UnderlyingTypeID    
 JOIN #TempLookThroughAllocationInput L ON L.EntityID = AI.UnderlyingEntityId AND    
 CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class')
 OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '~' + L.TrackingKey + '~' END  LIKE    
 CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class')
 OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '%'+Ai.TrackingKey+'%'END     
 JOIN #MapDefaultAllocRuleToLineItem M  
 ON CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE L.LineID END =CASE WHEN M.SelectedMappingID =-1 
 THEN 1 ELSE  M.SelectedMappingID END  AND M.RuleID=AI.AllocationTypeId     
 AND M.SourceID = CASE WHEN L.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE L.LineTypeID END    
 JOIN #DefaultAllocationRuleSetup D ON D.RuleID=AI.AllocationTypeId  AND AI.Underlyingtype=D.UnderlyingTypeID    
 JOIN ENU_RuleType R (NOLOCK) on D.RuleTypeID=R.RuleTypeID    
 JOIN ENU_AllocationBy EA ON D.AllocationByID=EA.AllocationByID    
 --LEFT JOIN MapRulesToUnderlyings MU ON    
 --CASE WHEN U.UnderlyingType = 'Asset Class' OR M.EntityID = -1 THEN '1' ELSE M.RuleID END  =    
 --CASE WHEN U.UnderlyingType = 'Asset Class' OR M.EntityID = -1 THEN '1' ELSE MU.RuleID END AND      
 --CASE WHEN U.UnderlyingType = 'Asset Class' OR M.EntityID = -1 THEN '1' ELSE AI.EntityId END  =    
 --CASE WHEN U.UnderlyingType = 'Asset Class' OR M.EntityID = -1 THEN '1' ELSE MU.UnderlyingID END     
 WHERE M.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID,-2)     
 AND D.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID,-2)     
END 

    
 INSERT INTO #TempAllUnderlyings (Underlyingtype,UnderlyingEntityId ,EntityId ,TrackingKey ,TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup,LineTypeID,AllocationBy,IsExcludefromTransfer)     
 SELECT Underlyingtype,UnderlyingEntityId,EntityId,TrackingKey, TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup,LineTypeID,AllocationBy,IsExcludefromTransfer    
 FROM #TempAllUnderlyingsOrdered WHERE RankForUnderlyingPickup = 1    
    
INSERT INTO #TempDefaultAllocationRule(LineId, AllocationRuleID, EntityID)    
Select SelectedMappingID, M.RuleID,L.UnderlyingEntityId    
FROM #MapDefaultAllocRuleToLineItem M    
INNER  JOIN #TempAllUnderlyings L ON  M.SelectedMappingID=L.LINEID    
JOIN ENU_Underlyingtype U on L.Underlyingtype = U.UnderlyingTypeId     
INNER JOIN MapRulesToUnderlyings MU (NOLOCK) ON    
 CASE WHEN U.UnderlyingType = 'Asset Class' THEN '1' ELSE M.RuleID END  =    
CASE WHEN U.UnderlyingType = 'Asset Class'  THEN '1' ELSE MU.RuleID END AND      
CASE WHEN U.UnderlyingType = 'Asset Class'  THEN '1' ELSE L.EntityId END  =    
CASE WHEN U.UnderlyingType = 'Asset Class'  THEN '1' ELSE MU.UnderlyingID END     
WHERE M.TransactionID IN (@DefaultAllocationRuleTransactionID) AND MU.TransactionID IN (@DefaultAllocationRuleTransactionID)    
       
INSERT INTO #TempDefaultAllocationRule(LineId, AllocationRuleID, EntityID)    
Select SelectedMappingID, M.RuleID,M.EntityID    
FROM #MapDefaultAllocRuleToLineItem M   
WHERE M.TransactionID IN (@GlobalDefaultAllocationRuleTransactionID)  
    
INSERT INTO #TempInputLines(UnderlyingEntityID,LineTypeID, LineID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)     
Select Distinct I.EntityID, I.LineTypeID, I.LineID,     
ISNULL(B.AdjustmentAllocationTypeID,     
 CASE WHEN K.AllocationTypeRuleId = @BookAllocationTypeID THEN @CostAllocationTypeID  ELSE ISNULL(ER.UpdatedAllocationRuleID, ISNULL(AI.AllocationTypeId, K.AllocationTypeRuleId)) END),     
ISNULL(B.TrackingKey, ISNULL(I.TrackingKey, '')), ISNULL(B.Tag,ISNULL(I.Tag, '')), ISNULL(B.IsExcludefromTransfer,AI.IsExcludefromTransfer)    
FROM #TempLookThroughAllocationInput I      
INNER JOIN #LineItem K ON I.LineID = K.LineID     
AND I.LineTypeID = CASE WHEN K.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE K.LineTypeID END    
LEFT JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID AND I.LineID = B.LineId AND B.LineID <> -1    
AND   B.SourceID = I.LineTypeID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
LEFT JOIN #TempEnitityAllocationRule ER ON ER.LineId = I.LineID    
--LEFT JOIN #TempDefaultAllocationRule DAR ON I.EntityID = DAR.EntityID AND  DAR.LineId = I.LineID    
--LEFT JOIN #TempDefaultAllocationRule GAR ON GAR.LineId = I.LineID AND GAR.EntityID = -1    
LEFT JOIN #TempAllUnderlyings AI ON I.EntityID =AI.UnderlyingEntityId AND I.TrackingKey= AI.TrackingKey AND I.LINEID=AI.LineID AND AI.LineTypeID = CASE WHEN I.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE I.LineTypeID END    
AND AI.AllocationBy = 'PERCENT'    
WHERE B.LineID!=-1 OR AI.LineID!=-1    
    
    
DELETE I    
FROM #TempLookThroughAllocationInput I     
LEFT JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID AND I.LineID = B.LineId    
AND   B.SourceID = I.LineTypeID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
LEFT JOIN #TempAllUnderlyings AI ON I.EntityID =AI.UnderlyingEntityId AND I.TrackingKey= AI.TrackingKey AND I.LINEID=AI.LineID AND AI.LineTypeID = CASE WHEN I.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE I.LineTypeID END    
WHERE B.LineID!=-1 OR AI.LineID!=-1    
    
    
DELETE FROM #TempBookEffectiveData WHERE ISNULL(LineID, 0) <> -1 AND SourceID = @K1LineTypeID    
    
INSERT INTO #TempInputLines(UnderlyingEntityID, LineTypeID, LineID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)     
Select Distinct I.EntityID, I.LineTypeID, I.LineID,     
ISNULL(B.AdjustmentAllocationTypeID,     
 CASE WHEN K.AllocationTypeRuleId = @BookAllocationTypeID THEN @CostAllocationTypeID ELSE ISNULL(ER.UpdatedAllocationRuleID, K.AllocationTypeRuleId) END),     
 ISNULL(B.TrackingKey, ISNULL(I.TrackingKey, '')), ISNULL(B.Tag,ISNULL(I.Tag, '')),     
 ISNULL(B.IsExcludefromTransfer, 0)    
FROM #TempLookThroughAllocationInput I     
Inner JOIN K1LineItem K with(nolock) ON I.LineID = K.LineID    
LEFT JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID    
AND   B.SourceID = I.LineTypeID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
LEFT JOIN #TempEnitityAllocationRule ER ON ER.LineId = I.LineID    
--LEFT JOIN #TempDefaultAllocationRule DAR ON I.EntityID = DAR.EntityID     
--LEFT JOIN #TempDefaultAllocationRule GAR ON GAR.LineId = I.LineID AND GAR.EntityID = -1    
--WHERE DAR.LIneID = -1 OR B.LineID = -1    
    
/* Calculate Effective Amounts to populate Allocated Amounts when allocation method is ByAmount */    
    
    
SELECT * INTO #LookThroughAllocationInput    
FROM LookThroughAllocationInput  (NOLOCK)    
WHERE RunID = @LocalRunID    
AND Round(ISNULL(Amount,0),0) <> 0      
AND ClientID = @LocalClientID  AND LineTypeID IN (@K1LineTypeID, @AdjustmentLineTypeID, @BoxJKLLineTypeID)    
    
IF(@LocalMode = 1 AND EXISTS(SELECT TOP 1 1 FROM #Mappings))
BEGIN
	CREATE TABLE #LookThroughAllocationInputAmounts(RunID BIGINT, ClientID BIGINT, EntityID INT, CostPercentageInvestmentID INT, LineTypeID INT, LineID INT, Amount FLOAT, QuicklinkID INT, CategoryID INT, ParentEntityID INT, PeriodID INT, LineCode VARCHAR(100), SuperParentEntityID INT, AdjustmentTypeID INT, TrackingKey VARCHAR(4000), Tag VARCHAR(5000), OriginalParentEntityID INT, FlowUpPartner VARCHAR(50))
	CREATE TABLE #LookThroughAllocationInputGrouped(CostPercentageInvestmentID INT, LineID INT, LineTypeID INT, Amount FLOAT)

    SELECT DISTINCT RegisterLineID INTO #DistinctMappedLines
	FROM #Mappings

	IF(ISNULL(@704cAllocationTypeName, '') LIKE '%Aggregate 704(c)%')
	BEGIN
		INSERT INTO #LookThroughAllocationInputAmounts(RunID, ClientID, EntityID, CostPercentageInvestmentID, LineTypeID, LineID, Amount, QuicklinkID, CategoryID, ParentEntityID, PeriodID, LineCode, SuperParentEntityID, AdjustmentTypeID, TrackingKey, Tag, OriginalParentEntityID, FlowUpPartner)
		SELECT LT.RunID, LT.ClientID, LT.EntityID, @LocalEntityID, LT.LineTypeID, LT.LineID, LT.Amount, LT.QuickLinkID, LT.CategoryID, LT.ParentEntityID, LT.PeriodID, LT.LineCode, LT.SuperParentEntityID, LT.AdjustmentTypeID, LT.TrackingKey, LT.Tag, LT.OriginalParentEntityID, LT.FlowUpPartner
		FROM #LookThroughAllocationInput LT
		INNER JOIN #DistinctMappedLines MS ON MS.RegisterLineID = LT.LineID
	END
	ELSE IF(ISNULL(@704cAllocationTypeName, '') = '704(c) - SP')
	BEGIN
        CREATE TABLE #DistinctCostPercentage_Snapshot(WorkFlowID INT, TransactionID INT, EntityID INT, InvestmentID INT, 
        Quarter VARCHAR(10), CommitmentPercent FLOAT, AllocationTypeID INT, Tag VARCHAR(4000), TrackingKey VARCHAR(4000),
        UnderlyingType INT, AllocatedAmount FLOAT, EntityUnderlyingType VARCHAR(100))

        INSERT INTO #DistinctCostPercentage_Snapshot(WorkFlowID, TransactionID, EntityID, InvestmentID, Quarter, CommitmentPercent, AllocationTypeID, Tag, TrackingKey,
        UnderlyingType, AllocatedAmount, EntityUnderlyingType)
        SELECT DISTINCT CS.WorkFlowID, CS.TransactionID, CS.EntityID, CS.InvestmentID, CS.Quarter, CS.CommitmentPercent, CS.AllocationTypeID, CS.Tag, CS.TrackingKey,
        CS.UnderlyingType, CS.AllocatedAmount, CS.EntityUnderlyingType
        FROM #CostPercentage_Snapshot CS
        INNER JOIN ENU_CustomAllocations ET (NOLOCK) ON ET.AllocationTypeID = CS.AllocationTypeID
        INNER JOIN #Mappings MS ON 'Special ' + MS.DatabaseName = ET.AllocationType

		INSERT INTO #LookThroughAllocationInputAmounts(RunID, ClientID, EntityID, CostPercentageInvestmentID, LineTypeID, LineID, Amount, QuicklinkID, CategoryID, ParentEntityID, PeriodID, LineCode, SuperParentEntityID, AdjustmentTypeID, TrackingKey, Tag, OriginalParentEntityID, FlowUpPartner)
		SELECT DISTINCT LT.RunID, LT.ClientID, LT.EntityID, CS.InvestmentID, LT.LineTypeID, LT.LineID, LT.Amount, LT.QuickLinkID, LT.CategoryID, LT.ParentEntityID, LT.PeriodID, LT.LineCode, LT.SuperParentEntityID, LT.AdjustmentTypeID, LT.TrackingKey, LT.Tag, LT.OriginalParentEntityID, LT.FlowUpPartner
		FROM #LookThroughAllocationInput LT
		INNER JOIN #TempAllUnderlyings T ON T.UnderlyingEntityID = LT.EntityID AND T.LineID = LT.LineID AND T.TrackingKey = LT.TrackingKey
		INNER JOIN #DistinctCostPercentage_Snapshot CS ON CS.InvestmentID = T.EntityID AND CS.AllocationTypeID = T.AllocationTypeID

        DROP TABLE IF EXISTS #DistinctCostPercentage_Snapshot
	END

    INSERT INTO #LookThroughAllocationInputGrouped(CostPercentageInvestmentID, LineID, LineTypeID, Amount)
	SELECT CostPercentageInvestmentID, LineID, LineTypeID, SUM(Amount)
	FROM #LookThroughAllocationInputAmounts LT
	GROUP BY CostPercentageInvestmentID, LineID, LineTypeID

    INSERT INTO #EntityTotalAmounts(UnderlyingEntityID, PartnerNumber, Quarter, CommitmentPercent, AllocationTypeID,
    TrackingKey, Tag, LineID, InputAmount, AllocatedAmount, CostEntityID, UnderlyingTypeID, LineTypeID, GPPartnerReceivingCarry)
	SELECT E.UnderlyingEntityID, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0), ISNULL(C.AllocationTypeId, @CostAllocationTypeID),    
	ISNULL(AI.TrackingKey, ''), ISNULL(AI.Tag, ''), AI.LineID, ISNULL(AI.Amount ,0), ISNULL(C.AllocatedAmount/LT.Amount, 0) * AI.Amount, C.InvestmentID, ISNULL(C.UnderlyingType, @EntityUnderlyingtype), AI.LineTypeID , C.GPPartnerReceivingCarry   
	FROM ENU_UnderlyingType U
	INNER JOIN #TempAllUnderlyings E ON U.UnderlyingTypeID = E.UnderlyingType
	INNER JOIN #CostPercentage_Snapshot C 
	ON E.EntityID = C.InvestmentID
	AND C.UnderlyingType = E.UnderlyingType
	AND E.AllocationTypeID = C.AllocationTypeID
	AND '~' +CASE WHEN ISNULL(C.TrackingKey, '') = '' THEN CONVERT(VARCHAR(4000), C.InvestmentID) ELSE C.TrackingKey END  +'~' = E.TrackingMatch    
	INNER JOIN #LookThroughAllocationInputAmounts AI ON E.UnderlyingEntityID = AI.EntityID AND E.LineTypeID = AI.LineTypeID AND AI.LineID = E.LineID AND ISNULL(AI.TrackingKey, '') = ISNULL(E.TrackingKey, '')
	INNER JOIN #LookThroughAllocationInputGrouped LT ON LT.CostPercentageInvestmentID = E.EntityId AND LT.LineID = AI.LineID AND LT.LineTypeID = AI.LineTypeID
	INNER JOIN #MapDefaultAllocRuleToLineItem M
	ON C.AllocationTypeId = M.RuleID    
	AND CASE WHEN M.SelectedMappingID = -1 THEN 1 ELSE M.SelectedMappingID END = CASE WHEN M.SelectedMappingID = -1 THEN 1 ELSE AI.LineID END    
	INNER JOIN ENU_LineType EL ON M.SourceID = El.LineTypeID
	WHERE ISNULL(C.UnderlyingType, '') IN (@UnderlyingOnlyUnderlyingType, @EntityTotalUnderlyingType)
	AND EL.LineTypeID IN (@K1LineTypeID, @BoxJKLLineTypeID)    
	AND AI.LineTypeID = M.SourceID AND ISNULL(C.AllocatedAmount, 0) <> 0    
	AND M.TransactionID IN (@DefaultAllocationRuleTransactionID, @GlobalDefaultAllocationRuleTransactionID, -2)     
	AND E.AllocationBy = 'AMOUNT'

    DROP TABLE IF EXISTS #DistinctMappedLines
    DROP TABLE IF EXISTS #LookThroughAllocationInputAmounts
    DROP TABLE IF EXISTS #LookThroughAllocationInputGrouped
END

INSERT INTO #EntityTotalAmounts(UnderlyingEntityID, PartnerNumber, Quarter, CommitmentPercent, AllocationTypeID,
TrackingKey, Tag, LineID, InputAmount, AllocatedAmount, CostEntityID, UnderlyingTypeID, LineTypeID, GPPartnerReceivingCarry)
SELECT E.UnderlyingEntityID, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0), ISNULL(C.AllocationTypeId, @CostAllocationTypeID),    
ISNULL(AI.TrackingKey, ''), ISNULL(AI.Tag, ''), AI.LineID, ISNULL(AI.Amount ,0), ISNULL(C.AllocatedAmount, 0), C.InvestmentID, ISNULL(C.UnderlyingType, @EntityUnderlyingtype), AI.LineTypeID, C.GPPartnerReceivingCarry 
FROM ENU_UnderlyingType U INNER JOIN #TempAllUnderlyings E ON U.UnderlyingTypeid = E.UnderlyingType     
INNER JOIN #CostPercentage_Snapshot C  ON  E.EntityID = C.InvestmentID AND C.UnderlyingType = E.UnderlyingType     
AND E.AllocationTypeId = C.AllocationTypeId     
AND '~' +CASE WHEN ISNULL(C.TrackingKey,'') = '' THEN CONVERT(VARCHAR(4000), C.InvestmentID) ELSE C.TrackingKey END  +'~' =E.TrackingMatch    
--INNER JOIN #TempInputLines L ON L.UnderlyingEntityID = E.EntityID AND E.TRACKINGKEY=L.TRACKINGKEY AND L.TypeID=C.AllocationTypeId    
INNER JOIN #LookThroughAllocationInput AI ON E.UnderlyingEntityID = AI.EntityID AND E.LineTypeID = AI.LineTypeID AND AI.LineID= E.LineID    
JOIN #MapDefaultAllocRuleToLineItem M ON C.AllocationTypeId = M.RuleID    
AND CASE WHEN M.SelectedMappingID = -1 THEN 1 ELSE M.SelectedMappingID END = CASE WHEN M.SelectedMappingID = -1 THEN 1 ELSE AI.LineID END    
JOIN ENU_LINETYPE EL ON M.SOURCEID= El.LineTypeID    
WHERE ISNULL(C.UnderlyingType, '') = @EntityUnderlyingtype
AND El.LineTypeID IN (@K1LineTypeID,@BoxJKLLineTypeID)    
AND AI.LineTypeID=M.SourceID AND ISNULL(C.AllocatedAmount ,0) <> 0    
AND M.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID,-2)     
AND E.AllocationBy = 'AMOUNT' 
    
INSERT INTO #TotalUnderlyingAmounts(LineID, Partnernumber, TotalAmount, CostEntityId,AllocationTypeId,TrackingKey,  Tag, LineTypeID)    
SELECT LineID, Partnernumber, SUM(ISNULL(InputAmount,0)) AS TotalAmount , CostEntityId,AllocationTypeId,TrackingKey,  Tag, LineTypeID    
FROM #EntityTotalAmounts     
GROUP BY LineID, Partnernumber, CostEntityId,AllocationTypeId,TrackingKey,  Tag , LineTypeID     
    
INSERT INTO #FinalEffectiveAmounts(UnderlyingEntityID,LineID,Partnernumber, Quarter,TypeId,TrackingKey, Tag, EffectiveAmount ,UnderlyingTypeId, LineTypeID, GPPartnerReceivingCarry)    
SELECT C.UnderlyingEntityID,C.LineId, C.PartnerNumber , C.Quarter, C.AllocationTypeId, C.TrackingKey, C.Tag,     
CASE  WHEN T.TotalAmount <> 0 THEN (C.InputAmount/T.TotalAmount) * C.AllocatedAmount ELSE 0 END AS EffectiveAmount ,UnderlyingTypeId, C.LineTypeID, C.GPPartnerReceivingCarry    
From #TotalUnderlyingAmounts T JOIN #EntityTotalAmounts C ON C.CostEntityId = T.CostEntityId     
AND C.AllocationTypeId = T.AllocationTypeId     
AND C.TrackingKey = T.TrackingKey     
AND C.Tag = T.Tag     
AND C.LineID = T.LineID AND C.PartnerNumber = T.Partnernumber AND T.LineTypeID = C.LineTypeID      
    
INSERT INTO #FinalAmounts(InvestmentID,Partnernumber, AllocationType,Quarter, TypeId,TrackingKey, Tag, LineId, EffectiveAmount, UnderlyingTypeId, LineTypeID, GPPartnerReceivingCarry)    
SELECT UnderlyingEntityID,  PartnerNumber , 'Cost', Quarter, TypeId, TrackingKey, Tag, LineId, EffectiveAmount, UnderlyingTypeId, LineTypeID, GPPartnerReceivingCarry    
FROM #FinalEffectiveAmounts

-- Delete data from #TempAllUnderlyings in case of CAR to be safe
-- Final efective amounts already calculated in above query and will be returned from sp in the end
IF(ISNULL(@IsCustomAllocationRuleEnabled, 'U') = 'C')
BEGIN
	DELETE FROM #TempAllUnderlyings WHERE AllocationBy = 'AMOUNT'
END
    
--DELETE L FROM #TempInputLines L INNER JOIN #FinalAmounts EFF ON L.LineId = EFF.LineId AND L.UnderlyingEntityID = EFF.InvestmentID    
--AND L.TrackingKey=EFF.TrackingKey    
--AND L.Tag=EFF.Tag    
    
DROP TABLE #TempFootnoteLines    
DROP TABLE #LookThroughAllocationInput    
--DROP TABLE #TempAllUnderlyings    
DROP TABLE #EntityTotalAmounts    
--DROP TABLE #TempAllUnderlyingsOrdered    
-----------------------------------------------------------------------------------------------------------------------------    
    
----------------------------------------------Non Dated Entities---------------------------------------------------------------    
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)    
SELECT DISTINCT L.UnderlyingEntityID, L.LineTypeID, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)    
FROM    
#TempInputLines L      
INNER JOIN #LineItem K ON K.LineID = L.LineID  AND CASE WHEN K.LineTypeID = @BoxJKLLineTypeID THEN ISNULL(K.IsTransactionDate,0)  ELSE 
ISNULL(K.IsTransfersAdjusted,0) END = 0    

-------------------------------------------------DATED LINES----------------------------------------------------------------    
IF(@LocalMode != 4)
BEGIN
IF (@AllocationTypeName = 'PE Book Allocation' AND @IsDatedTransfersConfigured = 'C' )    
BEGIN    
INSERT INTO #TempDatedEntities(Quarter, UnderlyingEntityID, TypeID, TrackingKey, Tag, IsExcludefromTransfer, LineID, LineTypeID, Preference)    
SELECT Distinct ISNULL(D.QUARTER, 'Q0'), L.UnderlyingEntityID EntityID, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0), K.LineID , L.LineTypeID, D.Preference   
FROM    
#TempInputLines L      
INNER JOIN K1LineItem K with(nolock) ON K.LineID = L.LineID AND K.IsTransactionDate = 1 AND IsTransfersAdjusted = 1    
INNER Join Quarterdates D (NOLOCK) On ISNULL(K.TransactionDate, '1900-01-01') BETWEEN D.StartDate and D.EndDate  
WHERE L.LineTypeID = @K1LineTypeID        
END    
ELSE    
BEGIN    
INSERT INTO #TempDatedEntities(Quarter, UnderlyingEntityID,LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer, LineID)    
SELECT Distinct D.LookUpData QUARTER, L.UnderlyingEntityID EntityID, L.LineTypeID, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0), K.LineID    
FROM    
#TempInputLines L      
INNER JOIN #LineItem K ON K.LineID = L.LineID AND K.IsTransactionDate = 1 AND IsTransfersAdjusted = 1 AND L.LineTypeID = K.LineTypeID    
Inner Join ENU_DF_DataList D On D.LookUpValue = ISNULL(Month(K.TransactionDate),0) AND D.Category = 'QuarterMonth'    
END    
END    
END
    
IF(@LocalMode = 2 OR (@LocalIsPEModel=1 AND @LocalMode=1) OR @LocalMode = 4)    
-------------------------------------------------PFIC LINES----------------------------------------------------------------    
BEGIN     
    
Declare @PFICFootNoteLineTypeID INT    
    
SELECT @PFICFootNoteLineTypeID = LineTypeID FROM ENU_LineType WHERE LineType = 'PFIC Footnote'    
    
---- Custom Footnote changes  
CREATE TABLE #tmpCustomFootnoteLineTypes( [LineTypeID] [INT])  
INSERT INTO #tmpCustomFootnoteLineTypes(LineTypeID)  
SELECT DISTINCT LineTypeID FROM dbo.udfGetLatestCustomFootnoteTransactionIDs(@LocalClientID,@LocalTaxPeriodID,CONVERT(VARCHAR(10),@LocalEntityID),@PhaseID,-1,0,0)  
    
IF(ISNULL(@IsCustomAllocationRuleEnabled, 'U') = 'C')  
BEGIN  
  INSERT INTO #TempAllUnderlyingsFNOrdered (Underlyingtype,UnderlyingEntityId ,EntityId ,TrackingKey ,TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup,LineTypeID, AllocationBy,IsExcludefromTransfer)     

	 SELECT  Ai.Underlyingtype, AI.UnderlyingEntityId, AI.EntityId, L.TrackingKey,AI.TrackingKey TrackingMatch,  CASE WHEN ISNULL(B.AdjustmentAllocationTypeID,'') = '' and @localMode=4  
	THEN @allocationTypeIDfor704c  else
	ISNULL(B.AdjustmentAllocationTypeID,isnull(D.RuleID,@CostAllocationTypeID)) end,L.LineID,  

  ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId, L.TrackingKey, L.LineID, CASE WHEN L.LineTypeID = @AdjustmentLineTypeID THEN @K1LineTypeID ELSE L.LineTypeID END ,AI.AllocationTypeId ORDER BY hlevel,U.DisplayOrder, AI.TrackingKey) RankForUnderlyingPickup     
  , L.LineTypeID  , 'PERCENT', M.ExcludeFromTransfers  
  From #tempunderlyingMod AI     
  JOIN ENU_UnderlyingType U ON AI.Underlyingtype=U.UnderlyingTypeID    
  JOIN #TempAllocationInput L ON L.EntityID = AI.UnderlyingEntityId AND    
  CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class') OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '~' + L.TrackingKey + '~' END 
  LIKE    
  CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class') OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '%'+Ai.TrackingKey+'%'END    
  INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID  
  LEFT JOIN #TempBookEffectiveData B ON L.EntityID = B.UnderlyingEntityID AND L.LineID = B.LineId AND B.LineID <> -1    
  AND   B.SourceID = L.LineTypeID    
  AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
  CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE L.TrackingKey  END     
  AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
  CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE L.Tag  END   
   AND    
  CASE WHEN ISNULL(B.AdjustmentAllocationTypeID,'') = '' and @localMode=4  
	THEN @allocationTypeIDfor704c  else
	ISNULL(B.AdjustmentAllocationTypeID,@CostAllocationTypeID) end = AI.AllocationTypeId   
LEFT JOIN #MapDefaultAllocRuleToLineItem M ON 
CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE L.LineID END =CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE  M.SelectedMappingID END 
    AND M.RuleID=AI.AllocationTypeId AND M.SourceID = L.LineTypeID AND M.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)     
LEFT JOIN #DefaultAllocationRuleSetup D ON D.RuleID=AI.AllocationTypeId  AND AI.Underlyingtype=D.UnderlyingTypeID  AND D.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)  
where EL.LineType IN ('PFIC Footnote', 'Form926', 'Form8865',  'Form1042S', 'Form8886', 'Form199A', 'At Risk') 
OR L.LineTypeID IN (SELECT LineTypeID FROM #tmpCustomFootnoteLineTypes)
END  
ELSE  
BEGIN   
  
  
INSERT INTO #TempAllUnderlyingsFNOrdered (Underlyingtype,UnderlyingEntityId ,EntityId ,TrackingKey ,TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup,LineTypeID,IsExcludefromTransfer)       
SELECT  AI.Underlyingtype, AI.UnderlyingEntityId, AI.EntityId, L.TrackingKey , AI.TrackingKey TrackingMatch,      
AI.AllocationTypeId,L.LineID , ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId, L.TrackingKey, L.LineID,L.LineTypeID  ORDER BY hlevel,R.DisplayOrder DESC, U.DisplayOrder,M.SelectedMappingID DESC, AI.TrackingKey) RankForUnderlyingPickup       
,L.LineTypeID ,M.ExcludeFromTransfers     
FROM #tempunderlyingMod AI        
JOIN ENU_UnderlyingType U ON AI.Underlyingtype=U.UnderlyingTypeID      
JOIN #TempAllocationInput L ON L.EntityID = AI.UnderlyingEntityId AND      
CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class') OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '~' + L.TrackingKey + '~' END  
LIKE    
 CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class') OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '%'+Ai.TrackingKey+'%'END     
JOIN #MapDefaultAllocRuleToLineItem M ON CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE L.LineID END =CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE  M.SelectedMappingID END  AND M.RuleID=AI.AllocationTypeId AND M.SourceID = L.LineTypeID    
JOIN #DefaultAllocationRuleSetup D ON D.RuleID=AI.AllocationTypeId  AND AI.Underlyingtype=D.UnderlyingTypeID    
JOIN ENU_RuleType R (NOLOCK) on D.RuleTypeID=R.RuleTypeID    
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID    
LEFT JOIN #tmpCustomFootnoteLineTypes CF ON CF.LineTypeID=L.LineTypeID  
WHERE M.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)     
AND D.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)     
AND (EL.LineType IN ('PFIC Footnote', 'Form926', 'Form8865',  'Form1042S', 'Form8886', 'Form199A', 'At Risk')    
OR  L.LineTypeID = CF.LineTypeID)  
  
         
-- Insert k-1 rule if At Risk rule is not present in the DAR import but At Risk input is present    
    
INSERT INTO #TempAllUnderlyingsFNOrdered (Underlyingtype,UnderlyingEntityId ,EntityId ,TrackingKey ,TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup,LineTypeID,AllocationBy,IsExcludefromTransfer)         
SELECT  AI.Underlyingtype, AI.UnderlyingEntityId, AI.EntityId, L.TrackingKey , AI.TrackingKey TrackingMatch,        
AI.AllocationTypeId,L.LineID , ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId, L.TrackingKey, L.LineID,L.LineTypeID,EA.DisplayOrder     
ORDER BY hlevel,R.DisplayOrder DESC, U.DisplayOrder,M.SelectedMappingID DESC, EA.DisplayOrder, AI.TrackingKey) RankForUnderlyingPickup         
,L.LineTypeID  ,EA.AllocationBy ,M.ExcludeFromTransfers        
FROM #TempAllUnderlyingsCombined AI          
JOIN ENU_UnderlyingType U ON AI.Underlyingtype=U.UnderlyingTypeID        
JOIN #TempAllocationInput L ON L.EntityID = AI.UnderlyingEntityId AND        
CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class') OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '~' + L.TrackingKey + '~' END      
LIKE     
CASE WHEN AI.UnderlyingEntityId = @LocalEntityID OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class') OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class') THEN '-1' ELSE '%'+Ai.TrackingKey+'%'END     
JOIN #MapDefaultAllocRuleToLineItem M ON CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE L.LineID END =CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE  M.SelectedMappingID END  AND M.RuleID=AI.AllocationTypeId AND M.SourceID =  @K1LineTypeID      
JOIN #DefaultAllocationRuleSetup D ON D.RuleID=AI.AllocationTypeId  AND AI.Underlyingtype=D.UnderlyingTypeID        
JOIN ENU_RuleType R (NOLOCK) on D.RuleTypeID=R.RuleTypeID      
JOIN ENU_AllocationBy EA (NOLOCK) ON D.AllocationByID=EA.AllocationByID        
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID        
LEFT JOIN #TempAllUnderlyingsFNOrdered TFO ON AI.UnderlyingEntityId = TFO.UnderlyingEntityId AND AI.EntityId = TFO.EntityId AND L.LineID = TFO.LineID AND L.LineTypeID = TFO.LineTypeID      
AND L.LineTypeID = @AtRiskLineTypeID       
WHERE  M.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)         
AND D.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)         
AND EL.LineType IN ('At Risk')       
AND TFO.LineTypeID IS NULL    
    
    
END    
      
INSERT INTO #TempAllUnderlyings (Underlyingtype,UnderlyingEntityId ,EntityId ,TrackingKey ,TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup, LineTypeID,IsExcludefromTransfer)       
SELECT Underlyingtype,UnderlyingEntityId,EntityId,TrackingKey, TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup ,  LineTypeID,IsExcludefromTransfer      
FROM #TempAllUnderlyingsFNOrdered WHERE RankForUnderlyingPickup = 1       
      
      
Select * INTO #TempFootnoteBookEffectiveData From #TempBookEffectiveData      
---------------------------------------------Get Allocation Input Data---------------------------------------------------      
      
--2782265:TPG - Clean Up Footnote IDs -Sarath: to include all lines      
--INSERT INTO #TempInputLines(UnderlyingEntityID, LineID, QuickLinkID, TypeID, TrackingKey, Tag, LineTypeID)       
--Select Distinct I.EntityID, I.LineID, QuicklinkID, ISNULL(B.AdjustmentAllocationTypeID, @CostAllocationTypeID), ISNULL(B.TrackingKey, ''), ISNULL(B.Tag,ISNULL(I.Tag, '')), I.LineTypeID      
--FROM #TempAllocationInput I        
--INNER JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID       
--AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)      
--AND  I.LineTypeID = B.SourceID      
--Where ISNULL(B.FootNoteID, 0) <> -1      
      
--UPDATE I      
--SET I.IsExcludefromTransfer = B.IsExcludefromTransfer      
--FROM #TempInputLines I      
--INNER JOIN #TempBookEffectiveData B      
--ON I.UnderlyingEntityID = B.UnderlyingEntityID         
--AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)        
--AND  I.LineTypeID = B.SourceID  AND I.LineID = B.LineID      
    
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID, QuickLinkID, TypeID, TrackingKey, Tag, LineTypeID, IsExcludefromTransfer)           
Select Distinct I.EntityID, I.LineID, QuicklinkID,         
ISNULL(B.AdjustmentAllocationTypeID, ISNULL(Bk.AdjustmentAllocationTypeID, ISNULL( AI.AllocationTypeId,        
 CASE WHEN I.LineTypeID =@AtRiskLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- LP - Offset' THEN @LPOffsetAllocationTypeID         
   WHEN I.LineTypeID = @AtRiskLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- GP - Offset' THEN @GPOffsetAllocationTypeID         
   ELSE @CostAllocationTypeID END))),         
ISNULL(B.TrackingKey, ISNULL(I.TrackingKey,'')), ISNULL(B.Tag,ISNULL(I.Tag, '')), I.LineTypeID, ISNULL(B.IsExcludefromTransfer, AI.IsExcludefromTransfer)           
FROM #TempAllocationInput I    
INNER JOIN K1Lineitem P with(nolock) ON P.LineID=I.LineID     
LEFT JOIN MAP_K1LineItemLineType M (NOLOCK) ON I.LineID = M.K1LineItemID AND I.LineTypeID = @AtRiskLineTypeID       
LEFT JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID      
AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)          
AND ISNULL(I.LineID, 0) = ISNULL(B.LineID, 0)        
AND  B.SourceID  =@AtRiskLineTypeID      
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =         
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END         
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =         
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END       
LEFT JOIN #TempBookEffectiveData BK ON I.EntityID = BK.UnderlyingEntityID           
AND ISNULL(I.LineID, 0) = ISNULL(BK.LineID, 0)        
AND  BK.SourceID  =@K1LineTypeID      
AND CASE WHEN ISNULL(BK.TrackingKey, '') = '' THEN '-1' ELSE BK.TrackingKey  END =         
    CASE WHEN ISNULL(BK.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END         
AND CASE WHEN ISNULL(BK.Tag, '') = '' THEN '-1' ELSE BK.Tag  END =         
    CASE WHEN ISNULL(BK.Tag, '') = '' THEN '-1' ELSE I.Tag  END        
LEFT JOIN #TempAllUnderlyings AI ON I.EntityID = AI.UnderlyingEntityId AND I.LINEID = AI.LineID AND I.LineTypeID = AI.LineTypeID AND I.TrackingKey= AI.TrackingKey         
Where  (ISNULL(B.FootNoteID, 0) <> -1 AND    
ISNULL(B.LineID, 0) <> -1 OR AI.LineID <> -1 OR  ISNULL(BK.LineID, 0) <> -1) AND I.LineTypeID = @AtRiskLineTypeID    
    
        
DELETE I        
FROM #TempAllocationInput I            
LEFT JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID    
AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)            
AND ISNULL(I.LineID, 0) = ISNULL(B.LineID, 0)        
AND  I.LineTypeID = @AtRiskLineTypeID   
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =         
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END         
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =         
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END         
LEFT JOIN #TempAllUnderlyings AI ON I.EntityID = AI.UnderlyingEntityId AND I.LINEID = AI.LineID AND I.LineTypeID = AI.LineTypeID AND I.TrackingKey=AI.TrackingKey        
Where (ISNULL(B.FootNoteID, 0) <> -1 AND     
(ISNULL(B.LineID, 0) <> -1) OR AI.LineID <> -1) AND I.LineTypeID = @AtRiskLineTypeID    
        
DELETE FROM #TempBookEffectiveData WHERE ISNULL(FootNoteID, 0) <> -1 AND       
ISNULL(LineID, 0) <> -1 AND SourceID = @AtRiskLineTypeID    
      
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID, QuickLinkID, TypeID, TrackingKey, Tag, LineTypeID, IsExcludefromTransfer)         
Select Distinct I.EntityID, I.LineID, QuicklinkID,       
ISNULL(B.AdjustmentAllocationTypeID, ISNULL(AI.AllocationTypeId,      
 CASE WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- LP - Offset' THEN @LPOffsetAllocationTypeID       
   WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- GP - Offset' THEN @GPOffsetAllocationTypeID       
   ELSE @CostAllocationTypeID END)),       
ISNULL(B.TrackingKey, ISNULL(I.TrackingKey,'')), ISNULL(B.Tag,ISNULL(I.Tag, '')), I.LineTypeID, ISNULL(B.IsExcludefromTransfer, AI.IsExcludefromTransfer)         
FROM #TempAllocationInput I        
LEFT JOIN PFICFootnoteLineItem P with(nolock) ON I.LineID = P.LineID AND I.LineTypeID = @PFICFootNoteLineTypeID      
LEFT JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID         
AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)     
AND ISNULL(I.LineID, 0) = ISNULL(B.LineID, 0)      
AND  I.LineTypeID = B.SourceID        
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END       
LEFT JOIN #TempAllUnderlyings AI ON I.EntityID = AI.UnderlyingEntityId AND I.LINEID = AI.LineID AND I.LineTypeID = AI.LineTypeID AND I.TrackingKey= AI.TrackingKey       
Where (ISNULL(B.FootNoteID, 0) <> -1 AND     
ISNULL(B.LineID, 0) <> -1) OR AI.LineID <> -1       
      
DELETE I      
FROM #TempAllocationInput I          
LEFT JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID         
AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)     
AND ISNULL(I.LineID, 0) = ISNULL(B.LineID, 0)      
AND  I.LineTypeID = B.SourceID        
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END       
LEFT JOIN #TempAllUnderlyings AI ON I.EntityID = AI.UnderlyingEntityId AND I.LINEID = AI.LineID AND I.LineTypeID = AI.LineTypeID AND I.TrackingKey=AI.TrackingKey      
Where (ISNULL(B.FootNoteID, 0) <> -1 AND     
ISNULL(B.LineID, 0) <> -1) OR AI.LineID <> -1       
      
DELETE FROM #TempBookEffectiveData WHERE ISNULL(FootNoteID, 0) <> -1 AND     
ISNULL(LineID, 0) <> -1      
      
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID, QuickLinkID, TypeID, TrackingKey, Tag, LineTypeID, IsExcludefromTransfer)         
Select Distinct I.EntityID, I.LineID, QuicklinkID,       
ISNULL(B.AdjustmentAllocationTypeID,       
 CASE WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- LP - Offset' THEN @LPOffsetAllocationTypeID       
   WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- GP - Offset' THEN @GPOffsetAllocationTypeID       
   ELSE @CostAllocationTypeID END),       
ISNULL(B.TrackingKey, ISNULL(I.TrackingKey,'')), ISNULL(B.Tag,ISNULL(I.Tag, '')), I.LineTypeID, ISNULL(B.IsExcludefromTransfer, 0)         
FROM #TempAllocationInput I          
LEFT JOIN PFICFootnoteLineItem P with(nolock) ON I.LineID = P.LineID AND I.LineTypeID = @PFICFootNoteLineTypeID      
INNER JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID         
AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)        
AND  I.LineTypeID = B.SourceID       
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END       
--LEFT JOIN #TempDefaultAllocationRule DAR ON I.EntityID = DAR.EntityID       
--LEFT JOIN #TempDefaultAllocationRule GAR ON GAR.LineId = I.LineID AND GAR.EntityID = -1      
Where ISNULL(B.FootNoteID, 0) <> -1 AND     
ISNULL(B.LineID, 0) = -1      
      
DELETE I      
FROM #TempAllocationInput I          
INNER JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID         
AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)        
AND  I.LineTypeID = B.SourceID       
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END       
Where ISNULL(B.FootNoteID, 0) <> -1 AND     
ISNULL(B.LineID, 0) = -1      
      
      
DELETE FROM #TempBookEffectiveData WHERE ISNULL(FootNoteID, 0) <> -1 AND     
ISNULL(LineID, 0) = -1      
      
      
      
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID, QuickLinkID, TypeID, TrackingKey, Tag, LineTypeID, IsExcludefromTransfer)          
Select Distinct I.EntityID, I.LineID, QuicklinkID,       
ISNULL(B.AdjustmentAllocationTypeID,      
 CASE WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- LP - Offset' THEN @LPOffsetAllocationTypeID       
   WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- GP - Offset' THEN @GPOffsetAllocationTypeID       
   ELSE @CostAllocationTypeID END),       
ISNULL(B.TrackingKey, ISNULL(I.TrackingKey,'')), ISNULL(B.Tag,ISNULL(I.Tag, '')), I.LineTypeID, ISNULL(B.IsExcludefromTransfer, 0)         
FROM #TempAllocationInput I          
LEFT JOIN PFICFootnoteLineItem P with(nolock) ON I.LineID = P.LineID AND I.LineTypeID = @PFICFootNoteLineTypeID      
INNER JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID       
AND ISNULL(I.QuicklinkID,0) = ISNULL(B.FootNoteID , 0)       
AND ISNULL(B.LineID, 0) = ISNULL(I.LineID, 0)       
AND  I.LineTypeID = B.SourceID       
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
                         CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END      
--LEFT JOIN #TempDefaultAllocationRule DAR ON I.EntityID = DAR.EntityID AND  DAR.LineId = I.LineID      
--LEFT JOIN #TempDefaultAllocationRule GAR ON GAR.LineId = I.LineID AND GAR.EntityID = -1      
Where ISNULL(B.FootNoteID, 0) = -1 AND     
ISNULL(B.LineID, 0) <> -1      
      
DELETE I      
FROM #TempAllocationInput I       
INNER JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID       
AND ISNULL(B.LineID, 0) = ISNULL(I.LineID, 0)      
AND  I.LineTypeID = B.SourceID      
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END       
Where ISNULL(B.FootNoteID, 0) = -1 AND     
ISNULL(B.LineID, 0) <> -1      
      
      
DELETE FROM #TempBookEffectiveData WHERE ISNULL(FootNoteID, 0) = -1 AND     
ISNULL(LineID, 0) <> -1      
      
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID, QuickLinkID, TypeID, TrackingKey, Tag, LineTypeID, IsExcludefromTransfer)          
Select Distinct I.EntityID, I.LineID, QuicklinkID,       
ISNULL(B.AdjustmentAllocationTypeID,       
 CASE WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- LP - Offset' THEN @LPOffsetAllocationTypeID       
   WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- GP - Offset' THEN @GPOffsetAllocationTypeID       
   ELSE @CostAllocationTypeID END),       
ISNULL(B.TrackingKey, ISNULL(I.TrackingKey,'')), ISNULL(B.Tag,ISNULL(I.Tag, '')), I.LineTypeID  , ISNULL(B.IsExcludefromTransfer, 0)        
FROM #TempAllocationInput I          
LEFT JOIN PFICFootnoteLineItem P with(nolock) ON I.LineID = P.LineID AND I.LineTypeID = @PFICFootNoteLineTypeID      
INNER JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID         
AND  I.LineTypeID = B.SourceID       
And ISNULL(I.QuicklinkID, 0) = ISNULL(B.FootNoteID, 0) AND ISNULL(I.LineID,0) = ISNULL(B.LineID, 0)    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
                         CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END      
--LEFT JOIN #TempDefaultAllocationRule DAR ON I.EntityID = DAR.EntityID       
--LEFT JOIN #TempDefaultAllocationRule GAR ON GAR.LineId = I.LineID AND GAR.EntityID = -1      
Where ISNULL(B.FootNoteID, 0) = -1 AND     
ISNULL(B.LineID, 0) = -1      
      
      
DELETE I      
FROM #TempAllocationInput I          
INNER JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID         
AND  I.LineTypeID = B.SourceID       
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END       
Where ISNULL(B.FootNoteID, 0) = -1 AND     
ISNULL(B.LineID, 0) = -1      
      
      
DELETE FROM #TempBookEffectiveData WHERE ISNULL(FootNoteID, 0) = -1 AND     
ISNULL(LineID, 0) = -1      
      
      
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID, QuickLinkID, TypeID, TrackingKey, Tag, LineTypeID, IsExcludefromTransfer)       
Select Distinct I.EntityID, I.LineID, QuicklinkID,       
ISNULL(B.AdjustmentAllocationTypeID,      
 CASE WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- LP - Offset' THEN @LPOffsetAllocationTypeID       
   WHEN I.LineTypeID = @PFICFootNoteLineTypeID AND ISNULL(P.LineDescription, '') LIKE '%- GP - Offset' THEN @GPOffsetAllocationTypeID       
   ELSE @CostAllocationTypeID END),        
ISNULL(B.TrackingKey, ISNULL(I.TrackingKey,'')), ISNULL(B.Tag,ISNULL(I.Tag, '')), I.LineTypeID, ISNULL(B.IsExcludefromTransfer, 0)      
FROM #TempAllocationInput I         
LEFT JOIN PFICFootnoteLineItem P with(nolock) ON I.LineID = P.LineID AND I.LineTypeID = @PFICFootNoteLineTypeID      
LEFT JOIN #TempBookEffectiveData B ON I.EntityID = B.UnderlyingEntityID       
AND  I.LineTypeID = B.SourceID And ISNULL(I.QuicklinkID, 0) = ISNULL(B.FootNoteID, 0)      
AND ISNULL(I.LineID,0) = ISNULL(B.LineID, 0)       
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =       
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END       
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =       
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END       
--LEFT JOIN #TempDefaultAllocationRule DAR ON I.EntityID = DAR.EntityID AND  DAR.LineId = I.LineID      
--LEFT JOIN #TempDefaultAllocationRule GAR ON GAR.LineId = I.LineID AND GAR.EntityID = -1      
WHERE B.UnderlyingEntityID IS NULL      
      
--PBI 197535 - Footnote Quarter Allocations       
DECLARE @PFICQuarterLineID INT, @Form199AQuarterLineID INT, @Form8886QuarterLineID INT, @Form926QuarterLineID INT, @Form8865QuarterLineID INT ,@PFICDistributionDateLineID INT       
SELECT @PFICQuarterLineID = LineID FROM PFICFootnoteLineItem li with(nolock) WHERE li.ShortName = 'QuarterAllocations' and IsActive = 1      
SELECT @Form199AQuarterLineID = LineID FROM Form199ALineItem li with(nolock) WHERE li.ShortName = 'QuarterAllocations' and IsActive = 1      
SELECT @Form8886QuarterLineID = LineID FROM Form8886LineItem li with(nolock) WHERE li.ShortName = 'QuarterAllocations' and IsActive = 1      
SELECT @Form926QuarterLineID = LineID FROM Form926LineItem li with(nolock) WHERE li.ShortName = 'TransferDate' and IsActive = 1      
SELECT @Form8865QuarterLineID = LineID FROM Form8865LineItem li with(nolock) WHERE li.ShortName = 'TransferDate' and IsActive = 1     
SELECT @PFICDistributionDateLineID = LineID FROM PFICFootnoteLineItem li with(nolock) WHERE li.ShortName = 'DatesofDistribution' and IsActive = 1      

    
-----------------------------------------------------------------------------------------------------------------------------    
-- Fix for 331593 - Start

UPDATE T SET T.IsExcludefromTransfer=M.ExcludefromTransfers
FROM #TempInputLines T 
JOIN #MapDefaultAllocRuleToLineItem M on 
CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE T.LineID END = CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE  M.SelectedMappingID END
AND T.LineTypeID=M.SourceID and T.TypeID=M.RuleID 
WHERE M.Clientid=@LocalClientID and M.TaxperiodID=@LocalTaxPeriodID and @IsDARSetup=1

-- Fix for 331593 - End
----------------------------------------------------------------------------------------------------------------------------------

INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer, LineID)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), EDF.LookUpValue, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0), L.LineID      
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
INNER JOIN PFICFootnoteLineItem PL with(nolock) ON L.LineID = PL.LineID      
Inner Join PFICFootnotePackage P (NOLOCK)      
ON L.QuicklinkID = P.PFICFootnoteID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
INNER JOIN ENU_DF_DataList EDF ON EDF.Category = 'DatedFootnoteLines' AND PL.ShortName = EDF.LookUpData       
WHERE EL.LineType = 'PFIC Footnote'       
      
INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), PF.TextValue, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)      
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join PFICFootnotePackage P (NOLOCK)      
ON L.QuicklinkID = P.PFICFootnoteID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
Inner Join PFICFootnoteFlowup PF (NOLOCK) ON PF.PFICFootnoteID =P.PFICFootnoteID and PF.RunID = @LocalRunID       
and PF.LineID = @PFICQuarterLineID and ISNULL(PF.TextValue,'') != ''      
WHERE EL.LineType = 'PFIC Footnote'       
      
      
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join PFICFootnotePackage P (NOLOCK)      
ON L.QuicklinkID = P.PFICFootnoteID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
LEFT Join PFICFootnoteFlowup PF (NOLOCK) ON PF.PFICFootnoteID =P.PFICFootnoteID and PF.RunID = @LocalRunID       
and PF.LineID = @PFICQuarterLineID and ISNULL(PF.TextValue,'') != ''      
WHERE EL.LineType = 'PFIC Footnote' AND PF.PFICFootnoteID IS NULL      

IF(ISNULL(@PartVAllocated,0) = 1)
BEGIN    
    INSERT INTO #tmpPartVQuarters(PFICFootnoteID,QUARTER,TextValue)
	SELECT PF.PFICFootnoteID,ISNULL(D.QUARTER, 'Q0') QUARTER,PF.TextValue    
    FROM PFICFootnoteFlowup PF (NOLOCK) 
    INNER JOIN QuarterDates D (NOLOCK) ON Convert(date,ISNULL(PF.TextValue, '1900-01-01')) BETWEEN D.StartDate And D.EndDate   
    WHERE PF.RunID = @LocalRunID  
    and PF.LineID = @PFICDistributionDateLineID and ISNULL(PF.TextValue,'') != '' AND ISNULL(PF.TextValue,'') != 'Various' 
 

    INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer,Lineid,transferdate)      
    Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), ISNULL(D.QUARTER, 'Q0'), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)    
    ,L.LineID,D.TextValue  
    From #TempInputLines L       
    INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
    Inner Join PFICFootnotePackage P (NOLOCK)      
    ON L.QuicklinkID = P.PFICFootnoteID       
    Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
    Inner Join #tmpPartVQuarters D ON D.PFICFootnoteID = P.PFICFootnoteID
    WHERE EL.LineType = 'PFIC Footnote'	
END
---------------------------------------PBI 252140-----------------------------------------
IF(ISNULL(@IsPFICAllocationbyQuarter, 'U') = 'C') 
BEGIN
	SELECT DISTINCT UnderlyingEntityID, @PFICFootNoteLineTypeID LineTypeID,QuicklinkID as PFICFootnoteID,LineID,TypeID, TrackingKey,Tag, ISNULL(IsExcludefromTransfer,0)  IsExcludefromTransfer  
    INTO #TempInputLinesPFIC
	From #TempInputLines          
    WHERE LineTypeID=@PFICFootNoteLineTypeID

     --Populating Quarter based on distribution date
	INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
	Select Distinct L.UnderlyingEntityID, @PFICFootNoteLineTypeID, D.LookUpData, L.TypeID, L.TrackingKey, L.Tag, L.IsExcludefromTransfer     
	From #TempInputLinesPFIC L           
	INNER JOIN PFICFootnoteFlowup PF (NOLOCK)   ON PF.PFICFootnoteID =L.PFICFootnoteID and PF.RunID = @LocalRunID       
	AND PF.LineID = @PFICDistributionDateLineID and ISNULL(PF.TextValue,'Various') != 'Various'
	INNER JOIN ENU_DF_DataList D On D.LookUpValue = ISNULL(Month(PF.TextValue), 0) AND D.Category = 'QuarterMonth'    
    INNER JOIN PFICFOOTNOTELINEITEM PL with(nolock) ON L.LINEID=PL.LINEID
	LEFT JOIN #TempDatedEntities TD ON TD.UnderlyingEntityID=L.UnderlyingEntityID AND TD.LineTypeID=L.LineTypeID
	AND TD.Quarter=D.LookUpData AND TD.TYPEID=L.TYPEID AND TD.TrackingKey=L.TrackingKey AND TD.TAG=L.TAG
	AND L.IsExcludefromTransfer =TD.IsExcludefromTransfer 
	WHERE  TD.Quarter IS NULL AND  PL.DefaultAllocationRule='Distribution Date Allocation'

    --Populating Max Quarter Q4 for Max Quarter Allocation lines
	INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
	Select Distinct L.UnderlyingEntityID, @PFICFootNoteLineTypeID, 'Q4', L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)      
	From #TempInputLinesPFIC L
	INNER JOIN PFICFOOTNOTELINEITEM PL with(nolock) ON L.LINEID=PL.LINEID
	LEFT JOIN #TempDatedEntities TD ON TD.UnderlyingEntityID=L.UnderlyingEntityID AND TD.LineTypeID=L.LineTypeID
	AND TD.Quarter='Q4' AND TD.TYPEID=L.TYPEID AND TD.TrackingKey=L.TrackingKey AND TD.TAG=L.TAG
	AND L.IsExcludefromTransfer =TD.IsExcludefromTransfer 
	WHERE  PL.DefaultAllocationRule='Max Quarter Allocation' AND  TD.Quarter IS NULL

    --Populating Quarter Q0 for Default to Q0 lines
	INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID,  TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
	Select Distinct L.UnderlyingEntityID, @PFICFootNoteLineTypeID, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)      
	From #TempInputLinesPFIC L
	INNER JOIN PFICFOOTNOTELINEITEM PL with(nolock) ON L.LINEID=PL.LINEID
	LEFT JOIN #TempNonDatedEntities TD ON TD.UnderlyingEntityID=L.UnderlyingEntityID AND TD.LineTypeID=L.LineTypeID
	AND  TD.TYPEID=L.TYPEID AND TD.TrackingKey=L.TrackingKey AND TD.TAG=L.TAG
	AND L.IsExcludefromTransfer =TD.IsExcludefromTransfer 
	WHERE PL.DefaultAllocationRule='Q0 Allocation' AND TD.UnderlyingEntityID IS NULL

    DROP TABLE #TempInputLinesPFIC

END
-------------------------------------------------------------------------------------------    

IF (@AllocationTypeName = 'PE Book Allocation' AND @IsDatedTransfersConfigured = 'C')    
BEGIN   
	INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer,Lineid,transferdate, Preference)    
	Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), ISNULL(D.QUARTER, 'Q0'), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)  
	,L.LineID,PF.TextValue, D.Preference
	From #TempInputLines L     
	INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID    
	Inner Join Form926Package P (NOLOCK)     
	ON L.QuicklinkID = P.Form926ID     
	Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID    
	Inner Join Form926Flowup PF (NOLOCK) ON PF.Form926ID =P.Form926ID and PF.RunID = @LocalRunID     
	and PF.LineID = @Form926QuarterLineID and ISNULL(PF.TextValue,'') != 'Various'    
	INNER JOIN QuarterDates D (NOLOCK) ON ISNULL(PF.TextValue, '1900-01-01') BETWEEN D.StartDate And D.EndDate 
	WHERE EL.LineType = 'Form926'     
END
ELSE
BEGIN

INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer,LineID,transferdate)    
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), 'Q'+Convert(Varchar(5),ISNULL(DATEPART(qq, PF.TextValue), 0)), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)    
,L.LineID,PF.TextValue
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form926Package P (NOLOCK)       
ON L.QuicklinkID = P.Form926ID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
Inner Join Form926Flowup PF (NOLOCK) ON PF.Form926ID =P.Form926ID and PF.RunID = @LocalRunID       
and PF.LineID = @Form926QuarterLineID and ISNULL(PF.TextValue,'') != 'Various'      
WHERE EL.LineType = 'Form926'       

END     
    

INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form926Package P (NOLOCK)      
ON L.QuicklinkID = P.Form926ID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
LEFT Join Form926Flowup PF (NOLOCK) ON PF.Form926ID =P.Form926ID and PF.RunID = @LocalRunID       
and PF.LineID = @Form926QuarterLineID and ISNULL(PF.TextValue,'') != 'Various'      
WHERE EL.LineType = 'Form926' AND PF.Form926ID IS NULL      
    
INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), 'Q'+Convert(Varchar(5),ISNULL(DATEPART(qq, PF.TextValue), 0)), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form8865Package P (NOLOCK)       
ON L.QuicklinkID = P.Form8865ID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
Inner Join Form8865Flowup PF (NOLOCK) ON PF.Form8865ID =P.Form8865ID and PF.RunID = @LocalRunID       
and PF.LineID = @Form8865QuarterLineID and ISNULL(PF.TextValue,'') != 'Various'      
WHERE EL.LineType = 'Form8865'       
      
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form8865Package P (NOLOCK)      
ON L.QuicklinkID = P.Form8865ID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
LEFT Join Form8865Flowup PF (NOLOCK) ON PF.Form8865ID =P.Form8865ID and PF.RunID = @LocalRunID       
and PF.LineID = @Form8865QuarterLineID and ISNULL(PF.TextValue,'') != 'Various'      
WHERE EL.LineType = 'Form8865' AND PF.Form8865ID IS NULL     
      
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form1042SPackage P  (NOLOCK)      
ON L.QuicklinkID = P.Form1042SID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
WHERE EL.LineType = 'Form1042S'       
      
INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID,Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1),PF.TextValue, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form8886Package P (NOLOCK)       
ON L.QuicklinkID = P.Form8886ID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
Inner Join Form8886FlowUp  PF (NOLOCK) ON PF.Form8886ID =P.Form8886ID and PF.RunID = @LocalRunID       
and PF.LineID = @Form8886QuarterLineID and ISNULL(PF.TextValue,'') != ''      
WHERE EL.LineType = 'Form8886'      
      
      
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form8886Package P (NOLOCK)       
ON L.QuicklinkID = P.Form8886ID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
LEFT Join Form8886FlowUp PF (NOLOCK) ON PF.Form8886ID =P.Form8886ID and PF.RunID = @LocalRunID       
and PF.LineID = @Form8886QuarterLineID and ISNULL(PF.TextValue,'') != ''      
WHERE EL.LineType = 'Form8886' AND PF.Form8886ID IS NULL       
      
INSERT INTO #TempDatedEntities(UnderlyingEntityID, LineTypeID,Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1),PF.TextValue, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form199APackage P (NOLOCK)       
ON L.QuicklinkID = P.Form199AID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
Inner Join Form199AFlowUp PF (NOLOCK) ON PF.Form199AID =P.Form199AID and PF.RunID = @LocalRunID       
and PF.LineID = @Form199AQuarterLineID and ISNULL(PF.TextValue,'') != ''      
WHERE EL.LineType = 'Form199A'      
      
      
      
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)      
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)       
From #TempInputLines L       
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID      
Inner Join Form199APackage P (NOLOCK)        
ON L.QuicklinkID = P.Form199AID       
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID      
LEFT Join Form199AFlowUp PF (NOLOCK) ON PF.Form199AID =P.Form199AID and PF.RunID = @LocalRunID       
and PF.LineID = @Form199AQuarterLineID and ISNULL(PF.TextValue,'') != ''      
WHERE EL.LineType = 'Form199A' AND PF.Form199AID IS NULL       
    
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)    
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)     
From #TempInputLines L     
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID    
Inner Join AtRiskPackage P (NOLOCK) ON L.QuicklinkID = P.AtRiskID     
Inner Join K1Package K with(nolock) On P.K1PackageID = K.K1PackageID    
LEFT Join AtRiskFlowup PF (NOLOCK) ON PF.AtRiskID =P.AtRiskID and PF.RunID = @LocalRunID     
WHERE L.LineTypeID = @AtRiskLineTypeID    
  
--CustomFootnote  
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)    
Select Distinct K.LowerTierEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0)     
From #TempInputLines L     
INNER JOIN ENU_LineType EL ON L.LineTypeID = EL.LineTypeID   
INNER JOIN #tmpCustomFootnoteLineTypes CF ON CF.LineTypeID=EL.LineTypeID  
Inner Join CustomFootNotePackage P (NOLOCK) ON L.QuicklinkID = P.CustomFootnoteID     
Inner Join K1Package K (NOLOCK) On P.K1PackageID = K.K1PackageID    
LEFT Join CustomFootnoteFlowup PF (NOLOCK) ON PF.CustomFootnoteID =P.CustomFootnoteID and PF.RunID = @LocalRunID     
    
      
      
IF (@IsForm199AEffectivePercentageLogic = 1 AND @LocalIsPEModel = 0) 
BEGIN      
      
-- Effective Percentage calculation for 199A line type - Start      
      
  CREATE TABLE #tmp199ALine (      
  RuleNumber INT,      
     Form199ALineID INT      
  )      
      
  CREATE TABLE #tmpK1Line (      
  RuleNumber INT,      
     LineNumber VARCHAR(10)      
  )      
      
  CREATE TABLE #tmpExcludeLine (      
  RuleNumber INT,      
     ExcludeLineID INT      
  )      
      
  CREATE TABLE #tmpRuleConfig (      
  RuleNumber INT,      
  Lookupdata VARCHAR(200),      
     LookUpValue VARCHAR(200)      
  )      
      
  CREATE Table #taxableIncomeEffecivePerLines(UnderlyingEntityID INT,RuleNumber INT,Form199ALineID INT)      
      
  CREATE TABLE #RuleAmount (      
    EntityID INT,      
    Partnernumber VARCHAR(50),      
    Amount FLOAT,      
    LineTypeID INT,      
    TypeID INT,      
    TrackingKey VARCHAR(4000) ,      
    Tag VARCHAR(5000),      
    LineID INT,      
    RuleNumber INT,      
    IsK1LineAmount BIT,      
    Quarter VARCHAR(50)      
  )      
      
  INSERT INTO #tmpRuleConfig(RuleNumber,Lookupdata,LookUpValue)      
  SELECT       
  CASE WHEN Category ='Form199AEffectivePercentageRule1' THEN 1      
       WHEN D.Category='Form199AEffectivePercentageRule2' THEN 2      
    WHEN D.Category='Form199AEffectivePercentageRule3' THEN 3      
    WHEN D.Category='Form199AEffectivePercentageRule4' THEN 4      
    END,      
  Lookupdata,      
  LookUpValue      
  FROM ENU_DF_DataList D (NOLOCK)       
  WHERE Category IN('Form199AEffectivePercentageRule1','Form199AEffectivePercentageRule2','Form199AEffectivePercentageRule3','Form199AEffectivePercentageRule4')      
        
      
    
  INSERT INTO #tmp199ALine(RuleNumber,Form199ALineID)    
  SELECT     
  RuleNumber,    
  F.LineID    
  FROM #tmpRuleConfig D    
  INNER JOIN Form199ALineItem F (NOLOCK) On RTRIM(LTRIM(D.LookUpValue)) = RTRIM(LTRIM(F.LineDescription))    
  WHERE LookUpData = '199ALine'    
    
     
    
  INSERT INTO #tmpK1Line(RuleNumber,LineNumber)    
  SELECT     
  RuleNumber,    
  LookUpValue    
  FROM #tmpRuleConfig D    
  WHERE LookUpData = 'K1Line'    
    
      
  INSERT INTO #tmpExcludeLine (    
  RuleNumber ,    
     ExcludeLineID     
  )    
  SELECT     
  RuleNumber,    
  F.LineID    
  FROM #tmpRuleConfig D    
  INNER JOIN K1LineItem F with(nolock) On RTRIM(LTRIM(D.LookUpValue)) = RTRIM(LTRIM(F.LineDescription))    
  WHERE LookUpData = 'ExcludeLine'    
    
  DECLARE @199ALineType INT ;    
    
   Select @199ALineType = Linetypeid FROM ENU_LineType EL        
  WHERE ISNULL(EL.LineType, '') = 'Form199A'     
    
  SELECT    
    D.UnderlyingEntityID,    
    D.LineTypeID,    
    D.TypeID,    
    D.TrackingKey,    
    D.Tag INTO #tmp199AUnderlying    
  FROM #TempNonDatedEntities D    
  LEFT JOIN #TempFootnoteBookEffectiveData  B (NOLOCK)    
    ON D.UnderlyingEntityID = B.UnderlyingEntityID    
    AND D.LineTypeID = B.SourceID    
  LEFT JOIN #TempInputLines L    
    ON B.UnderlyingEntityID = L.UnderlyingEntityID AND L.LineID = B.LineID     
    AND B.SourceID = L.LineTypeID     
    --LEFT JOIN ENU_LineType EL    
  --  ON D.LineTypeID = EL.LineTypeID    
  WHERE D.LineTypeID = @199ALineType AND ISNULL(D.IsExcludefromTransfer, 0) = 0    
  AND B.LineID IS NULL    
    
  Select T.UnderlyingEntityID, T.LineTypeID,    
    T.TypeID,    
    T.TrackingKey,    
    T.Tag,    
    I.LineID,    
    A.RuleNumber INTO #RuleUnderlyings    
    FROM     
  #tmp199AUnderlying T       
  INNER JOIN #TempInputLines I    
    ON I.UnderlyingEntityID = T.UnderlyingEntityID    
  INNER JOIN #tmp199ALine A    
  ON A.Form199ALineID = I.LineID     
    
 SELECT    
   [RunID],    
   [ClientID],    
   [EntityID],    
   [ShareClass],    
   [PartnerNumber],    
   [LineTypeID],    
   [LineID],    
   [Amount],    
   [AllocationType],    
   [QuicklinkID],    
   [Amount704b],    
   [CategoryID],    
   [ParentEntityID],    
   [PeriodID],    
   [LineCode],    
   [SuperParentEntityID],    
   [AdjustmentTypeID],    
   [TrackingKey],    
   [Tag] INTO #tmpLookThroughAllocationOutput    
 FROM LookThroughAllocationOutput(NOLOCK)    
 WHERE RUNID = @LocalRunID AND LineTypeID = @K1LineTypeID    
      
      
  -- Rule 1 Start-----------------    
    
  INSERT INTO #RuleAmount(EntityID,    
    Partnernumber,    
    Amount,    
    LineTypeID,    
    TypeID ,    
    TrackingKey  ,    
    Tag ,    
    LineID,    
    RuleNumber,    
    IsK1LineAmount)    
    SELECT DISTINCT    
    Entityid,    
    Partnernumber,    
    SUM(Amount),    
    T.LineTypeID,    
    T.TypeID,    
    T.TrackingKey,    
    T.Tag,    
    T.LineID,    
    1,    
    1    
  FROM #tmpLookThroughAllocationOutput L (NOLOCK)    
  INNER JOIN #RuleUnderlyings T     
  ON T.UnderlyingEntityID = L.Entityid AND T.RuleNumber = 1    
  INNER JOIN K1LineItem K with(nolock)   
    ON K.LineID = L.LineID    
  INNER JOIN #tmpK1Line KL    
    ON K.LineNumber = KL.LineNumber    
    AND KL.RuleNumber = T.RuleNumber    
  LEFT JOIN #tmpExcludeLine E    
    ON K.LineID = E.ExcludeLineID    
    AND E.RuleNumber = 1    
  WHERE E.RuleNumber IS NULL    
  GROUP By Entityid,    
           Partnernumber,    
      T.LineTypeID,    
      T.TypeID,    
      T.TrackingKey,    
      T.Tag,    
      T.LineID    
    
 ----------------------------------------------    
     
      
      
    
    
  ----------------------Rule 2 Start-------------------    
  
  SELECT * INTO #TempQuarterMonth FROM ENU_DF_DataList WHERE Category = 'QuarterMonth'
  
  INSERT INTO #RuleAmount(EntityID,    
    Partnernumber,    
    Amount,    
    LineTypeID,    
    TypeID ,    
    TrackingKey  ,    
    Tag ,    
    LineID,    
    RuleNumber,    
    IsK1LineAmount)    
    SELECT DISTINCT    
    Entityid,    
    Partnernumber,    
    SUM(Amount),    
    T.LineTypeID,    
    T.TypeID,    
    T.TrackingKey,    
    T.Tag,    
    T.LineID,    
    2,    
    1    
  FROM #tmpLookThroughAllocationOutput L    
  INNER JOIN #RuleUnderlyings T     
  ON T.UnderlyingEntityID = L.Entityid AND T.RuleNumber = 2    
  INNER JOIN K1LineItem K with(nolock)   
    ON K.LineID = L.LineID   
  INNER JOIN #TempQuarterMonth D    
    ON D.LookUpValue = ISNULL(MONTH(K.TransactionDate), 0)   
  INNER JOIN #tmpK1Line KL    
    ON K.LineNumber = KL.LineNumber    
    AND KL.RuleNumber = T.RuleNumber    
  LEFT JOIN #tmpExcludeLine E    
    ON K.LineID = E.ExcludeLineID    
      AND E.RuleNumber = 2    
  WHERE D.LookUpData = 'Q4'    
  AND E.RuleNumber IS NULL    
  GROUP By Entityid,    
           Partnernumber,    
      T.LineTypeID,    
      T.TypeID,    
      T.TrackingKey,    
      T.Tag,    
      T.LineID    
    
  SELECT    
   DealId,    
   MAX(T.Quarter) AS MaxQuarter,       
   T.TypeId,    
   T.TrackingKey,    
   T.Tag INTO #MaxPercentage    
    FROM #TempCostPercentage T    
    INNER JOIN #RuleUnderlyings U     
    ON T.DealId = U.UnderlyingEntityID     
     AND U.RuleNumber = 2    
    LEFT JOIN #RuleAmount R    
    ON T.DealId = R.EntityID    
     AND R.LineID = U.LineID    
    WHERE R.EntityID IS NULL AND T.TypeId=@CostAllocationTypeID    
    GROUP BY DealId,    
       T.TypeId,    
       T.TrackingKey,    
       T.Tag    
    
     
    
      
    
    INSERT INTO #Temp199ACostPercentage (RuleNumber, DealId, Partnernumber, Quarter, CommitmentPercent, TypeId, TrackingKey, Tag)    
    SELECT    
   2,    
   T.DealId,    
   T.Partnernumber,    
   M.MaxQuarter,    
   CommitmentPercent,    
   T.TypeId,    
   T.TrackingKey,    
   T.Tag    
    FROM #TempCostPercentage T    
   INNER JOIN #MaxPercentage M     
              ON M.DealId = T.DealId    
     AND M.MaxQuarter = T.Quarter    
     AND ISNULL(M.TypeId,0) = ISNULL(T.TypeId ,0)    
     AND ISNULL(M.TrackingKey,'') = ISNULL(T.TrackingKey ,'')    
     AND ISNULL(M.Tag,'') = ISNULL(T.Tag ,'')    
       
    
  INSERT INTO #TempFinalEffectivePercentageNonDated (InvestmentID, LineTypeID, TypeId, PartnerNumber, EffPercentage, AllocationType, TrackingKey, Tag, Quarter,Lineid, IsExcludefromTransfer)    
  SELECT DISTINCT    
    DealId,    
    T.LineTypeID,    
    T.TypeID,    
    Partnernumber,    
    CommitmentPercent,    
    'TI',    
    T.TrackingKey,    
    T.Tag,    
    'QO',    
    A.Form199ALineID,    
    0    
  FROM #Temp199ACostPercentage L 
  INNER JOIN #tmp199AUnderlying T    
    ON T.UnderlyingEntityID = L.DealId    
    and L.TypeId=T.TypeID    
  INNER JOIN #tmp199ALine A    
    ON A.RuleNumber = 2    
  WHERE L.RuleNumber =2    
       
      
      
    
  -------------------Rule 2 END-----------------------------    
    
      
    
-------------------Rule 4 Start-------------------    
     INSERT INTO #RuleAmount (EntityID,    
   Partnernumber,    
   Amount,    
   LineTypeID,    
   TypeID,    
   TrackingKey,    
   Tag,    
   LineID,    
   RuleNumber,    
   IsK1LineAmount)    
     SELECT DISTINCT    
    Entityid,    
    Partnernumber,    
    SUM(Amount),    
    T.LineTypeID,    
    T.TypeID,    
    T.TrackingKey,    
    T.Tag,    
    T.LineID,    
    4,    
    1    
     FROM #tmpLookThroughAllocationOutput L     
     INNER JOIN #RuleUnderlyings T ON T.UnderlyingEntityID = L.Entityid AND T.RuleNumber = 4    
     INNER JOIN K1LineItem K with(nolock)   
    ON K.LineID = L.LineID    
     INNER JOIN #tmpK1Line KL    
    ON K.LineNumber = KL.LineNumber    
    AND KL.RuleNumber = T.RuleNumber        
           GROUP By Entityid,    
           Partnernumber,    
      T.LineTypeID,    
      T.TypeID,    
      T.TrackingKey,    
      T.Tag,    
      T.LineID     
      
    
  -------------------Rule 4 END-----------------------------    
    
    
   --Effective Percentage calculation    
    
    SELECT    
    SUM(Amount) AS TotalAmount,    
    Entityid,    
    LineID,    
    RuleNumber INTO #TotalAmount    
  FROM #RuleAmount    
  WHERE IsK1LineAmount = 1    
  GROUP BY Entityid,    
     LineID,    
     RuleNumber    
     
  INSERT INTO #TempFinalEffectivePercentageNonDated (InvestmentID, LineTypeID, TypeId, PartnerNumber, EffPercentage, AllocationType, TrackingKey, Tag, Quarter,Lineid, IsExcludefromTransfer)    
   SELECT    
   I.Entityid,    
   LineTypeID,    
   TypeID,    
   PartnerNumber,    
   Amount / A.TotalAmount,    
   'TI',    
   TrackingKey,    
   Tag,    
   'Q0',    
   I.LineID,    
   0    
    FROM #RuleAmount I    
    INNER JOIN #TotalAmount A On I.EntityID = A.EntityID    
    AND I.LineID = A.LineID      
    WHERE TotalAmount <> 0    
    ORDER By LineID    
    
 DELETE FROM #RuleAmount    
  --------------------------    
    
  ----------------------Rule 3 Start-------------------    
      
   INSERT INTO #taxableIncomeEffecivePerLines (UnderlyingEntityID,RuleNumber, Form199ALineID)    
    SELECT    
      R.UnderlyingEntityID,    
   R.RuleNumber,    
   R.LineID    
    FROM #RuleUnderlyings R    
    LEFT JOIN #TempFinalEffectivePercentageNonDated A    
   ON R.UnderlyingEntityID = A.InvestmentID    
   AND R.LineID = A.LineID    
   AND R.TypeID = A.TypeID    
   AND R.TrackingKey = A.TrackingKey    
     WHERE A.InvestmentID IS NULL    
      
   INSERT INTO #taxableIncomeEffecivePerLines (UnderlyingEntityID, RuleNumber, Form199ALineID)    
      SELECT    
   T.UnderlyingEntityID,    
   3,    
   T.LineID    
    FROM #TempInputLines T    
    LEFT JOIN #tmp199ALine L    
   ON L.Form199ALineID = T.LineID    
         LEFT JOIN #TempFootnoteBookEffectiveData  B (NOLOCK)    
    ON T.UnderlyingEntityID = B.UnderlyingEntityID    
    AND T.TypeID = B.AdjustmentAllocationTypeID AND B.Sourceid=T.LineTypeID      
    WHERE T.LineTypeID = @199ALineType AND L.RuleNumber IS NULL AND B.UnderlyingEntityID IS NULL   
    
    
    INSERT INTO #RuleAmount (EntityID,    
   Partnernumber,    
   Amount,    
   LineTypeID,    
   TypeID,    
   TrackingKey,    
   Tag,    
   LineID,    
   RuleNumber,    
   IsK1LineAmount)    
     SELECT    
    Entityid,    
    Partnernumber,    
    SUM(TaxableIncome) AS Amount,    
    T.LineTypeID,    
    T.TypeID,    
    T.TrackingKey,    
    T.Tag,    
    T.LineID,    
    3,    
    1    
     FROM LookThroughTaxableIncome L (NOLOCK)      
    INNER JOIN #taxableIncomeEffecivePerLines A    
    ON         
    A.UnderlyingEntityID = L.Entityid    
    INNER JOIN #TempInputLines T    
    ON T.UnderlyingEntityID = A.UnderlyingEntityID    
    AND A.Form199ALineID = T.LineID    
    INNER JOIN ENU_LineType LT     
    ON T.LineTypeID = LT.LineTypeID    
    AND LT.LineType = 'Form199A'    
     WHERE RUNID = @LocalRunID    
     GROUP BY Entityid,    
        Partnernumber,    
        T.LineTypeID,    
        T.TypeID,    
        T.TrackingKey,    
        T.Tag,    
        T.LineID    
    
-------------------Rule 3 END-----------------------------    
--Get Taxable Income Total    
SELECT    
    SUM(Amount) AS TotalAmount,    
    Entityid,    
    LineID,    
    RuleNumber INTO #TITotalAmount    
  FROM #RuleAmount    
  WHERE IsK1LineAmount = 1    
  GROUP BY Entityid,    
     LineID,    
     RuleNumber    
     
  INSERT INTO #TempFinalEffectivePercentageNonDated (InvestmentID, LineTypeID, TypeId, PartnerNumber, EffPercentage, AllocationType, TrackingKey, Tag, Quarter,Lineid, IsExcludefromTransfer)    
   SELECT    
   I.Entityid,    
   LineTypeID,    
   TypeID,    
   PartnerNumber,    
   Amount / A.TotalAmount,    
   'TI',    
   TrackingKey,    
   Tag,    
   'Q0',    
   I.LineID,    
   0    
    FROM #RuleAmount I    
    INNER JOIN #TITotalAmount A On I.EntityID = A.EntityID    
    AND I.LineID = A.LineID    
    WHERE A.TotalAmount <> 0    
    ORDER By LineID    
     
       
   DELETE D    
     FROM #TempNonDatedEntities D    
     INNER JOIN #tmp199AUnderlying F    
    ON D.UnderlyingEntityID = F.UnderlyingEntityID    
    AND D.TypeID = F.TypeID    
    AND D.TrackingKey = F.TrackingKey    
    AND D.Tag = F.Tag    
    AND ISNULL(D.LineTypeID, -1) = ISNULL(F.LineTypeID, -1)    
     INNER JOIN #TempFinalEffectivePercentageNonDated T    
    ON D.UnderlyingEntityID = T.InvestmentID    
    AND D.TypeID = T.TypeID    
    AND D.TrackingKey = T.TrackingKey    
    AND D.Tag = T.Tag    
    AND ISNULL(D.LineTypeID, -1) = ISNULL(T.LineTypeID, -1)    
   WHERE ISNULL(D.IsExcludefromTransfer, 0) = 0    
      
   DROP TABLE #taxableIncomeEffecivePerLines    
   DROP TABLE #TotalAmount    
   DROP TABLE #tmp199AUnderlying    
   DROP TABLE #RuleAmount    
   DROP TABLE #tmp199ALine    
         DROP TABLE #tmpK1Line    
         DROP TABLE #tmpExcludeLine    
         DROP TABLE #tmpRuleConfig    
   DROP TABLE #RuleUnderlyings    
   DROP TABLE #TITotalAmount    
   DROP TABLE #tmpLookThroughAllocationOutput     
   DROP TABLE #MaxPercentage     
 -- Effective Percentage calculation for 199A line type - End    
    
 END    
  Drop table #TempFootnoteBookEffectiveData    
END    
    
IF(@LocalMode = 3)    
BEGIN    
    
SELECT @SM_CustomAllocationEventTypeID = EventTypeID    
FROM ENU_Event     
WHERE EventName = 'Import_StateAllocationRule'    
    
SET @SM_CustomAllocationWorkFlowID =      
ISNULL(dbo.udfGetApprovedWorkflow(@LocalClientID, @LocalTaxPeriodID,@SM_CustomAllocationEventTypeID,@LocalEntityID),0)    
    
INSERT INTO #SM_TempBookEffective(UnderlyingEntityID, StateLineID, StateID, AllocationTypeid, AdjustmentAllocationTypeID, TrackingKey, Tag)    
SELECT UnderlyingEntityID, StateLineID, StateID, AllocationTypeid, AdjustmentAllocationTypeID, TrackingKey, Tag     
FROM SM_StateLineAllocationRule_Snapshot (NOLOCK)    
WHERE WorkflowID = @SM_CustomAllocationWorkFlowID    
AND ClientID = @LocalClientID AND TaxPeriodID = @LocalTaxPeriodID    
    
Update #SM_TempBookEffective     
SET AdjustmentAllocationTypeID = AllocationTypeid    
WHERE AdjustmentAllocationTypeID IN (@BookAllocationTypeID, @OffsetAllocationTypeID)
    
SELECT @K1LineTypeID AS SourceID, StateID, SelectedMappingID, RuleID,ExcludeFromTransfers INTO #StatesMapDefaultAllocRuleToLineItem    
FROM #MapDefaultAllocRuleToLineItem M JOIN ENU_LineType EL ON EL.LineTypeID = M.SourceID    
WHERE M.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)     
AND EL.LineType = 'State Input'    
  
  
IF(ISNULL(@IsCustomAllocationRuleEnabled, 'U') = 'C')  
BEGIN  
  
 INSERT INTO #TempAllUnderlyingsStatesOrdered(Underlyingtype, UnderlyingEntityId,EntityId,TrackingKey,TrackingMatch, AllocationTypeId,StateLineID,RankForUnderlyingPickup, LineTypeID , AllocationBy ,StateID,IsExcludefromTransfer)  
 SELECT  AI.Underlyingtype, AI.UnderlyingEntityId, AI.EntityId, L.TrackingKey,AI.TrackingKey TrackingMatch, AI.AllocationTypeId,L.StateLineID , ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId
 , L.TrackingKey, L.StateLineID,L.StateID, AI.AllocationTypeId  ORDER BY hlevel  
 ,U.DisplayOrder, AI.TrackingKey) RankForUnderlyingPickup     
 ,L.LineTypeID , 'PERCENT', L.StateID ,0    
 FROM #tempunderlyingMod AI   
    JOIN ENU_UnderlyingType U ON AI.Underlyingtype=U.UnderlyingTypeID  
    JOIN #TempSMLookThroughAllocationInput L ON L.EntityID = AI.UnderlyingEntityId  
    AND  
    CASE WHEN AI.UnderlyingEntityId = @LocalEntityID  
    OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class')  
    OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class')  
    THEN '-1' ELSE '~' + L.TrackingKey + '~' END    
  LIKE    
  CASE WHEN AI.UnderlyingEntityId = @LocalEntityID  
     OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class')  
     OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class')  
     THEN '-1' ELSE '%'+Ai.TrackingKey+'%'END    
   
  
END  
ELSE  
BEGIN  
INSERT INTO #TempAllUnderlyingsStatesOrdered(Underlyingtype, UnderlyingEntityId,EntityId,TrackingKey,TrackingMatch, AllocationTypeId,StateLineID,RankForUnderlyingPickup, LineTypeID , AllocationBy ,StateID,IsExcludefromTransfer)  
SELECT  AI.Underlyingtype, AI.UnderlyingEntityId, AI.EntityId, L.TrackingKey,AI.TrackingKey TrackingMatch, AI.AllocationTypeId,L.StateLineID , ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId, L.TrackingKey, L.StateLineID,L.StateID, EA.DisplayOrder  ORDER BY hlevel,  
 R.DisplayOrder DESC, U.DisplayOrder,M.SelectedMappingID DESC, EA.DisplayOrder, AI.TrackingKey) RankForUnderlyingPickup     
,L.LineTypeID , EA.AllocationBy, L.StateID,M.ExcludeFromTransfers    
FROM #tempunderlyingMod AI   
JOIN ENU_UnderlyingType U ON AI.Underlyingtype=U.UnderlyingTypeID    
JOIN #TempSMLookThroughAllocationInput L ON L.EntityID = AI.UnderlyingEntityId    
AND      
 CASE WHEN AI.UnderlyingEntityId = @LocalEntityID  
 OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class')   
 OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class')   
 THEN '-1' ELSE '~' + L.TrackingKey + '~' END   
 LIKE      
 CASE WHEN AI.UnderlyingEntityId = @LocalEntityID   
 OR (AI.EntityId = @LocalEntityID AND U.Underlyingtype <> 'Asset Class')   
 OR (@OverrideIndirectLookthroughAssetClass <> 'C' AND U.UnderlyingType = 'Asset Class')  
 THEN '-1' ELSE '%'+Ai.TrackingKey+'%'END   
JOIN #StatesMapDefaultAllocRuleToLineItem M     
ON CASE WHEN M.StateID = -1 THEN 1 ELSE M.StateID END = CASE WHEN M.StateID = -1 THEN 1 ELSE L.StateID END    
AND CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE L.StateLineID END =CASE WHEN M.SelectedMappingID =-1 THEN 1 ELSE  M.SelectedMappingID END  AND M.RuleID=AI.AllocationTypeId     
AND M.SourceID =  L.LineTypeID     
JOIN #DefaultAllocationRuleSetup D ON D.RuleID=AI.AllocationTypeId AND AI.Underlyingtype=D.UnderlyingTypeID    
JOIN ENU_RuleType R (NOLOCK) on D.RuleTypeID=R.RuleTypeID     
JOIN ENU_AllocationBy EA ON D.AllocationByID=EA.AllocationByID    
WHERE D.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)     
END  
  
INSERT INTO #TempAllUnderlyings (Underlyingtype,UnderlyingEntityId ,EntityId ,TrackingKey ,TrackingMatch ,AllocationTypeId ,LineID ,RankForUnderlyingPickup, LineTypeID, AllocationBy,StateID,IsExcludefromTransfer)     
SELECT Underlyingtype,UnderlyingEntityId,EntityId,TrackingKey, TrackingMatch ,AllocationTypeId ,StateLineID ,RankForUnderlyingPickup , LineTypeID, AllocationBy,StateID ,IsExcludefromTransfer   
FROM #TempAllUnderlyingsStatesOrdered WHERE RankForUnderlyingPickup = 1    
    
/* Calculate Effective Amounts to populate Allocated Amounts when allocation method is ByAmount */    
    
Select E.UnderlyingEntityId, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0) CommitmentPercent, ISNULL(C.AllocationTypeId, @CostAllocationTypeID) AllocationTypeId,    
ISNULL(AI.TrackingKey, '') TrackingKey, ISNULL(AI.Tag, '') Tag,     
AI.StateLineId LineID, ISNULL(AI.Amount ,0) InputAmount, ISNULL(C.AllocatedAmount, 0) AllocatedAmount, C.InvestmentID CostEntityId,  ISNULL(C.UnderlyingType, @EntityUnderlyingtype) UnderlyingTypeId, AI.LineTypeID LineTypeID 
,M.ExcludeFromTransfers  
INTO #SMEntityTotalAmounts    
FROM ENU_UnderlyingType U INNER JOIN #TempAllUnderlyings E ON U.UnderlyingTypeid = E.UnderlyingType     
INNER JOIN #CostPercentage_Snapshot C ON  E.EntityID = C.InvestmentID AND C.UnderlyingType = E.UnderlyingType     
AND E.AllocationTypeId = C.AllocationTypeId     
AND '~' +CASE WHEN ISNULL(C.TrackingKey,'') = '' THEN CONVERT(VARCHAR(4000), C.InvestmentID) ELSE C.TrackingKey END  +'~' =E.TrackingMatch    
INNER JOIN #TempSMLookThroughAllocationInput AI ON E.UnderlyingEntityID = AI.EntityID AND E.LineTypeID = AI.LineTypeID    
AND AI.StateID = E.StateID AND AI.StateLineID= E.LineID    
JOIN #StatesMapDefaultAllocRuleToLineItem M ON C.AllocationTypeId = M.RuleID     
AND CASE WHEN M.StateID = -1 THEN 1 ELSE M.StateID END = CASE WHEN M.StateID = -1 THEN 1 ELSE AI.StateID END    
AND CASE WHEN M.SelectedMappingID = -1 THEN 1 ELSE M.SelectedMappingID END = CASE WHEN M.SelectedMappingID = -1 THEN 1 ELSE AI.StateLineID END   
AND E.IsExcludefromTransfer=M.ExcludeFromTransfers
WHERE AI.LineTypeID=M.SourceID AND ISNULL(C.AllocatedAmount ,0) <> 0    
AND E.AllocationBy = 'AMOUNT'    
    
    
INSERT INTO #TotalUnderlyingAmounts(LineID, Partnernumber, TotalAmount, CostEntityId,AllocationTypeId,TrackingKey,  Tag, LineTypeID)    
SELECT LineID, Partnernumber, SUM(ISNULL(InputAmount,0)) AS TotalAmount , CostEntityId,AllocationTypeId,TrackingKey,  Tag, LineTypeID    
FROM #SMEntityTotalAmounts     
GROUP BY LineID, Partnernumber, CostEntityId,AllocationTypeId,TrackingKey,  Tag, LineTypeID   
    
INSERT INTO #FinalEffectiveAmounts(UnderlyingEntityID,LineID,Partnernumber, Quarter,TypeId,TrackingKey, Tag, EffectiveAmount ,UnderlyingTypeId, LineTypeID,IsExcludefromTransfer)    
SELECT C.UnderlyingEntityID,C.LineId, C.PartnerNumber , C.Quarter, C.AllocationTypeId, C.TrackingKey, C.Tag,     
CASE  WHEN T.TotalAmount <> 0 THEN (C.InputAmount/T.TotalAmount) * C.AllocatedAmount ELSE 0 END AS EffectiveAmount ,UnderlyingTypeId , C.LineTypeID,C.ExcludeFromTransfers   
From #TotalUnderlyingAmounts T JOIN #SMEntityTotalAmounts C ON C.CostEntityId = T.CostEntityId     
AND C.AllocationTypeId = T.AllocationTypeId     
AND C.TrackingKey = T.TrackingKey     
AND C.Tag = T.Tag     
AND C.LineID = T.LineID AND C.PartnerNumber = T.Partnernumber  AND T.LineTypeID = C.LineTypeID             
    
INSERT INTO #FinalAmounts(InvestmentID,Partnernumber, AllocationType,Quarter, TypeId,TrackingKey, Tag, LineId, EffectiveAmount, UnderlyingTypeId, LineTypeId,IsExcludefromTransfer)    
SELECT UnderlyingEntityID,  PartnerNumber , CASE WHEN IsExcludefromTransfer = 1 THEN 'Cost without Transfer Adj %' ELSE 'Cost' END, Quarter, TypeId, TrackingKey, Tag, LineId, EffectiveAmount, UnderlyingTypeId, LineTypeId,IsExcludefromTransfer    
FROM #FinalEffectiveAmounts      
    
    
    
---------------------------------------------Get State ALLOCATION Input Data---------------------------------------------------    
---Both state and line mentioned    
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID,StateId, TypeID, TrackingKey, Tag)     
Select Distinct I.EntityID, I.StateLineid, I.StateId, ISNULL(B.AdjustmentAllocationTypeID, @CostAllocationTypeID), ISNULL(B.TrackingKey, ''), ISNULL(I.Tag, '')    
FROM #TempSMLookThroughAllocationInput I    
Inner JOIN  SM_StateLines K (NOLOCK) ON I.StateID = K.StateID AND I.StateLineID = K.StateFieldID    
INNER JOIN #SM_TempBookEffective B ON I.EntityID = B.UnderlyingEntityID AND I.StateLineID = B.StateLineID     
AND B.StateID = I.StateID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
Where  ISNULL(B.StateLineID, -1) <> -1 AND ISNULL(B.StateID, -1) <> -1    
    
DELETE I    
FROM #TempSMLookThroughAllocationInput I     
Inner JOIN  SM_StateLines K (NOLOCK) ON I.StateID = K.StateID AND I.StateLineID = K.StateFieldID    
INNER JOIN #SM_TempBookEffective B ON I.EntityID = B.UnderlyingEntityID AND I.StateLineID = B.StateLineID     
AND B.StateID = I.StateID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
Where  ISNULL(B.StateLineID, -1) <> -1 AND ISNULL(B.StateID, -1) <> -1    
    
DELETE FROM #SM_TempBookEffective WHERE ISNULL(StateLineID, -1) <> -1 AND ISNULL(StateID, -1) <> -1    
    
---One state and all line mentioned    
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID,StateId, TypeID, TrackingKey, Tag,IsExcludefromTransfer)     
Select Distinct I.EntityID, I.StateLineid, I.StateId, ISNULL(B.AdjustmentAllocationTypeID, @CostAllocationTypeID), ISNULL(B.TrackingKey, ''), ISNULL(I.Tag, ''),0  
FROM #TempSMLookThroughAllocationInput I       
Inner JOIN  SM_StateLines K (NOLOCK) ON I.StateID = K.StateID     
INNER JOIN #SM_TempBookEffective B ON I.EntityID = B.UnderlyingEntityID     
AND B.StateID = I.StateID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
Where ISNULL(B.StateLineID, -1) = -1 AND ISNULL(B.StateID, -1) <> -1     
    
DELETE I    
FROM #TempSMLookThroughAllocationInput I      
Inner JOIN  SM_StateLines K (NOLOCK) ON I.StateID = K.StateID     
INNER JOIN #SM_TempBookEffective B ON I.EntityID = B.UnderlyingEntityID     
AND B.StateID = I.StateID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
Where ISNULL(B.StateLineID, -1) = -1 AND ISNULL(B.StateID, -1) <> -1     
    
DELETE FROM #SM_TempBookEffective WHERE ISNULL(StateLineID, -1) = -1 AND ISNULL(StateID, -1) <> -1    
    
---All state and One line mentioned    
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID,StateId, TypeID, TrackingKey, Tag,IsExcludefromTransfer)     
Select Distinct I.EntityID, I.StateLineid, I.StateId, ISNULL(B.AdjustmentAllocationTypeID, @CostAllocationTypeID), ISNULL(B.TrackingKey, ''), ISNULL(I.Tag, ''),0    
FROM #TempSMLookThroughAllocationInput I       
Inner JOIN  SM_StateLines K (NOLOCK) ON I.StateID = K.StateID     
INNER JOIN #SM_TempBookEffective B ON I.EntityID = B.UnderlyingEntityID     
AND B.StateLineID = I.StateLineID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
Where ISNULL(B.StateLineID, -1) <> -1 AND ISNULL(B.StateID, -1) = -1       
    
DELETE I    
FROM #TempSMLookThroughAllocationInput I       
Inner JOIN  SM_StateLines K (NOLOCK) ON I.StateID = K.StateID     
INNER JOIN #SM_TempBookEffective B ON I.EntityID = B.UnderlyingEntityID     
AND B.StateLineID = I.StateLineID    
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
Where ISNULL(B.StateLineID, -1) <> -1 AND ISNULL(B.StateID, -1) = -1     
    
DELETE FROM #SM_TempBookEffective WHERE ISNULL(StateLineID, -1) <> -1 AND ISNULL(StateID, -1) = -1     
    
    
INSERT INTO #TempInputLines(UnderlyingEntityID, LineID,StateId, TypeID, TrackingKey, Tag,IsExcludefromTransfer)     
Select Distinct I.EntityID, I.StateLineid, I.StateId, ISNULL(B.AdjustmentAllocationTypeID, ISNULL(AI.AllocationTypeId, @CostAllocationTypeID)), ISNULL(B.TrackingKey, ISNULL(I.TrackingKey, '')), ISNULL(I.Tag, '')
,ISNULL(AI.IsExcludefromTransfer,0)
FROM #TempSMLookThroughAllocationInput I       
Inner JOIN  SM_StateLines K (NOLOCK) ON I.StateID = K.StateID AND I.StateLineID = K.StateFieldID    
LEFT JOIN #SM_TempBookEffective B ON I.EntityID = B.UnderlyingEntityID     
AND CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE B.TrackingKey  END =     
    CASE WHEN ISNULL(B.TrackingKey, '') = '' THEN '-1' ELSE I.TrackingKey  END     
AND CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE B.Tag  END =     
    CASE WHEN ISNULL(B.Tag, '') = '' THEN '-1' ELSE I.Tag  END     
LEFT JOIN #TempAllUnderlyings AI ON I.EntityID = AI.UnderlyingEntityId AND I.StateID = AI.StateID AND I.StateLineid = AI.LineID    
AND AI.AllocationBy = 'PERCENT'    
    
DROP TABLE #StatesMapDefaultAllocRuleToLineItem    
DROP TABLE #SMEntityTotalAmounts    
  
-----------------------------------------------------------------------------------------------------------------------------    
    
----------------------------------------------Non Dated Entities---------------------------------------------------------------    
INSERT INTO #TempNonDatedEntities(UnderlyingEntityID, TypeID, TrackingKey, Tag, IsExcludefromTransfer,StateID)    
SELECT DISTINCT L.UnderlyingEntityID, TypeID, TrackingKey, Tag, ISNULL(L.IsExcludefromTransfer,0) ,K.StateID    
FROM    
#TempInputLines L      
INNER JOIN SM_StateLines K (NOLOCK) ON K.StateFieldId = L.LineID  and L.StateID=K.StateID AND TransactionDate is null    
    
-------------------------------------------------DATED LINES----------------------------------------------------------------    
IF (@AllocationTypeName = 'PE Book Allocation' AND @IsDatedTransfersConfigured = 'C')    
BEGIN    
INSERT INTO #TempDatedEntities(Quarter, UnderlyingEntityID, TypeID, TrackingKey, Tag, IsExcludefromTransfer, LineID, Preference)    
SELECT Distinct ISNULL(D.QUARTER, 'Q0') , L.UnderlyingEntityID EntityID, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0), L.LineID, D.Preference    
FROM    
#TempInputLines L      
INNER JOIN SM_StateLines K (NOLOCK) ON K.StateFieldId = L.LineID and L.StateID=K.StateID    
INNER Join Quarterdates D (NOLOCK) On ISNULL(K.TransactionDate,'1900-01-01') BETWEEN D.StartDate and D.EndDate    
WHERE K.TransactionDate IS NOT NULL    
END    
ELSE    
BEGIN    
INSERT INTO #TempDatedEntities(Quarter, UnderlyingEntityID, TypeID, TrackingKey, Tag, IsExcludefromTransfer, LineID)    
SELECT Distinct D.LookUpData QUARTER, L.UnderlyingEntityID EntityID, L.TypeID, L.TrackingKey, L.Tag, ISNULL(L.IsExcludefromTransfer,0), L.LineID    
FROM    
#TempInputLines L      
INNER JOIN SM_StateLines K (NOLOCK) ON K.StateFieldId = L.LineID and L.StateID=K.StateID    
Inner Join ENU_DF_DataList D On D.LookUpValue = ISNULL(Month(K.TransactionDate),-1) AND D.Category = 'QuarterMonth'    
END    
END    
    
    
------------------------------------------Percentage Load----------------------------------------------------------------    
    
    
/***********get Underlyings****************/    
    
IF (@OverrideIndirectLookthroughAssetClass <> 'C' )      
BEGIN      
 INSERT INTO #TempEntityUnderlying (UnderlyingEntityId, AssetClassId, TrackingKey)      
 SELECT  Distinct UnderlyingEntityID, CASE WHEN ISNULL(EAR.AssetClassID,0) = 0 THEN E.AssetClassID ELSE EAR.AssetClassID END AS AssetClassId , L.TrackingKey     
 From #TempInputLines L 
 LEFT JOIN #EntityAssetClassRelationShip EAR ON L.UnderlyingEntityID = EAR.LowerTierEntityID 
	and CASE WHEN ISNULL(ear.TrackingKey, '') = '' THEN ISNULL(l.TrackingKey, '') ELSE ear.TrackingKey END = ISNULL(l.TrackingKey, '')
 INNER JOIN VW_Entity E ON L.UnderlyingEntityID = E.EntityID     
 WHERE  (@IgnoreAssetclassForPartnershipLevel = 'C' AND EntityID <> @LocalEntityID)    
 OR (@IgnoreAssetclassForPartnershipLevel <> 'C')    
END      
ELSE     
BEGIN      
 INSERT INTO #TempEntityUnderlying (UnderlyingEntityId, AssetClassId, TrackingKey)      
 SELECT DISTINCT L.UnderlyingEntityID, CASE WHEN ISNULL(EAR.AssetClassID,0) = 0 THEN E.AssetClassID ELSE EAR.AssetClassID END AS AssetClassId , L.TrackingKey       
 FROM #TempInputLines L JOIN (SELECT DISTINCT TC.UnderlyingEntityId, ImmediateLowerTierEntityID FROM  #TempAllUnderlyingsCombined TC) TC ON TC.UnderlyingEntityId = L.UnderlyingEntityID        
 AND TC.ImmediateLowerTierEntityID  = CASE WHEN L.Trackingkey NOT LIKE '%~%'  THEN L.Trackingkey ELSE RIGHT(L.Trackingkey, ABS(CHARINDEX('~', REVERSE(L.Trackingkey)) - 1)) END     
 LEFT JOIN #EntityAssetClassRelationShip EAR ON TC.ImmediateLowerTierEntityID = EAR.LowerTierEntityID    
 JOIN VW_Entity E ON E.EntityID = TC.ImmediateLowerTierEntityID  --AND E.AssetClassID = TC.AssetClassID        
END    
    
    
-- IF @IgnoreAssetclassForPartnershipLevel  <> 'C'     
--BEGIN      
-- INSERT INTO #TempEntityUnderlying (UnderlyingEntityId, AssetClassId)      
--  SELECT  Distinct UnderlyingEntityID, E.AssetClassID      
-- From #TempInputLines L INNER JOIN Entity E ON L.UnderlyingEntityID = E.EntityID  WHERE EntityID = @LocalEntityID    
--END      
    
    
--SELECT  AI.*,L.LINEID, ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId, L.LINEID ORDER BY hlevel,DisplayOrder) RankForUnderlyingPickup     
--INTO #TempAllUnderlyingsPercentagesOrdered    
--From #TempAllUnderlyingsCombined AI JOIN #TempInputLines L ON L.UnderlyingEntityID = AI.UnderlyingEntityId    
--JOIN ENU_Underlyingtype U on AI.Underlyingtype = U.UnderlyingTypeId    
    
    
--SELECT AI.*,L.LINEID, L.TypeID INTO #TempAllUnderlyingsPercentages FROM #TempAllUnderlyingsPercentagesOrdered O    
--JOIN #TempAllUnderlyingsCombined AI ON AI.UnderlyingEntityId =O.UnderlyingEntityId AND AI.EntityId=O.ENTITYID    
--JOIN #TempInputLines L ON L.UnderlyingEntityID = AI.UnderlyingEntityId    
-- WHERE RankForUnderlyingPickup = 1     
    
--SELECT  AI.*,L.LINEID, ROW_NUMBER() OVER (PARTITION BY AI.UnderlyingEntityId, L.LINEID ORDER BY DisplayOrder,hlevel) RankForUnderlyingPickup     
--INTO #TempAllUnderlyingsPercentagesOrdered    
--From #TempAllUnderlyingsCombined AI JOIN #TempInputLines L ON L.UnderlyingEntityID = AI.UnderlyingEntityId    
--JOIN ENU_Underlyingtype U on AI.Underlyingtype = U.UnderlyingTypeId    
      
--SELECT *INTO #TempAllUnderlyingsPercentages FROM #TempAllUnderlyingsPercentagesOrdered WHERE RankForUnderlyingPickup = 1     
    
--INSERT INTO #TempEntityUnderlyingWithTrackingkey (UnderlyingEntityId, Trackingkey, TrackingkeyMatch)    
--SELECT  Distinct UnderlyingEntityID, Trackingkey, CASE WHEN TrackingKey IS NULL THEN '' ELSE '~' + TrackingKey + '~' END    
--From #TempInputLines    
    
-----------------------Cost Adjusted Dated Transfer------------------------------------------    

INSERT INTO #TempFilteredTransfersAdjCostDefaultPercentage(RunID,ClientID,EntityID,InvestmentID,PartnerNumber,
  TransferPartnerNumber,TransferAdjPercent,EndingCostPercent,TransferDate,TransferDirection,
  BeginningPercentUsage,EffectivePercent,AllocationComplete,AllocationTypeID,TrackingKey,Tag,Underlyingtype,
  FormattedTrackingKey,FormattedEntityID)
SELECT RunID,ClientID,EntityID,InvestmentID,PartnerNumber,
  TransferPartnerNumber,TransferAdjPercent,EndingCostPercent,CASE WHEN ISNULL(isEODTransfer,0) = 1 THEN DATEADD(DAY, 1, TransferDate) ELSE  TransferDate END as TransferDate ,TransferDirection,
  BeginningPercentUsage,EffectivePercent,AllocationComplete,AllocationTypeID,TrackingKey,Tag,Underlyingtype,
  '~' +CASE WHEN ISNULL(T.TrackingKey,'') = '' THEN CONVERT(VARCHAR(4000), 
  IIF(T.InvestmentID = -1, T.EntityID, T.InvestmentID)) ELSE T.TrackingKey END +'~' FormattedTrackingKey,
  IIF(T.InvestmentID = -1, T.EntityID, T.InvestmentID)
  FROM   TransfersAdjCostDefaultPercentage T(NOLOCK)
  WHERE  RunID = @LocalRunID 
  AND ClientID = @LocalClientID

IF(@LocalMode != 4)
BEGIN

INSERT INTO #TempTransfersAdjCostDefaultPercentage(InvestmentID, TransferPartnerNumber, TransferDate, EndingCostPercent, PartnerNumber, 
EffectivePercent, TypeID, TrackingKey, Tag, underlyingtype)    
SELECT DISTINCT E.UnderlyingEntityId, T.TransferPartnerNumber, T.TransferDate TransferDate, T.EndingCostPercent, T.PartnerNumber, 
T.EffectivePercent, ISNULL(T.AllocationTypeId, @CostAllocationTypeID), ISNULL(T.TrackingKey, ''), ISNULL(T.Tag, ''), e.underlyingtype    
FROM #TempFilteredTransfersAdjCostDefaultPercentage T    
INNER JOIN #TempAllUnderlyings E    
ON E.EntityID = T.FormattedEntityID AND E.AllocationTypeId = T.AllocationTypeId AND T.TrackingKey  = E.TrackingKey  
AND E.UnderlyingType = T.underlyingtype
WHERE T.Underlyingtype <> @AssetClassUnderlyingType 
    
SELECT DISTINCT InvestmentID,TypeId,Tag,TrackingKey  INTO #TempCostTransferDefinedDeals    
FROM #TempTransfersAdjCostDefaultPercentage    

INSERT INTO #TempTransfersAdjCostDefaultPercentage(InvestmentID, TransferPartnerNumber, TransferDate, EndingCostPercent, PartnerNumber, EffectivePercent, TypeID, TrackingKey, Tag,TrackingKeyMatch, underlyingtype)    
SELECT DISTINCT E.UnderlyingEntityId, T.TransferPartnerNumber, T.TransferDate TransferDate, T.EndingCostPercent, T.PartnerNumber, T.EffectivePercent, ISNULL(T.AllocationTypeId, @CostAllocationTypeID), ISNULL(T.TrackingKey, ''), ISNULL(T.Tag, ''),CASE WHEN IIF(T.InvestmentID = -1, T.EntityID, T.InvestmentID)!=T.EntityID AND ISNULL(T.TrackingKey,'') = '' THEN E.TrackingMatch ELSE NULL END, E.underlyingtype   
FROM #TempFilteredTransfersAdjCostDefaultPercentage T    
INNER JOIN #TempAllUnderlyings E    
ON E.EntityID = T.FormattedEntityID AND E.AllocationTypeId = T.AllocationTypeId--CROSS JOIN (SELECT DISTINCT UnderlyingEntityId FROM #TempEntityUnderlying) E    
AND  T.FormattedTrackingKey=E.TrackingMatch   
LEFT JOIN #TempCostTransferDefinedDeals D ON E.UnderlyingEntityId = D.InvestmentID     
AND D.Tag=ISNULL(T.Tag, '')    
AND D.TypeId=ISNULL(T.AllocationTypeId, @CostAllocationTypeID)    
AND D.TrackingKey =ISNULL(T.TrackingKey, '')    
WHERE D.InvestmentID IS NULL  AND T.Underlyingtype <> @AssetClassUnderlyingType
  

INSERT INTO #TempTransfersAdjCostDefaultPercentage(InvestmentID, TransferPartnerNumber, TransferDate, EndingCostPercent, PartnerNumber, 
                EffectivePercent, TypeID, TrackingKey, Tag,TrackingKeyMatch, underlyingtype)  
SELECT DISTINCT E.UnderlyingEntityId, T.TransferPartnerNumber, T.TransferDate TransferDate, T.EndingCostPercent, T.PartnerNumber, 
        T.EffectivePercent, ISNULL(T.AllocationTypeId, @CostAllocationTypeID), ISNULL(E.TrackingKey, ''), ISNULL(T.Tag, ''),
        CASE WHEN T.FormattedEntityID != T.EntityID AND ISNULL(T.TrackingKey,'') = '' THEN E.TrackingMatch ELSE NULL END, E.underlyingtype  
FROM #TempFilteredTransfersAdjCostDefaultPercentage T 
INNER JOIN #TempEntityUnderlying EU ON EU.AssetClassId = T.InvestmentId  
INNER JOIN #TempAllUnderlyings E ON EU.UnderlyingEntityID = E.UnderlyingEntityID
AND EU.TrackingKey = E.TrackingKey
AND T.Underlyingtype = E.UnderlyingType
WHERE T.Underlyingtype = @AssetClassUnderlyingType 
END
-----------------------Get Cost Percentage---------------------------------------------------    
     
-----------------------Underlying Only---------------------------------------------------    
    
    
SELECT DISTINCT DealID,TypeId,Tag,TrackingKey  INTO #TempEntityExcludeDeals1    
FROM #TempCostPercentage    
    
--When tracking key is given in allocation data, it will take priority. So get only records with Tracking key    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag, UnderlyingType,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
SELECT DISTINCT E.UnderlyingEntityId, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0), ISNULL(C.AllocationTypeId, @CostAllocationTypeID), ISNULL(E.TrackingKey, ''), ISNULL(C.Tag, ''), C.UnderlyingType,
[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry
FROM #CostPercentage_Snapshot C INNER JOIN #TempAllUnderlyings E    
ON E.EntityID = IIF(C.InvestmentID = -1, C.EntityID, C.InvestmentID) AND E.AllocationTypeId = C.AllocationTypeId AND ISNULL(C.TrackingKey,'')  = E.TrackingKey    
LEFT JOIN #TempEntityExcludeDeals1 D ON E.UnderlyingEntityId = D.DealId    
AND D.Tag=ISNULL(C.Tag, '')    
AND D.TypeId=ISNULL(C.AllocationTypeId, @CostAllocationTypeID)    
AND D.TrackingKey =ISNULL(C.TrackingKey, '')    
WHERE ISNULL(C.UnderlyingType, @EntityUnderlyingtype) = @UnderlyingOnlyUnderlyingType AND D.DealId IS NULL    
--AND C.InvestmentID <> -1    
AND ClientID = @LocalClientID AND TaxPeriodID = @LocalTaxPeriodID    
    
SELECT DISTINCT DealID,TypeId,Tag,TrackingKey  INTO #TempEntityExcludeDeals2    
FROM #TempCostPercentage    
    
--Get records only with empty Tracking Key in Allocation data (2nd priority)    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag, UnderlyingType,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
SELECT DISTINCT E.UnderlyingEntityId, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0), ISNULL(C.AllocationTypeId, @CostAllocationTypeID), ISNULL(E.TrackingKey, ''), ISNULL(C.Tag, ''), C.UnderlyingType,
[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry
FROM #CostPercentage_Snapshot C INNER JOIN #TempAllUnderlyings E    
ON E.EntityID = IIF(C.InvestmentID = -1, C.EntityID, C.InvestmentID) AND E.AllocationTypeId = C.AllocationTypeId AND    
'~' +CASE WHEN ISNULL(C.TrackingKey,'') = '' THEN CONVERT(VARCHAR(4000), IIF(C.InvestmentID = -1, C.EntityID, C.InvestmentID)) ELSE C.TrackingKey END  +'~' =E.TrackingMatch    
LEFT JOIN #TempEntityExcludeDeals2 D ON E.UnderlyingEntityId = D.DealId    
AND D.Tag=ISNULL(C.Tag, '')    
AND D.TypeId=ISNULL(C.AllocationTypeId, @CostAllocationTypeID)    
AND D.TrackingKey =ISNULL(E.TrackingKey, '')    
WHERE ISNULL(C.UnderlyingType, @EntityUnderlyingtype) = @UnderlyingOnlyUnderlyingType AND D.DealId IS NULL    
--AND C.InvestmentID <> -1    
AND ClientID = @LocalClientID AND TaxPeriodID = @LocalTaxPeriodID    
    
-----------------------Entity Total---------------------------------------------------    
    
SELECT DISTINCT DealID,TypeId,Tag,TrackingKey  INTO #TempCostDefinedDeals    
FROM #TempCostPercentage    
    
--When tracking key is given in allocation data, it will take priority. So get only records with Tracking key    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag, UnderlyingType,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
SELECT DISTINCT E.UnderlyingEntityId, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0), ISNULL(C.AllocationTypeId, @CostAllocationTypeID), ISNULL(C.TrackingKey, ''), ISNULL(C.Tag, ''), C.UnderlyingType
,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry FROM #CostPercentage_Snapshot C (NOLOCK)  INNER JOIN #TempAllUnderlyings E    
ON E.EntityID = IIF(C.InvestmentID = -1, C.EntityID, C.InvestmentID) AND E.AllocationTypeId = C.AllocationTypeId AND C.TrackingKey  = E.TrackingKey    
LEFT JOIN #TempCostDefinedDeals D ON E.UnderlyingEntityId = D.DealId    
AND D.Tag=ISNULL(C.Tag, '')    
AND D.TypeId=ISNULL(C.AllocationTypeId, @CostAllocationTypeID)    
AND D.TrackingKey =ISNULL(C.TrackingKey, '')    
WHERE ISNULL(C.UnderlyingType, @EntityUnderlyingtype) = @EntityTotalUnderlyingType AND D.DealId IS NULL    
--AND C.InvestmentID <> -1    
AND ClientID = @LocalClientID AND TaxPeriodID = @LocalTaxPeriodID    
    
SELECT DISTINCT DealID,TypeId,Tag,TrackingKey  INTO #TempCostDefinedDeals1    
FROM #TempCostPercentage    
    
--Get records only with empty Tracking Key in Allocation data (2nd priority)    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag, TrackingKeyMatch,UnderlyingType,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
SELECT DISTINCT E.UnderlyingEntityId, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0), ISNULL(C.AllocationTypeId, @CostAllocationTypeID), ISNULL(C.TrackingKey, ''), ISNULL(C.Tag, '') ,  
--E.TrackingMatch  
case when (IIF(C.InvestmentID = -1, C.EntityID, C.InvestmentID) <> C.EntityId and ISNULL(C.TrackingKey, '') ='') then E.TrackingMatch ELSE null END,C.UnderlyingType  
,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry FROM #CostPercentage_Snapshot C (NOLOCK)  INNER JOIN  (select distinct EntityID,AllocationTypeId,TrackingMatch,UnderlyingEntityId,TrackingKey from #TempAllUnderlyings)  E     
ON E.EntityID = IIF(C.InvestmentID = -1, C.EntityID, C.InvestmentID) AND E.AllocationTypeId = C.AllocationTypeId AND     
'~' +CASE WHEN ISNULL(C.TrackingKey,'') = '' THEN CONVERT(VARCHAR(4000), IIF(C.InvestmentID = -1, C.EntityID, C.InvestmentID)) ELSE C.TrackingKey END  +'~' =E.TrackingMatch    
LEFT JOIN #TempCostDefinedDeals1 D ON E.UnderlyingEntityId = D.DealId    
AND D.Tag=ISNULL(C.Tag, '')    
AND D.TypeId=ISNULL(C.AllocationTypeId, @CostAllocationTypeID)    
AND D.TrackingKey =ISNULL(C.TrackingKey, '')    
WHERE  ISNULL(C.UnderlyingType, @EntityUnderlyingtype) = @EntityTotalUnderlyingType AND D.DealId IS NULL    
--AND C.InvestmentID <> -1    
AND ClientID = @LocalClientID AND TaxPeriodID = @LocalTaxPeriodID    
    
    
-----------------------Underlying Type : Asset Class---------------------------------------------------    
    
SELECT DISTINCT DealID,TypeId,Tag,TrackingKey  INTO #TempExcludeExistingDeals    
FROM #TempCostPercentage    
    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag, UnderlyingType,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
Select E.UnderlyingEntityId, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0), ISNULL(C.AllocationTypeId, @CostAllocationTypeID), ISNULL(E.TrackingKey, ''), ISNULL(C.Tag, ''), C.Underlyingtype
,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry FROM #CostPercentage_Snapshot C (NOLOCK)      
INNER JOIN #TempEntityUnderlying E ON E.AssetClassId = C.InvestmentId     
--INNER JOIN Entity EN ON EN.AssetClassID = C.InvestmentID    
LEFT JOIN #TempExcludeExistingDeals D ON E.UnderlyingEntityId = D.DealId    
AND D.Tag=ISNULL(C.Tag, '')    
AND D.TypeId=ISNULL(C.AllocationTypeId, @CostAllocationTypeID)    
AND D.TrackingKey =ISNULL(C.TrackingKey, '')    
WHERE  ISNULL(C.UnderlyingType, @EntityUnderlyingtype) = @AssetClassUnderlyingType AND D.DealId IS NULL    
AND C.InvestmentID <> -1    
AND C.ClientID = @LocalClientID AND C.TaxPeriodID = @LocalTaxPeriodID    
    
    
SELECT DISTINCT DealID,TypeId,Tag,TrackingKey  INTO #TempCostDefinedDeals3    
FROM #TempCostPercentage    
    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag, UnderlyingType,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
Select E.UnderlyingEntityId, C.PartnerNumber, Quarter, ISNULL(CommitmentPercent ,0), ISNULL(C.AllocationTypeId, @CostAllocationTypeID), ISNULL(C.TrackingKey, ''), ISNULL(C.Tag, ''), C.UnderlyingType
,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry FROM #CostPercentage_Snapshot C (NOLOCK)  CROSS JOIN (SELECT DISTINCT UnderlyingEntityId FROM #TempEntityUnderlying) E    
LEFT JOIN #TempCostDefinedDeals3 D ON E.UnderlyingEntityId = D.DealId    
AND D.Tag=ISNULL(C.Tag, '')    
AND D.TypeId=ISNULL(C.AllocationTypeId, @CostAllocationTypeID)    
AND D.TrackingKey =ISNULL(C.TrackingKey, '')    
WHERE   C.InvestmentID = -1   
AND D.DealId IS NULL    
AND ClientID = @LocalClientID AND TaxPeriodID = @LocalTaxPeriodID AND ISNULL(C.underlyingType, @EntityUnderlyingtype) = @EntityUnderlyingtype    
---------------------------------------Load percentage for ALL scenarios----------------------------------------------------------    
INSERT INTO #TempAllEntities (UnderlyingEntityID, TypeID, TrackingKey, Tag )    
SELECT DISTINCT UnderlyingEntityID, TypeID, TrackingKey, Tag     
FROM #TempDatedEntities    
UNION    
SELECT DISTINCT UnderlyingEntityID, TypeID, TrackingKey, Tag     
FROM #TempNonDatedEntities    
UNION    
SELECT DISTINCT UnderlyingEntityID, @CostAllocationTypeID, TrackingKey, Tag     
FROM #TempDatedEntities    
WHERE TypeID <> @CostAllocationTypeID    
UNION    
SELECT DISTINCT UnderlyingEntityID, @CostAllocationTypeID, TrackingKey, Tag     
FROM #TempNonDatedEntities    
WHERE TypeID <> @CostAllocationTypeID    
    
    
--Delete all entities which have a direct match with cost% table.     
DELETE A     
FROM #TempAllEntities A INNER JOIN #TempCostPercentage C    
ON A.UnderlyingEntityID = C.DealId AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag    

----Insert into % table for underlyings : Input : Tracking key , Tag  -  Percentage : Tag    
SELECT DISTINCT E.UnderlyingEntityId, E.TypeID, E.TrackingKey, E.Tag ,U.EntityId,U.Underlyingtype ,DENSE_RANK() OVER (partition by E.UnderlyingEntityId, E.TypeID, E.TrackingKey, E.Tag ORDER BY Underlyingtype) AS RuleRank
INTO #tmpAllUnderlyingswithParent 
FROM #TempAllEntities E LEFT JOIN #TempAllUnderlyings U
ON E.UnderlyingEntityID = U.UnderlyingEntityId and E.TrackingKey=U.TrackingKey and E.TypeID = U.AllocationTypeID


SELECT DISTINCT UnderlyingEntityId, TypeID, TrackingKey, Tag ,EntityId,Underlyingtype INTO #tmpAllUnderlyingswithParentOrdered
FROM #tmpAllUnderlyingswithParent
WHERE RuleRank=1

--Ajusted Transfer----------
SELECT UnderlyingEntityId, TypeID, TrackingKey, Tag ,EntityId,Underlyingtype INTO #tmpAllUnderlyingswithParentOrdered_Adj
FROM #tmpAllUnderlyingswithParentOrdered

--Insert into % table for underlyings : Input : Tracking key , Tag  -  Percentage : Tag - with Matching Key 
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry) 
SELECT DISTINCT  E.UnderlyingEntityId,
                C.partnernumber,
                C.quarter,
                C.commitmentpercent,
                C.typeid,
                E.trackingkey,
                C.tag,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry			
FROM   #tempcostpercentage C
       INNER JOIN #tmpAllUnderlyingswithParentOrdered E
               ON CASE
                    WHEN ((ISNULL(E.entityid,C.dealid) = C.dealid) AND ISNULL(C.underlyingType, @EntityUnderlyingtype)=@EntityTotalUnderlyingType )  THEN E.underlyingentityid
                    ELSE C.dealid
                  END = E.underlyingentityid
                  AND C.typeid = E.typeid
                  AND C.tag = E.tag
                  AND REPLACE(C.TrackingKeyMatch, '~', '') = E.EntityID
				  AND ISNULL(C.UnderlyingType, @EntityUnderlyingtype)=ISNULL(E.Underlyingtype, @EntityUnderlyingtype)
                  AND C.trackingkey = ''
                  AND    '~' + E.trackingkey + '~'   LIKE '%' + C.trackingkeymatch + '%'			
                  WHERE ISNULL(TrackingKeyMatch ,'')<>''

--Adjusted Transfer-----------------------
INSERT INTO #TempTransfersAdjCostDefaultPercentage(InvestmentID, TransferPartnerNumber, TransferDate, EndingCostPercent, PartnerNumber,     
     EffectivePercent, TypeID, TrackingKey, Tag) 
SELECT DISTINCT  E.UnderlyingEntityId,
                C.TransferPartnerNumber,
				C.TransferDate,
                C.EndingCostPercent,
				C.PartnerNumber,
				C.EffectivePercent,
                C.typeid,
                E.trackingkey,
                C.tag		
FROM   #TempTransfersAdjCostDefaultPercentage C 
       INNER JOIN #tmpAllUnderlyingswithParentOrdered_Adj E
                 ON CASE
                    WHEN ((ISNULL(E.entityid,C.InvestmentID) = C.InvestmentID) AND ISNULL(C.underlyingType, @EntityUnderlyingtype)=@EntityTotalUnderlyingType )  THEN E.underlyingentityid
                    ELSE C.InvestmentID
                  END = E.underlyingentityid
                  AND
				  C.typeid = E.typeid
                  AND C.tag = E.tag
				  AND ISNULL(C.UnderlyingType, @EntityUnderlyingtype)=ISNULL(E.Underlyingtype, @EntityUnderlyingtype)
                  AND C.trackingkey = ''
                  AND    '~' + E.trackingkey + '~'   LIKE '%' + C.trackingkeymatch + '%'			
                  WHERE ISNULL(TrackingKeyMatch ,'')<>'' 

							   --Delete all entities which have a direct match with cost% table.   
DELETE A   
FROM #tmpAllUnderlyingswithParentOrdered A INNER JOIN #TempCostPercentage C  
ON A.UnderlyingEntityID = C.DealId AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag  

--Adjusted Transfer------------
DELETE A   
FROM #tmpAllUnderlyingswithParentOrdered_Adj A INNER JOIN  #TempTransfersAdjCostDefaultPercentage C  
ON A.UnderlyingEntityID = C.InvestmentID AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag 

 --Insert into % table for underlyings : Input : Tracking key , Tag  -  Percentage : Tag  -- without Matching Key 
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry) 
SELECT DISTINCT  E.UnderlyingEntityId,
                C.partnernumber,
                C.quarter,
                C.commitmentpercent,
                C.typeid,
                E.trackingkey,
                C.tag	,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry		
FROM   #tempcostpercentage C 
       INNER JOIN #tmpAllUnderlyingswithParentOrdered E
               ON CASE
                    WHEN ((ISNULL(E.entityid,C.dealid) = C.dealid) AND ISNULL(C.underlyingType, @EntityUnderlyingtype)=@EntityTotalUnderlyingType )  THEN E.underlyingentityid
                    ELSE C.dealid
                  END = E.underlyingentityid
                  AND C.typeid = E.typeid
                  AND C.tag = E.tag
				  AND ISNULL(C.UnderlyingType, @EntityUnderlyingtype)=ISNULL(E.Underlyingtype, @EntityUnderlyingtype)
                  AND C.trackingkey = ''
                  WHERE ISNULL(TrackingKeyMatch ,'')=''	
--Adjusted Transfer-------------------------------
INSERT INTO #TempTransfersAdjCostDefaultPercentage(InvestmentID, TransferPartnerNumber, TransferDate, EndingCostPercent, PartnerNumber,     
     EffectivePercent, TypeID, TrackingKey, Tag) 
SELECT DISTINCT  E.UnderlyingEntityId,
                C.TransferPartnerNumber,
				C.TransferDate,
                C.EndingCostPercent,
				C.PartnerNumber,
				C.EffectivePercent,
                C.typeid,
                E.trackingkey,
                C.tag					
FROM   #TempTransfersAdjCostDefaultPercentage C
       INNER JOIN #tmpAllUnderlyingswithParentOrdered_Adj E
               ON CASE
                    WHEN ((ISNULL(E.entityid,C.InvestmentID) = C.InvestmentID) AND ISNULL(C.underlyingType, @EntityUnderlyingtype)=@EntityTotalUnderlyingType )  THEN E.underlyingentityid
                    ELSE C.InvestmentID
                  END = E.underlyingentityid
                  AND C.typeid = E.typeid
                  AND C.tag = E.tag
				  AND ISNULL(C.UnderlyingType, @EntityUnderlyingtype)=ISNULL(E.Underlyingtype, @EntityUnderlyingtype)
                  AND C.trackingkey = ''
                  WHERE ISNULL(TrackingKeyMatch ,'')='' 
          
 DROP TABLE #tmpallunderlyingswithparent
 DROP TABLE #tmpAllUnderlyingswithParentOrdered
 DROP TABLE #tmpAllUnderlyingswithParentOrdered_Adj

---Adjusted Transfer-------------
SELECT UnderlyingEntityID, TypeID, TrackingKey, Tag INTO #TempAllEntities_Adj FROM #TempAllEntities

DELETE A   
FROM #TempAllEntities_Adj A INNER JOIN #TempTransfersAdjCostDefaultPercentage C  
ON A.UnderlyingEntityID = C.InvestmentID AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag 
--------------------------
DELETE A   
FROM #TempAllEntities A INNER JOIN #TempCostPercentage C  
ON A.UnderlyingEntityID = C.DealId AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag  
 
-- SELECT DISTINCT E.UnderlyingEntityId, E.TypeID, E.TrackingKey, E.Tag ,U.EntityId,U.Underlyingtype,u.trackingmatch 
--INTO #tmpAllUnderlyingsWithTrackingKeyMatch     
--FROM #TempAllEntities E LEFT JOIN #TempAllUnderlyings U    
--ON E.UnderlyingEntityID = U.UnderlyingEntityId and E.TrackingKey=U.TrackingKey    
--and e.TypeID=u.AllocationTypeId 

----CostAdjusted Transfer-----------------
--SELECT DISTINCT E.UnderlyingEntityId, E.TypeID, E.TrackingKey, E.Tag ,U.EntityId,U.Underlyingtype,u.trackingmatch   
--INTO #tmpAllUnderlyingsWithTrackingKeyMatch_Adj     
--FROM #TempAllEntities_Adj E LEFT JOIN #TempAllUnderlyings U    
--ON E.UnderlyingEntityID = U.UnderlyingEntityId and E.TrackingKey=U.TrackingKey    
--and e.TypeID=u.AllocationTypeId 
--------------------------
-- --added this logic.  
--INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag)    
--SELECT C.DealId, C.Partnernumber, C.Quarter, C.CommitmentPercent, C.TypeId, E.TrackingKey, C.Tag    
--FROM #TempCostPercentage C (NOLOCK) INNER JOIN #tmpAllUnderlyingsWithTrackingKeyMatch E    
--ON C.DealId = E.UnderlyingEntityID AND C.TypeId = E.TypeID  AND C.Tag = E.Tag AND C.TrackingKey = ''  and  CASE WHEN ISNULL(C.TrackingKeyMatch,'')!='' THEN  
--'~'+E.trackingmatch+'~'  ELSE '-1' END  LIKE CASE WHEN ISNULL(C.TrackingKeyMatch,'')!='' THEN '%'+C.TrackingKeyMatch+'%'  ELSE '-1' END  
  
  
  
--INSERT INTO #TempTransfersAdjCostDefaultPercentage(InvestmentID, TransferPartnerNumber, TransferDate, EndingCostPercent, PartnerNumber,     
--     EffectivePercent, TypeID, TrackingKey, Tag)    
--SELECT C.InvestmentID, C.TransferPartnerNumber, C.TransferDate, C.EndingCostPercent, C.PartnerNumber, C.EffectivePercent, C.TypeID, E.TrackingKey, C.Tag    
--FROM #TempTransfersAdjCostDefaultPercentage C (NOLOCK) INNER JOIN #tmpAllUnderlyingsWithTrackingKeyMatch_Adj E    
--ON C.InvestmentID = E.UnderlyingEntityID AND C.TypeId = E.TypeID  AND C.Tag = E.Tag AND C.TrackingKey = '' AND CASE WHEN ISNULL(C.TrackingKeyMatch,'')!='' THEN  
--'~'+E.trackingmatch+'~'  ELSE '-1' END  LIKE CASE WHEN ISNULL(C.TrackingKeyMatch,'')!='' THEN '%'+C.TrackingKeyMatch+'%'  ELSE '-1' END  
  
-------Start Fix for Entity Total by using Tracking Key Match--------------  
Delete T  
FROM #TempCostPercentage T   
WHERE  ISNULL(T.TrackingKey,'')='' and ISNULL(T.TrackingKeyMatch,'') !=''     
  
Delete T  
FROM #TempTransfersAdjCostDefaultPercentage T   
WHERE  ISNULL(T.TrackingKey,'')='' and ISNULL(T.TrackingKeyMatch,'') !=''     
  
-------End Fix for Entity Total by using Tracking Key Match--------------  
     
DELETE A     
FROM #TempAllEntities A INNER JOIN #TempCostPercentage C    
ON A.UnderlyingEntityID = C.DealId AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag  

--adjusted Transfer----------
DELETE A     
FROM #TempAllEntities_Adj A INNER JOIN #TempTransfersAdjCostDefaultPercentage C    
ON A.UnderlyingEntityID = C.InvestmentId AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag  

-----------------------------------------------------------------------------------------------------------------    
--Insert into % table for underlyings : Input : Tracking key , Tag  -  Percentage : Trackingkey    
    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
SELECT DISTINCT C.DealId, C.Partnernumber, C.Quarter, C.CommitmentPercent, C.TypeId, C.TrackingKey, E.Tag,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry    
FROM #TempCostPercentage C INNER JOIN #TempAllEntities E    
ON C.DealId = E.UnderlyingEntityID AND C.TypeId = E.TypeID  AND C.TrackingKey = E.TrackingKey AND C.Tag = ''    
    
INSERT INTO #TempTransfersAdjCostDefaultPercentage(InvestmentID, TransferPartnerNumber, TransferDate, EndingCostPercent, PartnerNumber,     
     EffectivePercent, TypeID, TrackingKey, Tag)    
SELECT C.InvestmentID, C.TransferPartnerNumber, C.TransferDate, C.EndingCostPercent, C.PartnerNumber, C.EffectivePercent, C.TypeID, C.TrackingKey, E.Tag    
FROM #TempTransfersAdjCostDefaultPercentage C INNER JOIN #TempAllEntities_Adj E    
ON C.InvestmentID = E.UnderlyingEntityID AND C.TypeId = E.TypeID  AND C.TrackingKey = E.TrackingKey AND C.Tag = ''     
     
DELETE A     
FROM #TempAllEntities A INNER JOIN #TempCostPercentage C    
ON A.UnderlyingEntityID = C.DealId AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag 

-- Adjusted Transfer--------
DELETE A     
FROM #TempAllEntities_Adj A INNER JOIN #TempTransfersAdjCostDefaultPercentage C    
ON A.UnderlyingEntityID = C.InvestmentId AND A.TypeID = C.TypeId AND A.TrackingKey = C.TrackingKey AND A.Tag = C.Tag 
-----------------------------------------------------------------------------------------------------------------    
----Insert into % table for underlyings : Input : Tracking key , Tag  -  Percentage : Nothing    
INSERT INTO #TempCostPercentage(DealId, Partnernumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
SELECT DISTINCT C.DealId, C.Partnernumber, C.Quarter, C.CommitmentPercent, C.TypeId, E.TrackingKey, E.Tag,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry
FROM #TempCostPercentage C INNER JOIN #TempAllEntities E    
ON C.DealId = E.UnderlyingEntityID AND C.TypeId = E.TypeID  AND C.TrackingKey = '' AND C.Tag = ''    
    
INSERT INTO #TempTransfersAdjCostDefaultPercentage(InvestmentID, TransferPartnerNumber, TransferDate, EndingCostPercent, PartnerNumber,     
     EffectivePercent, TypeID, TrackingKey, Tag)    
SELECT C.InvestmentID, C.TransferPartnerNumber, C.TransferDate, C.EndingCostPercent, C.PartnerNumber, C.EffectivePercent, C.TypeID, E.TrackingKey, E.Tag    
FROM #TempTransfersAdjCostDefaultPercentage C INNER JOIN #TempAllEntities_Adj E    
ON C.InvestmentID = E.UnderlyingEntityID AND C.TypeId = E.TypeID  AND C.TrackingKey = '' AND C.Tag = ''     
    
------------------------------Pick Entities which do not have percentages for the type defined----------------    
INSERT INTO #TempNonDatedEntitiesCost(UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)    
SELECT DISTINCT UnderlyingEntityID, ISNULL(D.LineTypeID, -1), D.TypeID, D.TrackingKey, D.Tag, D.IsExcludefromTransfer    
FROM #TempNonDatedEntities D LEFT JOIN #TempCostPercentage C    
ON D.UnderlyingEntityID = C.DealId AND D.TypeID = C.TypeId AND D.TrackingKey = C.TrackingKey AND D.Tag = C.Tag    
WHERE D.TypeID <> @CostAllocationTypeID AND D.UnderlyingEntityID IS NULL    
    
INSERT INTO #TempDatedEntitiesCost(Quarter, UnderlyingEntityID, LineTypeID, TypeID, TrackingKey, Tag, IsExcludefromTransfer)    
SELECT DISTINCT D.Quarter, D.UnderlyingEntityID, ISNULL(D.LineTypeID, -1), D.TypeID, D.TrackingKey, D.Tag, D.IsExcludefromTransfer    
FROM #TempDatedEntities D  LEFT JOIN #TempCostPercentage C    
ON D.UnderlyingEntityID = C.DealId AND D.TypeID = C.TypeId AND D.TrackingKey = C.TrackingKey AND D.Tag = C.Tag    
AND  C.Quarter <= D.Quarter    
WHERE D.TypeID <> @CostAllocationTypeID  AND D.UnderlyingEntityID IS NULL    
    
Update D SET D.TypeID = @CostAllocationTypeID    
FROM    
#TempDatedEntities D INNER JOIN #TempDatedEntitiesCost C     
ON D.UnderlyingEntityID = C.UnderlyingEntityID AND ISNULL(D.LineTypeID, -1) = ISNULL(C.LineTypeID, -1) AND D.Quarter = C.Quarter    
 AND D.TypeID = C.TypeId AND D.TrackingKey = C.TrackingKey AND D.Tag = C.Tag AND D.IsExcludefromTransfer = C.IsExcludefromTransfer    
    
 Update D SET D.TypeID = @CostAllocationTypeID    
FROM    
#TempNonDatedEntities D INNER JOIN #TempNonDatedEntitiesCost C     
ON D.UnderlyingEntityID = C.UnderlyingEntityID AND ISNULL(D.LineTypeID, -1) = ISNULL(C.LineTypeID, -1)    
 AND D.TypeID = C.TypeId AND D.TrackingKey = C.TrackingKey AND D.Tag = C.Tag AND D.IsExcludefromTransfer = C.IsExcludefromTransfer    
    
    
    
    
----------------------------------------Finding Min quarter-------------------------------------------------    
INSERT INTO #TempCostPercentageDeals(DealId, Quarter, TypeID, TrackingKey, Tag)    
SELECT DISTINCT DealID, Quarter, TypeId, TrackingKey, Tag    
FROM #TempCostPercentage    
    
INSERT INTO #FinalCostPercentage(DealId, PartnerNumber, Quarter, CommitmentPercent,TypeId, TrackingKey, Tag,[704cAllocationTypeID],[704cPercentageType], GPPartnerReceivingCarry)    
Select D.DealId, P.PartnerNumber, D.Quarter, ISNULL(C.CommitmentPercent ,0), D.TypeID, D.TrackingKey, D.Tag ,C.[704cAllocationTypeID],C.[704cPercentageType], C.GPPartnerReceivingCarry   
FROM #TempCostPercentageDeals D CROSS JOIN #entitypartners P     
LEFT JOIN  #TempCostPercentage C      
ON  C.Partnernumber = P.PartnerNumber AND C.DealId = D.DealId    
AND D.Quarter = C.Quarter AND D.TypeID = C.TypeId AND D.TrackingKey = C.TrackingKey AND D.Tag = C.Tag    
    
---------------------------------------Cost Percentage not equal to 100% due to missing partners------------------    
    
    
IF(@LocalMode = 1)    
BEGIN    
INSERT INTO #TempErrorUnderlyings(DealID)    
Select Distinct DealId     
FROM #FinalCostPercentage F LEFT JOIN #DefaultAllocationRuleSetup E ON F.TypeId=E.RuleID    
WHERE E.RuleID IS NULL  
GROUP By DealId, Quarter, TypeId, TrackingKey, Tag    
Having Round(Sum(CommitmentPercent),8) <> 1.00000000    
    
    
IF EXISTS(SELECT 1 From #TempErrorUnderlyings)    
BEGIN     
    
Declare @UnderlyingNames varchar(max)     
    
SELECT @UnderlyingNames = COALESCE(@UnderlyingNames + ', ', '') + E.DisplayName    
       FROM #TempErrorUnderlyings  U Inner Join VW_Entity E with(nolock) ON    
    U.DealId = E.EntityID    
        
    
INSERT INTO AllocationRunErrors(RunID, EntityID, ErrorMessage, LogID, ErrororWarning)      
SELECT @LocalRunID, @LocalEntityID, 'The sum of Cost Percentage does not sum to 100% for following deals -'+ ISNULL(@UnderlyingNames, ''), @LogID, 'Error'    
    
Update AllocationRun     
SET RunStatus = 'FAIL',
RunEndDate = GETDATE()
WHERE RunID = @LocalRunID    
    
SET @EndDate = GETDATE()    
EXEC [dbo].[uspUpdateAllocationLog] @LogID, @EndDate     
    
    
RETURN    
    
END    
END    
    
-----------------------FIND Minimum quarter----------------------------------     
IF (@AllocationTypeName = 'PE Book Allocation' AND @IsDatedTransfersConfigured = 'C')    
BEGIN    
INSERT INTO #TempMinimumQuarter(DealID, TypeID, TrackingKey, Tag, MinQuarter)    
Select DISTINCT T.DealId, T.TypeId, T.TrackingKey, T.Tag,  Min(D.Preference) MinQuarter      
FROM #FinalCostPercentage T INNER JOIN QuarterDates D (NOLOCK)     
ON T.Quarter = D.Quarter    
Group By T.DealId, T.TypeId, T.TrackingKey, T.Tag    
    
INSERT INTO #TempCostPercentageMinQuarter(DealID, TypeID, TrackingKey, Tag, Quarter, Preference)    
SELECT T.DealId, T.TypeID, T.TrackingKey, T.Tag, D.Quarter, D.Preference    
FROM #TempMinimumQuarter T INNER JOIN  QuarterDates D  (NOLOCK)    
ON T.MinQuarter = D.Preference    
END    
ELSE    
BEGIN    
INSERT INTO #TempMinimumQuarter(DealID, TypeID, TrackingKey, Tag, MinQuarter, QuarterType)    
Select DISTINCT T.DealId, T.TypeId, T.TrackingKey, T.Tag,  Min(D.DisplayOrder) MinQuarter, D.Comments      
FROM #FinalCostPercentage T INNER JOIN ENU_DF_DataList D     
ON D.Category = 'Quarters' AND T.Quarter = D.LookUpData  
AND T.Quarter LIKE ISNULL(D.Comments,'') + '%' -- Using comments column to filter the records based on percentages provided for Quarter/Month 
Group By T.DealId, T.TypeId, T.TrackingKey, T.Tag, D.Comments    
    
INSERT INTO #TempCostPercentageMinQuarter(DealID, TypeID, TrackingKey, Tag, Quarter)    
SELECT T.DealId, T.TypeID, T.TrackingKey, T.Tag, D.LookUpData    
FROM #TempMinimumQuarter T INNER JOIN  ENU_DF_DataList D     
ON D.Category = 'Quarters' AND T.MinQuarter = D.DisplayOrder AND ISNULL(D.Comments,'') = ISNULL(T.QuarterType,'')

--If a deal has Q0 and a Monthly percentage as Min quarter from the above query. We are retaining Q0.
--Below query deletes recods that are not Q0 for a deal that has Q0
DELETE T FROM #TempCostPercentageMinQuarter T
INNER JOIN #TempCostPercentageMinQuarter Q
ON T.DealId = Q.DealId AND T.TypeID = Q.TypeID
AND T.TrackingKey = Q.TrackingKey AND T.Tag = Q.Tag
WHERE Q.Quarter = 'Q0' AND T.Quarter <> 'Q0'

--Picking the distinct Quarter types provided for a deal
SELECT DISTINCT DealId, TypeId, TrackingKey, Tag, D.Comments AS QuarterType INTO #QuarterTypeByDeal
FROM #FinalCostPercentage T INNER JOIN ENU_DF_DataList D     
ON D.Category = 'Quarters' AND T.Quarter = D.LookUpData

SELECT DealId, TypeId, TrackingKey, Tag INTO #MultipleQuarterTypes FROM #QuarterTypeByDeal
GROUP BY DealId, TypeId, TrackingKey, Tag
HAVING COUNT(DISTINCT QuarterType) > 1

--If there are multiple quarter types provided for deal i.e; Quarter and Month we will delete Quarter records in below quaery. Preference is for Monthly allocaions
--If the entity has only quarterly allocations - #QuarterTypeByDeal will only have Q records and nothing will be deleted in below query
DELETE Q
FROM #QuarterTypeByDeal Q
INNER JOIN #MultipleQuarterTypes M
ON Q.DealId = M.DealId AND M.TypeId = Q.TypeId
AND M.TrackingKey = Q.TrackingKey AND M.Tag = Q.Tag
WHERE Q.QuarterType = 'Q'

--As there are both Quarter and Months in ENU_DF_DataList table. #TempDatedEntities will have records for both Q and M for a deal. 
--We need to preserve only one of them to prevent duplicate allocations.
--If the client only has Quarterly Allocations - #QuarterTypeByDeal will have records with Q and with left join Monthly recrds will be deleted from #TempDatedEntities
--If the client has both Q and M allocations - #QuarterTypeByDeal will have records with M and with left join quarterly recrds will be deleted from #TempDatedEntities
DELETE D
FROM #TempDatedEntities D   
INNER JOIN ENU_DF_DataList DQ ON DQ.Category = 'Quarters' AND D.Quarter = DQ.LookUpData
LEFT JOIN #QuarterTypeByDeal F    
ON D.UnderlyingEntityID = F.DealId AND DQ.Comments = F.QuarterType    
AND D.TypeID = F.TypeId AND D.TrackingKey = F.TrackingKey AND D.Tag = F.Tag
WHERE f.DealId IS NULL

DROP TABLE IF EXISTS #QuarterTypeByDeal
DROP TABLE IF EXISTS #MultipleQuarterTypes 
END    
    
--------------------------------------------Calculation--------------------------------------------------------------------    
  
----------------------------------------Cost Percentage-------------------------------------------------------    
INSERT INTO #TempFinalEffectivePercentageDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, PickUpOrder,TypeId, TrackingKey, Tag, IsExcludefromTransfer, GPPartnerReceivingCarry)    
SELECT DISTINCT L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1) ,C.Partnernumber, ISNULL(C.CommitmentPercent,0), L.Quarter,  'Cost', 1, C.TypeId, C.TrackingKey, C.Tag, L.IsExcludefromTransfer, ISNULL(C.GPPartnerReceivingCarry,0)    
FROM     
#TempDatedEntities L     
Inner Join #FinalCostPercentage C     
ON L.UnderlyingEntityID = C.DealId AND C.Quarter = L.QUARTER AND L.TypeID = C.TypeId    
AND L.TrackingKey = C.TrackingKey AND L.Tag = C.Tag    
GROUP BY L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1) ,C.Partnernumber, ISNULL(C.CommitmentPercent,0), L.Quarter, C.TypeId, C.TrackingKey, C.Tag, L.IsExcludefromTransfer  , ISNULL(C.GPPartnerReceivingCarry,0)   
    
DELETE D    
FROM     
#TempDatedEntities D INNER JOIN #TempFinalEffectivePercentageDated F    
ON D.UnderlyingEntityID = F.InvestmentID AND D.QUARTER = F.Quarter    
AND D.TypeID = F.TypeId AND D.TrackingKey = F.TrackingKey AND D.Tag = F.Tag    
    
--------------------------------Transfer affected Cost Percentage----------------------------------------------    
    
SELECT T.InvestmentID, T.TransferPartnerNumber, T.TransferDate TransferDate, T.EndingCostPercent, T.PartnerNumber, T.TypeID, T.TrackingKey, T.Tag    
INTO #TempTransferAdjDatedPercentages    
FROM #TempTransfersAdjCostDefaultPercentage T     
WHERE T.TransferDate IS NOT NULL    
UNION    
SELECT -1, T.TransferPartnerNumber, CASE WHEN ISNULL(T.isEODTransfer,0) = 1 THEN DATEADD(DAY, 1, T.TransferDate) ELSE  TransferDate END as TransferDate, T.EndingCommitmentPercent, T.PartnerNumber, -1, '', ''    
FROM TransfersAdjDefaultPercentage T with(nolock)    
WHERE RunID = @LocalRunID AND ClientID = @LocalClientID  AND T.TransferDate IS NOT NULL    
    
IF (@AllocationTypeName = 'PE Book Allocation' AND @IsDatedTransfersConfigured = 'C')    
BEGIN    
INSERT INTO #TempTransferDate(UnderlyingEntityID,LineTypeID,QUARTER,UnderlyingTypeID,UnderlyingTrackingKey,UnderlyingTag,TypeID,TrackingKey,Tag,InvestmentID,TransferPartnerNumber,TransferDate)    
SELECT L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1) LineTypeID, L.QUARTER, L.TypeID UnderlyingTypeID, L.TrackingKey UnderlyingTrackingKey,    
L.Tag UnderlyingTag, T.TypeID, T.TrackingKey, T.Tag,  T.InvestmentID, T.TransferPartnerNumber, Max(T.TransferDate) TransferDate    
FROM     
#TempDatedEntities L     
INNER JOIN #TempTransferAdjDatedPercentages T     
ON CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE T.InvestmentID END  =    
 CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE L.UnderlyingEntityID END    
 AND CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE T.TypeID END  =    
 CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE L.TypeID END    
 AND CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE T.TrackingKey END  =    
 CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE L.TrackingKey END    
  AND CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE T.Tag END  =    
 CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE L.Tag END    
INNER JOIN QuarterDates D (NOLOCK) ON ISNULL(T.TransferDate, '1900-01-01') BETWEEN D.StartDate And D.EndDate     
  --AND D.QUARTER <= L.QUARTER     
  INNER JOIN K1LineItem K with(nolock)   
  ON K.LineID = L.LineID    
WHERE L.IsExcludefromTransfer = 0 AND (ISNULL(K.TransactionDate, '1900-01-01') >= ISNULL(T.TransferDate, '1900-01-01')) AND L.LineTypeID = @K1LineTypeID
Group By L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1), L.QUARTER, L.TypeID, L.Trackingkey, L.Tag, T.TypeID, T.TrackingKey, T.Tag, T.InvestmentID, T.TransferPartnerNumber  

DECLARE @Form926LineTypeID INT

SELECT @Form926LineTypeID = LineTypeID FROM ENU_LineType WHERE LineType='Form926'

INSERT INTO #TempTransferDate(UnderlyingEntityID,LineTypeID,QUARTER,UnderlyingTypeID,UnderlyingTrackingKey,UnderlyingTag,TypeID,TrackingKey,Tag,InvestmentID,TransferPartnerNumber,TransferDate)    
SELECT L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1) LineTypeID, L.QUARTER, L.TypeID UnderlyingTypeID, L.TrackingKey UnderlyingTrackingKey,    
L.Tag UnderlyingTag, T.TypeID, T.TrackingKey, T.Tag,  T.InvestmentID, T.TransferPartnerNumber, Max(T.TransferDate) TransferDate    
FROM     
#TempDatedEntities L     
INNER JOIN #TempTransferAdjDatedPercentages T     
ON CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE T.InvestmentID END  =    
 CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE L.UnderlyingEntityID END    
 AND CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE T.TypeID END  =    
 CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE L.TypeID END    
 AND CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE T.TrackingKey END  =    
 CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE L.TrackingKey END    
  AND CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE T.Tag END  =    
 CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE L.Tag END    
INNER JOIN QuarterDates D (NOLOCK) ON ISNULL(T.TransferDate, '1900-01-01') BETWEEN D.StartDate And D.EndDate     
  --AND D.QUARTER <= L.QUARTER     
WHERE L.IsExcludefromTransfer = 0 AND L.LineTypeID = @Form926LineTypeID and (ISNULL(L.Transferdate, '1900-01-01') >= ISNULL(T.TransferDate, '1900-01-01'))

Group By L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1), L.QUARTER, L.TypeID, L.Trackingkey, L.Tag, T.TypeID, T.TrackingKey, T.Tag, T.InvestmentID, T.TransferPartnerNumber    
END    
ELSE    
BEGIN    
INSERT INTO #TempTransferDate(UnderlyingEntityID,LineTypeID,QUARTER,UnderlyingTypeID,UnderlyingTrackingKey,UnderlyingTag,TypeID,TrackingKey,Tag,InvestmentID,TransferPartnerNumber,TransferDate)    
SELECT DISTINCT L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1) LineTypeID, L.QUARTER, L.TypeID UnderlyingTypeID, L.TrackingKey UnderlyingTrackingKey,    
L.Tag UnderlyingTag, T.TypeID, T.TrackingKey, T.Tag,  T.InvestmentID, T.TransferPartnerNumber, Max(T.TransferDate) TransferDate    
FROM     
#TempDatedEntities L     
INNER JOIN #TempTransferAdjDatedPercentages T     
ON CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE T.InvestmentID END  =    
 CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE L.UnderlyingEntityID END    
 AND CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE T.TypeID END  =    
 CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE L.TypeID END    
 AND CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE T.TrackingKey END  =    
 CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE L.TrackingKey END    
  AND CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE T.Tag END  =    
 CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE L.Tag END    
Inner Join ENU_DF_DataList D On D.LookUpValue = ISNULL(Month(T.TransferDate), 0) AND D.Category = 'QuarterMonth'  AND L.Quarter LIKE D.Comments + '%'
inner join ENU_DF_DataList DF ON DF.Category = 'QuarterMonth'  AND  L.Quarter = DF.LookUpData AND CONVERT(INT, D.LookUpValue) <= CONVERT(INT, DF.LookUpValue)  
  --AND D.LookUpData <= L.QUARTER      
WHERE L.IsExcludefromTransfer = 0    
Group By L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1), L.QUARTER, L.TypeID, L.Trackingkey, L.Tag, T.TypeID, T.TrackingKey, T.Tag, T.InvestmentID, T.TransferPartnerNumber    
END    

IF(ISNULL(@PartVAllocated,0) = 1)
BEGIN		
	INSERT INTO #TempTransferDate(UnderlyingEntityID,LineTypeID,QUARTER,UnderlyingTypeID,UnderlyingTrackingKey,UnderlyingTag,TypeID,TrackingKey,Tag,InvestmentID,TransferPartnerNumber,TransferDate)    
	SELECT L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1) LineTypeID, L.QUARTER, L.TypeID UnderlyingTypeID, L.TrackingKey UnderlyingTrackingKey,    
	L.Tag UnderlyingTag, T.TypeID, T.TrackingKey, T.Tag,  T.InvestmentID, T.TransferPartnerNumber, Max(T.TransferDate) TransferDate    
	FROM     
	#TempDatedEntities L     
	INNER JOIN #TempTransferAdjDatedPercentages T     
	ON CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE T.InvestmentID END  =    
	 CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE L.UnderlyingEntityID END    
	 AND CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE T.TypeID END  =    
	 CASE WHEN  T.InvestmentID =-1 THEN 1 ELSE L.TypeID END    
	 AND CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE T.TrackingKey END  =    
	 CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE L.TrackingKey END    
	  AND CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE T.Tag END  =    
	 CASE WHEN  T.InvestmentID =-1 THEN '1' ELSE L.Tag END    
	INNER JOIN QuarterDates D (NOLOCK) ON ISNULL(T.TransferDate, '1900-01-01') BETWEEN D.StartDate And D.EndDate     	  
    LEFT JOIN #TempTransferDate TD ON TD.UnderlyingEntityID = L.UnderlyingEntityID
    AND TD.LineTypeID = ISNULL(L.LineTypeID, -1) AND TD.QUARTER = L.Quarter AND L.TypeID = TD.UnderlyingTypeID AND L.TrackingKey = TD.UnderlyingTrackingKey
    AND ISNULL(L.Tag,'') = ISNULL(TD.UnderlyingTag,'') AND TD.TypeID = T.TypeID AND T.TrackingKey =TD.TrackingKey
    AND T.Tag = TD.Tag AND T.InvestmentID =TD.InvestmentID AND T.TransferPartnerNumber =TD.TransferPartnerNumber
	WHERE L.IsExcludefromTransfer = 0 AND L.LineTypeID = @PFICFootnoteLineTypeID and (ISNULL(L.Transferdate, '1900-01-01') >= ISNULL(T.TransferDate, '1900-01-01'))
    AND TD.InvestmentID IS NULL
	Group By L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1), L.QUARTER, L.TypeID, L.Trackingkey, L.Tag, T.TypeID, T.TrackingKey, T.Tag, T.InvestmentID, T.TransferPartnerNumber    

    --delete quarter for which transferdate was not applied
    DELETE L 
	FROM #TempDatedEntities L 
	LEFT JOIN #TempTransferDate T ON T.QUarter = L.Quarter
	WHERE T.Quarter IS NULL AND ISNULL(L.LineTypeID, -1)=@PFICFootnoteLineTypeID
END
    
INSERT INTO #TempFinalEffectivePercentageDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, PickUpOrder, TypeId, TrackingKey, Tag, IsExcludefromTransfer)    
SELECT P.UnderlyingEntityID, ISNULL(P.LineTypeID, -1), T.Partnernumber, SUM(ISNULL(T.EndingCostPercent,0)), P.QUARTER,     
CASE WHEN T.InvestmentID = -1 THEn 'ProRata' ELSE 'CostAdjustedDatedTransfer' END ,     
CASE WHEN T.InvestmentID = -1 THEn 3 ELSE 2 END , P.UnderlyingTypeID, P.UnderlyingTrackingKey, P.UnderlyingTag, 0    
FROM     
#TempTransferAdjDatedPercentages T     
INNER JOIN  #TempTransferDate P ON T.InvestmentID   = P.InvestmentID  AND T.TransferPartnerNumber = P.TransferPartnerNumber     
AND isnull(T.TransferDate,'1/1/9999') = isnull(P.TransferDate,'1/1/9999') AND T.TypeID = P.TypeID AND T.TrackingKey = P.TrackingKey AND T.Tag = P.Tag    
Group By P.UnderlyingEntityID, ISNULL(P.LineTypeID, -1),T.PartnerNumber,P.QUARTER, T.InvestmentID, T.TypeID, T.TrackingKey, T.Tag, P.UnderlyingTypeID, P.UnderlyingTrackingKey, P.UnderlyingTag    
    
--to take exclude from transfer    
IF(@IsDatedTransfersConfigured = 'C')
BEGIN
INSERT INTO #TempFinalEffectivePercentageDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, PickUpOrder, TypeId, TrackingKey, Tag, IsExcludefromTransfer, GPPartnerReceivingCarry)    
Select DISTINCT D.UnderlyingEntityID, ISNULL(D.LineTypeID, -1) LineTypeID,  Y.PartnerNumber, Y.CommitmentPercent, D.Quarter, 'Cost without Transfer Adj %', 2,  D.TypeId, D.TrackingKey, D.Tag, D.IsExcludefromTransfer , Y.GPPartnerReceivingCarry   
From     
#FinalCostPercentage Y INNER JOIN #TempCostPercentageMinQuarter M     
ON Y.DealId = M.DealId AND Y.Quarter = M.Quarter    
AND Y.TypeId = M.TypeID AND Y.TrackingKey = M.TrackingKey AND Y.Tag = M.Tag    
INNER JOIN #TempDatedEntities D ON D.UnderlyingEntityID = M.DealId AND D.TypeID = M.TypeID AND D.Tag = M.Tag    
AND D.TrackingKey = M.TrackingKey AND M.Preference < D.Preference   
WHERE D.IsExcludefromTransfer=1 
END
ELSE
BEGIN
INSERT INTO #TempFinalEffectivePercentageDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, PickUpOrder, TypeId, TrackingKey, Tag, IsExcludefromTransfer, GPPartnerReceivingCarry)    
Select DISTINCT D.UnderlyingEntityID, ISNULL(D.LineTypeID, -1) LineTypeID,  Y.PartnerNumber, Y.CommitmentPercent, D.Quarter, 'Cost without Transfer Adj %', 2,  D.TypeId, D.TrackingKey, D.Tag, D.IsExcludefromTransfer, Y.GPPartnerReceivingCarry    
From     
#FinalCostPercentage Y INNER JOIN #TempCostPercentageMinQuarter M     
ON Y.DealId = M.DealId AND Y.Quarter = M.Quarter    
AND Y.TypeId = M.TypeID AND Y.TrackingKey = M.TrackingKey AND Y.Tag = M.Tag    
 INNER JOIN #TempDatedEntities D ON D.UnderlyingEntityID = M.DealId AND D.TypeID = M.TypeID AND D.Tag = M.Tag    
 AND D.TrackingKey = M.TrackingKey AND M.Quarter < D.Quarter
 WHERE D.IsExcludefromTransfer=1    
END
    
-------------------------------------------------PICK UP THE PREFERED ALLOCATION METHOD -------------------------------------------------------------------------    
---#TempUnderlyingsPickUpOrderDated - All dated entities data except for entities which have non of the partners not involved in transfer.------------------------    
INSERT INTO #TempUnderlyingsPickUpOrderDated(InvestmentID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, PickUpOrder, IsExcludefromTransfer)    
SELECT InvestmentID, ISNULL(LineTypeID, -1), Quarter, Typeid, Trackingkey, Tag, MIN(PickUpOrder) PickUpOrder, isexcludefromTransfer    
FROM #TempFinalEffectivePercentageDated     
GROUP BY InvestmentID, ISNULL(LineTypeID, -1), Quarter, TypeId, TrackingKey, Tag, isexcludefromTransfer    
    
    
SELECT DISTINCT D.UnderlyingEntityID InvestmentID, ISNULL(D.LineTypeID, -1) LineTypeID, D.Quarter, D.Typeid, D.Trackingkey, D.Tag, D.IsExcludefromTransfer      
INTO #TempDatedEntitiesNotransfer    
FROM #TempDatedEntities D INNER JOIN #TempCostPercentageMinQuarter M    
ON D.UnderlyingEntityID = M.DealId     
AND D.TypeId = M.TypeID AND D.TrackingKey = M.TrackingKey AND D.Tag = M.Tag 
AND CASE WHEN @IsDatedTransfersConfigured  = 'C' AND M.Preference <  D.Preference THEN 1
         WHEN @IsDatedTransfersConfigured <> 'C' AND M.Quarter < D.Quarter THEN 1
               ELSE 0 END = 1
LEFT JOIN (SELECT DISTINCT InvestmentID, ISNULL(LineTypeID, -1) LineTypeID, Quarter, TypeID, TrackingKey, Tag, isexcludefromTransfer    
    FROM #TempUnderlyingsPickUpOrderDated WHERE PickupOrder <> 3 ) U    
ON D.UnderlyingEntityID = U.InvestmentID AND D.Quarter = U.Quarter AND D.TypeID = U.TypeID AND D.TrackingKey = U.TrackingKey AND D.Tag = U.Tag    
AND ISNULL(D.LinetypeID, -1) = U.LineTypeID AND D.IsExcludefromTransfer = U.IsExcludefromTransfer    
LEFT JOIN #tmpPartVQuarters QD ON QD.Quarter = D.Quarter
WHERE d.IsExcludefromTransfer = 0 AND QD.Quarter IS NULL AND
U.InvestmentID IS NULL    
    
--If ALL the partners with custom percentage have no transfers it wont come up in transfers % table and hence the dates lines will go through prorata.     
--To prevent this we check those dated lines and if they have custom % defined at a lesser quarter we use that and delete the pro rata percents.     
--In other cases when there are some partners which do not involve in transfers but others do then we pick up the percent for these remaining partners    
--based on the pickup order. if yearly prorata else custom %.  Since we pick up minimum at top and and check for the lesser quarters below we have to delete     
--the ones marked with prorata once we find that there is a minimum quarter for the underlying combination.     
    
SELECT InvestmentID, LineTypeID, Quarter, TypeID, TrackingKey, Tag INTO #TempYearlyDatedToBeDeleted    
FROM #TempDatedEntitiesNotransfer     
    
DELETE M    
FROM #TempUnderlyingsPickUpOrderDated M INNER JOIN #TempDatedEntitiesNotransfer D    
ON D.InvestmentID = M.InvestmentID     
AND D.TypeId = M.TypeID AND D.TrackingKey = M.TrackingKey AND D.Tag = M.Tag AND ISNULL(D.LineTypeID, -1) = ISNULL(M.LineTypeID, -1)    
AND M.IsExcludefromTransfer = D.IsExcludefromTransfer    
WHERE M.PickupOrder = 3    
    
-------------------Insert percentage for allocation type where no partners were involved in transfer but has % supplied for quarter less than the actual quarter----------------    
CREATE TABLE #TempDatedEntitiesNotransferPickUpQuarter(InvestmentID INT, LineTypeID INT, PickUpQuarter VARCHAR(10), ActualQuarter  VARCHAR(10), TypeId INT, TrackingKey VARCHAR(5000), Tag  VARCHAR(5000), IsExcludefromTransfer BIT)    

IF(@LocalMode != 4)
BEGIN
IF (@AllocationTypeName = 'PE Book Allocation' AND @IsDatedTransfersConfigured = 'C')    
BEGIN    
INSERT INTO #TempDatedEntitiesNotransferPickUpQuarter(InvestmentID, LineTypeID, PickUpQuarter, ActualQuarter, TypeId, TrackingKey, Tag, IsExcludefromTransfer)    
Select DISTINCT P.InvestmentID, ISNULL(P.LineTypeID, -1) LineTypeID, MIN(Y.Quarter) PickUpQuarter, P.Quarter ActualQuarter, Y.TypeId, Y.TrackingKey, Y.Tag, P.IsExcludefromTransfer    
From     
#FinalCostPercentage Y INNER JOIN #TempDatedEntitiesNotransfer P     
ON P.InvestmentID = Y.DealId AND  Y.TypeId = P.TypeID    
 AND Y.TrackingKey = P.TrackingKey AND Y.Tag = P.Tag AND CAST(REPLACE(REPLACE(Y.Quarter, 'M', ''), 'Q', '') AS INT) <= CAST(REPLACE(REPLACE(P.Quarter, 'M', ''), 'Q', '') AS INT)    
GROUP BY P.InvestmentID, ISNULL(P.LineTypeID, -1),  P.Quarter , Y.TypeId, Y.TrackingKey, Y.Tag, P.IsExcludefromTransfer    
    
INSERT INTO #TempFinalEffectivePercentageDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, PickUpOrder,TypeId,     
TrackingKey, Tag, IsExcludefromTransfer, GPPartnerReceivingCarry)    
Select DISTINCT M.InvestmentID, ISNULL(M.LineTypeID, -1) LineTypeID, Y.PartnerNumber, Y.CommitmentPercent, M.ActualQuarter Quarter,    
'CostAdjustedDatedTransfer', 2, M.TypeId, M.TrackingKey, M.Tag, M.IsExcludefromTransfer , Y.GPPartnerReceivingCarry   
From     
#FinalCostPercentage Y INNER JOIN #TempDatedEntitiesNotransferPickUpQuarter M     
ON Y.DealId = M.InvestmentID AND Y.Quarter = M.PickUpQuarter    
AND Y.TypeId = M.TypeID AND Y.TrackingKey = M.TrackingKey AND Y.Tag = M.Tag     
END    
ELSE    
BEGIN    
INSERT INTO #TempDatedEntitiesNotransferPickUpQuarter(InvestmentID, LineTypeID, PickUpQuarter, ActualQuarter, TypeId, TrackingKey, Tag, IsExcludefromTransfer)    
Select DISTINCT P.InvestmentID, ISNULL(P.LineTypeID, -1) LineTypeID, MAX(Y.Quarter) PickUpQuarter, P.Quarter ActualQuarter, Y.TypeId, Y.TrackingKey, Y.Tag, P.IsExcludefromTransfer    
From     
#FinalCostPercentage Y INNER JOIN #TempDatedEntitiesNotransfer P     
ON P.InvestmentID = Y.DealId AND  Y.TypeId = P.TypeID    
 AND Y.TrackingKey = P.TrackingKey AND Y.Tag = P.Tag AND Y.Quarter <= P.Quarter    
GROUP BY P.InvestmentID, ISNULL(P.LineTypeID, -1),  P.Quarter , Y.TypeId, Y.TrackingKey, Y.Tag, P.IsExcludefromTransfer    
    
INSERT INTO #TempFinalEffectivePercentageDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, PickUpOrder,TypeId,     
TrackingKey, Tag, IsExcludefromTransfer, GPPartnerReceivingCarry)    
Select DISTINCT M.InvestmentID, ISNULL(M.LineTypeID, -1) LineTypeID, Y.PartnerNumber, Y.CommitmentPercent, M.ActualQuarter Quarter,    
'CostAdjustedDatedTransfer', 2, M.TypeId, M.TrackingKey, M.Tag, M.IsExcludefromTransfer , Y.GPPartnerReceivingCarry   
From     
#FinalCostPercentage Y INNER JOIN #TempDatedEntitiesNotransferPickUpQuarter M     
ON Y.DealId = M.InvestmentID AND Y.Quarter = M.PickUpQuarter    
AND Y.TypeId = M.TypeID AND Y.TrackingKey = M.TrackingKey AND Y.Tag = M.Tag     
END    
    
-----------------------------------------Insert missing partners for transfer adjusted cost percentage--------------------------------------------------------    
    
Select distinct P.InvestmentID, ISNULL(P.LineTypeID, -1) LineTypeID, P.Quarter, Y.PartnerNumber, Y.CommitmentPercent,Y.TypeId, Y.TrackingKey, Y.Tag, D.IsExcludefromTransfer    
INTO #TempCost     
From     
#FinalCostPercentage Y INNER JOIN #TempCostPercentageMinQuarter M     
ON Y.DealId = M.DealId AND Y.Quarter = M.Quarter    
AND Y.TypeId = M.TypeID AND Y.TrackingKey = M.TrackingKey AND Y.Tag = M.Tag    
 INNER JOIN #TempUnderlyingsPickUpOrderDated P     
ON P.InvestmentID = Y.DealId AND P.PickupOrder = 2 AND Y.TypeId = P.TypeID    
 AND Y.TrackingKey = P.TrackingKey AND Y.Tag = P.Tag    
 INNER JOIN #TempDatedEntities D ON D.UnderlyingEntityID = P.InvestmentID AND D.TypeID = P.TypeID AND D.Quarter = P.Quarter AND D.Tag = P.Tag    
 AND D.TrackingKey = P.TrackingKey    
    
    
INSERT INTO #TempFinalEffectivePercentageDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, PickUpOrder,TypeId,     
TrackingKey, Tag, IsExcludefromTransfer, GPPartnerReceivingCarry)    
Select Y.InvestmentID, ISNULL(Y.LineTypeID, -1), Y.PartnerNumber,  ISNULL(Y.CommitmentPercent,0), Y.Quarter,     
CASE WHEN Y.IsExcludefromTransfer = 1 THEN 'Cost without Transfer Adj %' ELSE 'CostAdjustedDatedTransfer' END, 2,Y.TypeId,     
Y.TrackingKey, Y.Tag, CASE WHEN Y.IsExcludefromTransfer = 1 THEN 1 ELSE 0 END, F.GPPartnerReceivingCarry    
FROM #TempFinalEffectivePercentageDated(NOLOCK) F RIGHT JOIN #TempCost Y ON F.PartnerNumber = Y.PartnerNumber    
AND F.InvestmentID = Y.InvestmentID AND F.Quarter = Y.Quarter  AND F.TypeId = Y.TypeId    
AND F.TrackingKey = Y.TrackingKey AND F.Tag = Y.Tag AND ISNULL(F.LineTypeID, -1) = ISNULL(Y.LineTypeID, -1)    
AND F.IsExcludefromTransfer = Y.IsExcludefromTransfer    
WHERE F.PartnerNumber IS NULL     
    
--------------------------------------------------------------------------------------------------------------------------------------------    
---#TempUnderlyingsPickUpOrderDated - Addding dated entities data which have none of the partners involved in transfer.------------------------    
INSERT INTO #TempUnderlyingsPickUpOrderDated(InvestmentID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, PickUpOrder, IsExcludefromTransfer)    
Select InvestmentID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, 2, IsExcludefromTransfer    
FROM #TempDatedEntitiesNotransfer    
    
    
--------------------------------------------------------------------------------------------------------------------------------------    
INSERT INTO #TempUnderlyingsPickUpOrderDated(InvestmentID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, PickUpOrder, IsExcludefromTransfer)    
SELECT DISTINCT D.UnderlyingEntityID, ISNULL(D.LineTypeID, -1), D.Quarter, D.Typeid, D.Trackingkey, D.Tag, 3 PickUpOrder , d.IsExcludefromTransfer    
FROM #TempDatedEntities D LEFT JOIN     
(SELECT DISTINCT InvestmentID, ISNULL(LineTypeID, -1) LineTypeID, Quarter, TypeID, TrackingKey, Tag, IsExcludefromTransfer FROM #TempUnderlyingsPickUpOrderDated ) U    
ON D.UnderlyingEntityID = U.InvestmentID AND D.Quarter = U.Quarter AND D.TypeID = U.TypeID AND D.TrackingKey = U.TrackingKey AND D.Tag = U.Tag    
AND ISNULL(D.LinetypeID, -1) = U.LineTypeID and d.IsExcludefromTransfer = u.IsExcludefromTransfer    
WHERE U.InvestmentID IS NULL    
    
    
-----------------------------------------Insert missing partners for transfer adjusted yearly percentage--------------------------------------------------------    
Select P.InvestmentID, ISNULL(P.LineTypeID,-1) LineTypeID, P.Quarter, Y.PartnerNumber, Y.ProRataEffOwnPercent, P.TypeID, P.TrackingKey, P.Tag, IsExcludefromTransfer     
INTO #TempYearly     
From     
Yearly_Snapshot Y(NOLOCK) CROSS JOIN #TempUnderlyingsPickUpOrderDated P     
WHERE WorkflowID = @YearlyWorkflowID AND P.PickupOrder = 3    
    
    
IF NOT EXISTS(SELECT TOP 1 1 From #TempYearly) AND @LocalIsPEModel = 0    
BEGIN    
    
 Select P.InvestmentID, ISNULL(P.LineTypeID,-1) LineTypeID, P.Quarter, Y.PartnerNumber, P.TypeID, P.TrackingKey, P.Tag, IsExcludefromTransfer     
 INTO #TempPartner1     
 From     
 #EntityPartners Y(NOLOCK) CROSS JOIN #TempUnderlyingsPickUpOrderDated P     
 WHERE P.PickupOrder = 3    
    
 IF EXISTS(Select TOP 1 1    
 FROM #TempFinalEffectivePercentageDated F RIGHT JOIN #TempPartner1 Y ON F.PartnerNumber = Y.PartnerNumber    
 AND F.InvestmentID = Y.InvestmentID AND F.Quarter = Y.Quarter AND F.TypeId = Y.TypeID    
 AND F.TrackingKey = Y.TrackingKey AND F.Tag = Y.Tag AND ISNULL(F.LineTypeID, -1) = ISNULL(Y.LineTypeID, -1) AND F.IsExcludefromTransfer = Y.IsExcludefromTransfer    
 WHERE F.PartnerNumber IS NULL)    
 BEGIN     
    
  INSERT INTO AllocationRunErrors(RunID, EntityID, ErrorMessage, LogID, ErrororWarning)      
  SELECT @LocalRunID, @LocalEntityID, 'Please update yearly prorata percentages', @LogID, 'Error'    
    
  Update AllocationRun     
  SET RunStatus = 'FAIL',
  RunEndDate = GETDATE() 
  WHERE RunID = @LocalRunID    
    
  SET @EndDate = GETDATE()    
  EXEC [dbo].[uspUpdateAllocationLog] @LogID, @EndDate     
    
  RETURN    
 END    
 DROP TABLE #TempPartner1    
END    
    
INSERT INTO #TempFinalEffectivePercentageDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, PickUpOrder,TypeId,     
TrackingKey, Tag, IsExcludefromTransfer, GPPartnerReceivingCarry)    
Select Y.InvestmentID, ISNULL(Y.LineTypeID, -1), Y.PartnerNumber,  ISNULL(Y.ProRataEffOwnPercent,0), Y.Quarter,     
CASE WHEN Y.IsExcludefromTransfer = 1 THEN 'Cost without Transfer Adj %' ELSE 'ProRata' END, 3, Y.TypeID, Y.TrackingKey,     
Y.Tag, Y.IsExcludefromTransfer  , F.GPPartnerReceivingCarry  
FROM #TempFinalEffectivePercentageDated F RIGHT JOIN #TempYearly Y ON F.PartnerNumber = Y.PartnerNumber    
AND F.InvestmentID = Y.InvestmentID AND F.Quarter = Y.Quarter AND F.TypeId = Y.TypeID    
AND F.TrackingKey = Y.TrackingKey AND F.Tag = Y.Tag AND ISNULL(F.LineTypeID, -1) = ISNULL(Y.LineTypeID, -1) AND F.IsExcludefromTransfer = Y.IsExcludefromTransfer    
WHERE F.PartnerNumber IS NULL     
    
INSERT INTO #TempUnderlyingsPickUpOrderDated(InvestmentID, LineTypeID, Quarter, TypeID, TrackingKey, Tag, PickUpOrder, IsExcludefromTransfer)    
Select F.InvestmentID, F.LineTypeID, F.Quarter, F.TypeID, F.TrackingKey, F.Tag, MIN(F.PickUpOrder), F.IsExcludefromTransfer    
FROM #TempUnderlyingsPickUpOrderDated P RIGHT JOIN #TempFinalEffectivePercentageDated F    
ON P.InvestmentID = F.InvestmentID AND P.Quarter = F.Quarter AND P.TypeID = F.TypeId AND P.TrackingKey = F.TrackingKey AND     
P.Tag = F.Tag AND P.IsExcludefromTransfer = F.IsExcludefromTransfer    
WHERE P.InvestmentID IS NULL    
GROUP BY  F.InvestmentID, F.LineTypeID, F.Quarter, F.TypeID, F.TrackingKey, F.Tag, F.PickUpOrder, F.IsExcludefromTransfer     
    
----------------------------Loading Non dated PFIC and K1 Lines Percentages----------------------------------    
    
--------------------------------Transfer affected Cost Percentage----------------------------------------------    
INSERT INTO #TempFinalEffectivePercentageNonDated(InvestmentID, LineTypeID, TypeId, PartnerNumber, EffPercentage, AllocationType, TrackingKey, Tag, Quarter, IsExcludefromTransfer)    
SELECT DISTINCT L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1), L.TypeID, T.Partnernumber, SUM(ISNULL(T.EffectivePercent,0)), 'Cost', L.TrackingKey, L.Tag, 'Q0', 0    
FROM     
#TempNonDatedEntities L     
Inner JOIN #TempTransfersAdjCostDefaultPercentage T    
ON L.UnderlyingEntityID= T.InvestmentID AND L.TypeID = T.TypeID    
AND L.TrackingKey = T.TrackingKey AND L.Tag = T.Tag    
WHERE ISNULL(L.IsExcludefromTransfer,0) = 0    
Group By L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1),T.PartnerNumber, L.TypeID, L.TrackingKey, L.Tag    
    
-----------------------------------------Insert missing partners for transfer adjusted cost percentage--------------------------------------------------------    
    
INSERT INTO #TempSelectedNonDatedLines(InvestmentID, LineTypeID, TypeID, TrackingKey, Tag)    
SELECT DISTINCT Investmentid, ISNULL(LineTypeID, -1), TypeId, TrackingKey, Tag FROM #TempFinalEffectivePercentageNonDated    
    
INSERT INTO #TempFinalEffectivePercentageNonDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, Quarter, AllocationType, TypeId, TrackingKey, Tag, IsExcludefromTransfer, GPPartnerReceivingCarry)    
Select DISTINCT Y.DealId, ISNULL(S.LineTypeID, -1), Y.PartnerNumber,  ISNULL(Y.CommitmentPercent,0), 'Q0', 'Cost', Y.TypeId, Y.TrackingKey, Y.Tag, 0, Y.GPPartnerReceivingCarry    
FROM #TempSelectedNonDatedLines S INNER JOIN  #FinalCostPercentage Y      
ON S.InvestmentID = Y.DealId AND S.TypeID = Y.TypeId    
AND S.TrackingKey = Y.TrackingKey AND S.Tag = Y.Tag    
INNER JOIN #TempCostPercentageMinQuarter M     
ON Y.DealId = M.DealId AND Y.Quarter = M.Quarter AND Y.TypeId = M.TypeID    
AND Y.TrackingKey = M.TrackingKey AND Y.Tag = M.Tag    
LEFT JOIN #TempFinalEffectivePercentageNonDated F     
ON Y.DealID = F.InvestmentID AND Y.Partnernumber = F.Partnernumber AND Y.TypeId = F.TypeId    
AND Y.TrackingKey = F.TrackingKey AND Y.Tag = F.Tag AND ISNULL(S.LineTypeID, -1) = ISNULL(F.LineTypeID, -1)    
WHERE F.PartnerNumber IS NULL     
    
DELETE D    
FROM     
#TempNonDatedEntities D INNER JOIN #TempSelectedNonDatedLines F    
ON D.UnderlyingEntityID = F.InvestmentID AND D.TypeID = F.TypeID    
AND D.TrackingKey = F.TrackingKey AND D.Tag = F.Tag AND ISNULL(D.LineTypeID, -1) = ISNULL(F.LineTypeID, -1)    
AND D.IsExcludefromTransfer = 0   

END
----------------------------------------Cost Percentage-------------------------------------------------------    
    
INSERT INTO #TempFinalEffectivePercentageNonDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, AllocationType, Quarter,TypeId, TrackingKey, Tag,    
  IsExcludefromTransfer,[704cAllocationTypeId],[704cPercentageType], GPPartnerReceivingCarry)     
SELECT DISTINCT L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1), C.Partnernumber, ISNULL(C.CommitmentPercent,0),     
CASE WHEN L.IsExcludefromTransfer = 1 THEN 'Cost without Transfer Adj %' ELSE 'Cost' END, 'Q0', C.TypeId, C.TrackingKey, C.Tag,    
 CASE WHEN L.IsExcludefromTransfer = 1 THEN 1 ELSE 0 END    ,[704cAllocationTypeId],[704cPercentageType], C.GPPartnerReceivingCarry
FROM     
#TempNonDatedEntities L     
INNER JOIN #TempCostPercentageMinQuarter M     
ON L.UnderlyingEntityID = M.DealId AND L.TypeID = M.TypeID    
AND L.TrackingKey = M.TrackingKey AND L.Tag = M.Tag    
Inner Join #FinalCostPercentage C     
ON L.UnderlyingEntityID = C.DealId AND C.Quarter = M.Quarter    
AND L.TypeID = C.TypeId AND L.TrackingKey = C.TrackingKey AND L.Tag = C.Tag    
    
DELETE D    
FROM     
#TempNonDatedEntities D INNER JOIN #TempFinalEffectivePercentageNonDated F    
ON D.UnderlyingEntityID = F.InvestmentID AND D.TypeID = F.TypeId    
AND D.TrackingKey = F.TrackingKey AND D.Tag = F.Tag    
AND ISNULL(D.LineTypeID, -1) = ISNULL(F.LineTypeID, -1)    
    
----------------------------------------Yearly-------------------------------------------------------    
    
INSERT INTO #TempFinalEffectivePercentageNonDated(InvestmentID, LineTypeID, PartnerNumber, EffPercentage, AllocationType, Quarter, TypeId, TrackingKey, Tag,    
 IsExcludefromTransfer,StateID)    
SELECT L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1), D.Partnernumber, SUM(ISNULL(D.EffectivePercent,0)),     
CASE WHEN L.IsExcludefromTransfer = 1 THEN 'Cost without Transfer Adj %' ELSE 'ProRata' END, 'Q0', L.TypeID, L.TrackingKey, L.Tag,    
 CASE WHEN L.IsExcludefromTransfer = 1 THEN 1 ELSE 0 END,stateid    
FROM     
#TempNonDatedEntities L INNER JOIN TransfersAdjDefaultPercentage D  (NOLOCK)    
 ON D.RunID = @LocalRunID AND D.ClientID = @LocalClientID     
 GROUP BY L.UnderlyingEntityID, ISNULL(L.LineTypeID, -1), D.Partnernumber, L.TypeID, L.TrackingKey, L.Tag,  L.stateid,    
 CASE WHEN L.IsExcludefromTransfer = 1 THEN 1 ELSE 0 END, CASE WHEN L.IsExcludefromTransfer = 1 THEN 'Cost without Transfer Adj %' ELSE 'ProRata' END     
 
 ----------------------------------------------------------------------------------------------------------------------    
------------------------------------------------Plugging Logic--------------------------------------------------------    
    
   Create Table #TempEffectivePercentagePlug(    
   InvestmentID INT,      
   ExcessAllocationPercentage float,    
   BigPartner varchar(50),    
   Quarter Varchar(50),    
   TypeID INT,     
   TrackingKey Varchar(5000),     
   Tag Varchar(5000),    
   LinetypeID INT,    
   AllocationType Varchar(255),    
   LineId INT,    
   IsExcludefromTransfer BIT,
   [704cAllocationTypeID] INT,
   [704cPercentageType] VARCHAR(50)
   )    
    
   Create Table #TempEffectivePercentageMaxCommitment(    
   InvestmentID INT,    
   BigPartner varchar(50),    
   Quarter Varchar(50),    
   TypeID INT,     
   TrackingKey Varchar(5000),     
   Tag Varchar(5000),    
   LinetypeID INT,    
   AllocationType Varchar(255),       
   InvestmentPercentage float  ,    
   LineId INT,    
   IsExcludefromTransfer BIT,
   [704cAllocationTypeID] INT,
   [704cPercentageType] VARCHAR(50)
   )    
  
    Select InvestmentID ,    
 PartnerNumber ,    
 Round(EffPercentage,8) AS EffPercentage,    
 AllocationType,    
 Quarter,    
 PickUpOrder,    
 TypeId ,    
 TrackingKey ,    
 Tag ,    
 LineTypeID ,      
 IsExcludefromTransfer     
 INTO  #TempFinalEffectivePercentageDatedRounded  
 FROM  #TempFinalEffectivePercentageDated  
  
   Insert Into #TempEffectivePercentagePlug(InvestmentID , LinetypeID, AllocationType, Quarter, TypeID, TrackingKey, Tag, ExcessAllocationPercentage,     
   IsExcludefromTransfer)    
   Select L.InvestmentID, ISNULL(L.LineTypeID, -1),  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),      
   ROUND(Round(1.00000000,8) - Round(SUM(EffPercentage),8),8), IsExcludefromTransfer    
   From #TempFinalEffectivePercentageDatedRounded L    
 LEFT JOIN #DefaultAllocationRuleSetup E ON L.TypeId=E.RuleID  AND  
 E.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)    
 LEFT JOIN ENU_ALLOCATIONPERCENTAGETYPE EA (NOLOCK) ON E.AllocationPercentageTypeID=EA.AllocationPercentageTypeID  
 AND EA.AllocationPercentageType  IN ('Allocate > 100%','Allocate < 100%')  
 WHERE E.RuleID IS NULL  
   Group By L.InvestmentID, ISNULL(L.LineTypeID, -1),  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),    
   IsExcludefromTransfer    
   Having Round(SUM(EffPercentage),8) > 0    
    
   Insert Into #TempEffectivePercentageMaxCommitment(InvestmentID , LinetypeID, AllocationType, Quarter, TypeID, TrackingKey, Tag, InvestmentPercentage,    
   IsExcludefromTransfer)    
   Select L.InvestmentID, ISNULL(L.LineTypeID, -1),  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),      
   Max(EffPercentage), IsExcludefromTransfer    
   From #TempFinalEffectivePercentageDatedRounded L    
   Group By L.InvestmentID, ISNULL(L.LineTypeID, -1),  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),    
   IsExcludefromTransfer    
    
   Update T       
   Set T.BigPartner = S.PartnerNumber    
   From #TempEffectivePercentagePlug T Inner Join #TempFinalEffectivePercentageDatedRounded S       
   ON T.InvestmentID = S.InvestmentID AND T.AllocationType = S.AllocationType AND T.Quarter = S.Quarter    
   AND T.TypeID = S.TypeId AND ISNULL(T.TrackingKey, '') = ISNULL(S.TrackingKey, '') AND ISNULL(T.Tag, '') = ISNULL(S.Tag, '')     
   AND ISNULL(T.LinetypeID, -1) = ISNULL(S.LineTypeID, -1) AND T.IsExcludefromTransfer = S.IsExcludefromTransfer    
   Inner Join #TempEffectivePercentageMaxCommitment C    
   ON T.InvestmentID = C.InvestmentID AND T.AllocationType = C.AllocationType AND T.Quarter = C.Quarter    
   AND T.TypeID = C.TypeId AND T.TrackingKey = C.TrackingKey AND T.Tag = C.Tag   AND ISNULL(T.LinetypeID, -1) = ISNULL(C.LineTypeID, -1)    
   AND C.InvestmentPercentage = S.EffPercentage AND T.IsExcludefromTransfer = C.IsExcludefromTransfer    
   Where T.ExcessAllocationPercentage <> 0    
    
   Update T       
   Set T.EffPercentage = Round(T.EffPercentage + S.ExcessAllocationPercentage,8)    
   From #TempFinalEffectivePercentageDatedRounded T Inner Join  #TempEffectivePercentagePlug S     
   ON T.InvestmentID = S.InvestmentID AND T.AllocationType = S.AllocationType AND T.Quarter = S.Quarter    
   AND T.TypeID = S.TypeId AND ISNULL(T.TrackingKey, '') = ISNULL(S.TrackingKey, '') AND ISNULL(T.Tag, '') = ISNULL(S.Tag, '')     
   AND ISNULL(T.LinetypeID, -1) = ISNULL(S.LineTypeID, -1) AND T.PartnerNumber = S.BigPartner AND  T.IsExcludefromTransfer = S.IsExcludefromTransfer    
    
   DELETE FROM #TempEffectivePercentagePlug    
   DELETE FROM #TempEffectivePercentageMaxCommitment    
  
   Select InvestmentID ,    
  PartnerNumber ,    
  Round(EffPercentage,8) AS EffPercentage,    
  AllocationType,    
  Quarter,    
  PickUpOrder,    
  TypeId ,    
  TrackingKey ,    
  Tag ,    
  LineTypeID ,    
  LineID ,    
  IsExcludefromTransfer ,
  [704cAllocationTypeId],[704cPercentageType], GPPartnerReceivingCarry
  INTO  #TempFinalEffectivePercentageNonDatedRounded  
  FROM  #TempFinalEffectivePercentageNonDated  
    
   Insert Into #TempEffectivePercentagePlug(InvestmentID, LinetypeID, AllocationType, Quarter, TypeID, TrackingKey, Tag, ExcessAllocationPercentage,    
   LineID, IsExcludefromTransfer)    
   Select DISTINCT L.InvestmentID, ISNULL(L.LineTypeID, -1),  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),      
   ROUND(Round(1.00000000,8) - ROUND(SUM(EffPercentage),8),8),L.LineId, IsExcludefromTransfer    
   From #TempFinalEffectivePercentageNonDatedRounded L    
 LEFT JOIN #DefaultAllocationRuleSetup E ON L.TypeId=E.RuleID  AND  
 E.TransactionID IN (@DefaultAllocationRuleTransactionID,@GlobalDefaultAllocationRuleTransactionID)    
 LEFT JOIN ENU_ALLOCATIONPERCENTAGETYPE EA (NOLOCK) ON E.AllocationPercentageTypeID=EA.AllocationPercentageTypeID  
 AND EA.AllocationPercentageType  IN ('Allocate > 100%','Allocate < 100%')  
 WHERE E.RuleID IS NULL  AND [704cPercentageType] =''
   Group By L.InvestmentID, ISNULL(L.LineTypeID, -1), L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),L.LineId,    
   IsExcludefromTransfer    
   Having Round(SUM(EffPercentage),8) > 0
   
   INSERT INTO #TempEffectivePercentagePlug(InvestmentID, LineTypeID, AllocationType, [Quarter], TypeID, TrackingKey, Tag, ExcessAllocationPercentage,
   LineID, IsExcludefromTransfer, [704cAllocationTypeID], [704cPercentageType])
   SELECT DISTINCT L.InvestmentID, ISNULL(L.LineTypeID, -1), L.AllocationType, L.[Quarter], L.TypeID, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),
   ROUND(ROUND(1.00000000, 8) - ROUND(SUM(EffPercentage), 8), 8), L.LineID, IsExcludefromTransfer, ISNULL([704cAllocationTypeID], 0), ISNULL([704cPercentageType], '')
   FROM #TempFinalEffectivePercentageNonDatedRounded L    
   LEFT JOIN #DefaultAllocationRuleSetup E ON L.TypeID = E.RuleID AND E.TransactionID IN (@DefaultAllocationRuleTransactionID, @GlobalDefaultAllocationRuleTransactionID)
   LEFT JOIN ENU_AllocationPercentageType EA (NOLOCK) ON E.AllocationPercentageTypeID = EA.AllocationPercentageTypeID AND EA.AllocationPercentageType = 'Allocate 100%'
   WHERE ISNULL([704cPercentageType], '') <> ''
   GROUP BY L.InvestmentID, ISNULL(L.LineTypeID, -1), L.AllocationType, L.[Quarter], L.[TypeID], ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),
   L.LineID, IsExcludefromTransfer, ISNULL([704cAllocationTypeID], 0), ISNULL([704cPercentageType], '')
   HAVING ROUND(SUM(EffPercentage), 8) > 0
    
   Insert Into #TempEffectivePercentageMaxCommitment(InvestmentID , LinetypeID, AllocationType, Quarter, TypeID, TrackingKey, Tag, InvestmentPercentage,    
   LineId, IsExcludefromTransfer, [704cAllocationTypeID], [704cPercentageType])    
   Select L.InvestmentID, ISNULL(L.LineTypeID, -1), L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),      
   Max(EffPercentage) ,L.LineId, IsExcludefromTransfer, ISNULL([704cAllocationTypeID], 0), ISNULL([704cPercentageType], '')
   From #TempFinalEffectivePercentageNonDatedRounded L
   Group By L.InvestmentID, ISNULL(L.LineTypeID, -1), L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),L.LineId,    
   IsExcludefromTransfer, ISNULL([704cAllocationTypeID], 0), ISNULL([704cPercentageType], '')
    
   Update T       
   Set T.BigPartner = S.PartnerNumber    
   From #TempEffectivePercentagePlug T Inner Join #TempFinalEffectivePercentageNonDatedRounded S       
   ON T.InvestmentID = S.InvestmentID AND T.AllocationType = S.AllocationType AND T.Quarter = S.Quarter    
   AND T.TypeID = S.TypeId AND ISNULL(T.TrackingKey, '') = ISNULL(S.TrackingKey, '') AND ISNULL(T.Tag, '') = ISNULL(S.Tag, '')     
   AND ISNULL(T.LinetypeID, -1) = ISNULL(S.LineTypeID, -1)  AND ISNULL(T.LineID, -1) = ISNULL(S.LineID, -1)    
   AND T.IsExcludefromTransfer = S.IsExcludefromTransfer
   AND ISNULL(T.[704cAllocationTypeID], 0) = ISNULL(S.[704cAllocationTypeID], 0) AND ISNULL(T.[704cPercentageType], '') = ISNULL(S.[704cPercentageType], '')
   Inner Join #TempEffectivePercentageMaxCommitment C    
   ON T.InvestmentID = C.InvestmentID AND T.AllocationType = C.AllocationType AND T.Quarter = C.Quarter    
   AND T.TypeID = C.TypeId AND T.TrackingKey = C.TrackingKey AND T.Tag = C.Tag AND  ISNULL(T.LinetypeID, -1) = ISNULL(C.LineTypeID, -1) AND ISNULL(T.LineID, -1) = ISNULL(C.LineID, -1)    
   AND C.InvestmentPercentage = S.EffPercentage AND T.IsExcludefromTransfer = C.IsExcludefromTransfer
   AND ISNULL(T.[704cAllocationTypeID], 0) = ISNULL(C.[704cAllocationTypeID], 0) AND ISNULL(T.[704cPercentageType], '') = ISNULL(C.[704cPercentageType], '')
   Where T.ExcessAllocationPercentage <> 0
    
   Update T       
   Set T.EffPercentage = Round(T.EffPercentage + S.ExcessAllocationPercentage,8)    
   From #TempFinalEffectivePercentageNonDatedRounded T Inner Join  #TempEffectivePercentagePlug S     
   ON T.InvestmentID = S.InvestmentID AND T.AllocationType = S.AllocationType AND T.Quarter = S.Quarter    
   AND T.TypeID = S.TypeId AND ISNULL(T.TrackingKey, '') = ISNULL(S.TrackingKey, '') AND ISNULL(T.Tag, '') = ISNULL(S.Tag, '')     
   AND ISNULL(T.LinetypeID, -1) = ISNULL(S.LineTypeID, -1) AND ISNULL(T.LineId, -1) = ISNULL(S.LineId, -1)     
   AND T.PartnerNumber = S.BigPartner AND T.IsExcludefromTransfer = S.IsExcludefromTransfer
   AND ISNULL(T.[704cAllocationTypeID], 0) = ISNULL(S.[704cAllocationTypeID], 0) AND ISNULL(T.[704cPercentageType], '') = ISNULL(S.[704cPercentageType], '')
    
-----------------------------------------------------------------------------------------------------------------------    
    
------------------------Update TypeID For Non Cost types which are now using cost %    
    
Update D    
SET D.TypeId = C.TypeID, D.AllocationType = CASE WHEN D.AllocationType = 'COST' THEN 'DEFAULT'     
             WHEN D.AllocationType = 'CostAdjustedDatedTransfer' THEN 'DefaultAdjustedDatedTransfer'    
             WHEN D.AllocationType = 'Cost without Transfer Adj %'     
              THEN 'Default without Transfer Adj %'    
              ELSE 'ProRata' END    
FROM #TempFinalEffectivePercentageNonDatedRounded D INNER JOIN #TempNonDatedEntitiesCost C    
ON D.InvestmentID = C.UnderlyingEntityID AND ISNULL(D.LineTypeID, -1) = ISNULL(C.LineTypeID, -1)     
 AND D.TypeID = @CostAllocationTypeID AND D.TrackingKey = C.TrackingKey AND D.Tag = C.Tag AND D.IsExcludefromTransfer = C.IsExcludefromTransfer    
    
Update D    
SET D.TypeId = C.TypeID, D.AllocationType = CASE WHEN D.AllocationType = 'COST' THEN 'DEFAULT'     
             WHEN D.AllocationType = 'CostAdjustedDatedTransfer' THEN 'DefaultAdjustedDatedTransfer'    
             WHEN D.AllocationType = 'Cost without Transfer Adj %'     
              THEN 'Default without Transfer Adj %'    
              ELSE 'ProRata' END    
FROM #TempFinalEffectivePercentageDatedRounded D INNER JOIN #TempDatedEntitiesCost C    
ON D.InvestmentID = C.UnderlyingEntityID AND ISNULL(D.LineTypeID, -1) = ISNULL(C.LineTypeID, -1) AND D.Quarter = C.Quarter    
 AND D.TypeID = @CostAllocationTypeID AND D.TrackingKey = C.TrackingKey AND D.Tag = C.Tag AND D.IsExcludefromTransfer = C.IsExcludefromTransfer    
  
--------------------------------------------Return Data-----------------------------------------------------------------    
IF(@LocalMode =1)    
BEGIN    
 IF (@LocalIsPEModel = 0)    
 BEGIN    
    SELECT DISTINCT L.InvestmentID,  L.Partnernumber, L.EffPercentage,  L.AllocationType, L.Quarter,TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),-1, L.IsExcludefromTransfer, NULL
    , ISNULL(E.AssetClassId,0), ISNULL(L.LineTypeID, -1) ,[704cAllocationTypeId],[704cPercentageType], GPPartnerReceivingCarry   
    FROM #TempFinalEffectivePercentageNonDatedRounded L LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey 
    WHERE ISNULL(L.EffPercentage,0) <> 0  
    UNION ALL    
    SELECT DISTINCT  L.InvestmentID, L.Partnernumber, L.EffPercentage,  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),-1, L.IsExcludefromTransfer, NULL
    , ISNULL(E.AssetClassId,0), ISNULL(L.LineTypeID, -1)   
    ,0,'', NULL
    FROM #TempFinalEffectivePercentageDatedRounded L    
    INNER JOIN #TempUnderlyingsPickUpOrderDated T ON L.InvestmentID = T.InvestmentID AND L.PickUpOrder = T.PickUpOrder    
        AND L.TypeId = T.TypeID AND L.TrackingKey = T.TrackingKey AND L.Tag = T.Tag    
        AND T.Quarter = L.Quarter   
        AND ISNULL(L.EffPercentage,0) <> 0 AND L.IsExcludefromTransfer = T.IsExcludefromTransfer AND ISNULL(L.LineTypeID, -1) = ISNULL(T.LineTypeID, -1)        
    LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey    
    UNION ALL    
    SELECT DISTINCT InvestmentID,Partnernumber, 0, AllocationType,Quarter, TypeId,ISNULL(L.TrackingKey, ''), ISNULL(Tag, ''), -1, 0, EffectiveAmount, ISNULL(E.AssetClassId,0), ISNULL(L.LineTypeID, -1)    
    ,0,'', GPPartnerReceivingCarry FROM #FinalAmounts L LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey    
 END    
ELSE    
 BEGIN    
    
    SELECT L.InvestmentID,  L.Partnernumber, L.EffPercentage,  L.AllocationType, L.Quarter,TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''), ISNULL(L.LineTypeID, -1),ISNULL(Lineid,-1), L.IsExcludefromTransfer   ,
    [704cAllocationTypeId],[704cPercentageType], GPPartnerReceivingCarry
    FROM #TempFinalEffectivePercentageNonDatedRounded L    
    WHERE ISNULL(L.EffPercentage,0) <> 0  
    UNION ALL    
    SELECT  L.InvestmentID, L.Partnernumber, Round(L.EffPercentage,8),  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''), ISNULL(L.LineTypeID, -1),-1, L.IsExcludefromTransfer    
    ,0,'', NULL FROM #TempFinalEffectivePercentageDatedRounded L    
    INNER JOIN #TempUnderlyingsPickUpOrderDated T ON L.InvestmentID = T.InvestmentID AND L.PickUpOrder = T.PickUpOrder    
        AND L.TypeId = T.TypeID AND L.TrackingKey = T.TrackingKey AND L.Tag = T.Tag    
        AND T.Quarter = L.Quarter   
        AND ISNULL(L.EffPercentage,0) <> 0   
        AND L.IsExcludefromTransfer = T.IsExcludefromTransfer AND ISNULL(L.LineTypeID, -1) = ISNULL(T.LineTypeID, -1)        
 END    
END    
ELSE IF (@LocalMode = 2)    
BEGIN    
    SELECT DISTINCT L.InvestmentID,  L.Partnernumber, L.EffPercentage,  L.AllocationType, L.Quarter,TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''), ISNULL(L.LineTypeID, -1),LineID, L.IsExcludefromTransfer, ISNULL(E.AssetClassId,0)    
    ,[704cAllocationTypeId],[704cPercentageType] FROM #TempFinalEffectivePercentageNonDatedRounded L   
    LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey    
    WHERE ISNULL(L.EffPercentage,0) <> 0     
    UNION ALL    
    SELECT DISTINCT  L.InvestmentID, L.Partnernumber, L.EffPercentage,  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''), ISNULL(L.LineTypeID, -1),NULL, L.IsExcludefromTransfer, ISNULL(E.AssetClassId,0)    
    ,0,'' FROM #TempFinalEffectivePercentageDatedRounded L    
    INNER JOIN #TempUnderlyingsPickUpOrderDated T ON L.InvestmentID = T.InvestmentID AND L.PickUpOrder = T.PickUpOrder    
        AND L.TypeId = T.TypeID AND L.TrackingKey = T.TrackingKey AND L.Tag = T.Tag    
        AND T.Quarter = L.Quarter   
        AND ISNULL(L.EffPercentage,0) <> 0   
        AND L.IsExcludefromTransfer = T.IsExcludefromTransfer    
        AND ISNULL(L.LineTypeID, -1) = ISNULL(T.LineTypeID, -1)    
    LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey    
END    
ELSE IF (@LocalMode = 3)    
BEGIN    
    SELECT Distinct L.InvestmentID,  L.Partnernumber, L.EffPercentage,  L.AllocationType, L.Quarter,TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),NULL, NULL,L.IsExcludefromTransfer, ISNULL(E.AssetClassId,0)    
    ,[704cAllocationTypeId],[704cPercentageType] FROM #TempFinalEffectivePercentageNonDatedRounded L   
    LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey    
    WHERE ISNULL(L.EffPercentage,0)  <> 0  
    UNION ALL    
    SELECT Distinct  L.InvestmentID, L.Partnernumber, L.EffPercentage,  L.AllocationType, L.Quarter, L.TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),NULL, NULL ,L.IsExcludefromTransfer, ISNULL(E.AssetClassId,0)    
    ,0,'' FROM #TempFinalEffectivePercentageDatedRounded L   
    LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey    
    INNER JOIN #TempUnderlyingsPickUpOrderDated T ON L.InvestmentID = T.InvestmentID AND L.PickUpOrder = T.PickUpOrder    
        AND L.TypeId = T.TypeID AND L.TrackingKey = T.TrackingKey AND L.Tag = T.Tag    
        AND T.Quarter = L.Quarter   
        AND ISNULL(L.EffPercentage,0)<> 0  
        AND L.IsExcludefromTransfer = T.IsExcludefromTransfer AND ISNULL(L.LineTypeID, -1) = ISNULL(T.LineTypeID, -1)       
    UNION ALL    
    SELECT DISTINCT InvestmentID,Partnernumber, 0, AllocationType,Quarter, TypeId,ISNULL(L.TrackingKey, ''), ISNULL(Tag, ''), NULL, EffectiveAmount,L.IsExcludefromTransfer, ISNULL(E.AssetClassId,0)    
    ,0,'' FROM #FinalAmounts L   
    LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey    
END   
ELSE IF(@LocalMode =4)    
BEGIN 
 SELECT DISTINCT L.InvestmentID,  L.Partnernumber, L.EffPercentage, L.AllocationType, L.Quarter,TypeId, ISNULL(L.TrackingKey, ''), ISNULL(L.Tag, ''),-1, L.IsExcludefromTransfer, NULL
 , ISNULL(E.AssetClassId,0), ISNULL(L.LineTypeID, -1) ,[704cAllocationTypeId],[704cPercentageType] , GPPartnerReceivingCarry   
	FROM #TempFinalEffectivePercentageNonDatedRounded L LEFT JOIN #TempEntityUnderlying E ON E.UnderlyingEntityId = L.InvestmentID AND E.TrackingKey = L.TrackingKey 
    WHERE ISNULL(L.EffPercentage,0) <> 0  AND ISNULL([704cPercentageType],'') <> ''
END
---------------------------------------------------------------------------------------------------------------------------    
    
    
    
SET @EndDate = GETDATE()    
EXEC [dbo].[uspUpdateAllocationLog] @LogID, @EndDate    
   
DROP TABLE IF EXISTS #TempMinimumQuarter    
DROP TABLE IF EXISTS #FinalCostPercentage    
DROP TABLE IF EXISTS #TempInputLines    
DROP TABLE IF EXISTS #TempDatedEntities    
DROP TABLE IF EXISTS #TempNonDatedEntities    
DROP TABLE IF EXISTS #TempCostPercentageMinQuarter    
DROP TABLE IF EXISTS #TempUnderlyingsPickUpOrderDated    
DROP TABLE IF EXISTS #TempFinalEffectivePercentageDated    
DROP TABLE IF EXISTS #TempErrorUnderlyings    
DROP TABLE IF EXISTS #TempFinalEffectivePercentageNonDated    
DROP TABLE IF EXISTS #TempSelectedNonDatedLines    
DROP TABLE IF EXISTS #EntityPartners    
DROP TABLE IF EXISTS #TempCostPercentage    
DROP TABLE IF EXISTS #TempCostPercentageDeals     
DROP TABLE IF EXISTS #TempEntityUnderlying    
DROP TABLE IF EXISTS #TempCostDefinedDeals    
DROP TABLE IF EXISTS #TempCostTransferDefinedDeals    
DROP TABLE IF EXISTS #TempTransfersAdjCostDefaultPercentage    
DROP TABLE IF EXISTS #TempEffectivePercentagePlug    
DROP TABLE IF EXISTS #TempEffectivePercentageMaxCommitment    
DROP TABLE IF EXISTS #TempBookEffectiveData    
DROP TABLE IF EXISTS #TempTransferAdjDatedPercentages    
DROP TABLE IF EXISTS #TempYearly    
DROP TABLE IF EXISTS #TempTransferDate    
DROP TABLE IF EXISTS #TempCost    
DROP TABLE IF EXISTS #TempAllEntities
DROP TABLE IF EXISTS #TempDatedEntitiesCost    
DROP TABLE IF EXISTS #TempNonDatedEntitiesCost    
DROP TABLE IF EXISTS #TempYearlyDatedToBeDeleted    
DROP TABLE IF EXISTS #TempDatedEntitiesNotransfer    
DROP TABLE IF EXISTS #TempDatedEntitiesNotransferPickUpQuarter    
DROP TABLE IF EXISTS #SM_TempBookEffective    
DROP TABLE IF EXISTS #Temp199ACostPercentage     
DROP TABLE IF EXISTS #TempCostDefinedDeals3    
DROP TABLE IF EXISTS #TempEnitityAllocationRule    
DROP TABLE IF EXISTS #TempDefaultAllocationRule    
DROP TABLE IF EXISTS #FinalAmounts    
DROP TABLE IF EXISTS #TempEntityExcludeDeals1    
DROP TABLE IF EXISTS #TempExcludeExistingDeals    
DROP TABLE IF EXISTS #TempAllUnderlyings    
DROP TABLE IF EXISTS #LineItem    
DROP TABLE IF EXISTS #TempAllUnderlyingsCombined    
DROP TABLE IF EXISTS #TempCostUnderlyingTypes    
DROP TABLE IF EXISTS #TotalUnderlyingAmounts    
DROP TABLE IF EXISTS #FinalEffectiveAmounts    
DROP TABLE IF EXISTS #TempAllUnderlyingsOrdered    
DROP TABLE IF EXISTS #TempAllUnderlyingsFNOrdered    
DROP TABLE IF EXISTS #TempLookThroughAllocationInput    
DROP TABLE IF EXISTS #TempAllocationInput    
DROP TABLE IF EXISTS #TempSMLookThroughAllocationInput    
DROP TABLE IF EXISTS #EntityAssetClassRelationShip    
DROP TABLE IF EXISTS #TempCostDefinedDeals1    
DROP TABLE IF EXISTS #TempEntityExcludeDeals2  
DROP TABLE IF EXISTS #TempAllUnderlyingsStatesOrdered  
DROP TABLE IF EXISTS #TempFinalEffectivePercentageDatedRounded  
DROP TABLE IF EXISTS #TempFinalEffectivePercentageNonDatedRounded  
DROP TABLE IF EXISTS #TempAllUnderlyingsCombinedOrdered  
DROP TABLE IF EXISTS #tmpCustomFootnoteLineTypes  
DROP TABLE IF EXISTS #CostPercentage_Snapshot
DROP TABLE IF EXISTS #tmpAllUnderlyingsWithTrackingKeyMatch
DROP TABLE IF EXISTS #tempunderlyingMod
DROP TABLE IF EXISTS #tmpAllUnderlyingsWithTrackingKeyMatch_Adj
DROP TABLE IF EXISTS #TempAllEntities_Adj
DROP TABLE IF EXISTS #CostPercentage_function
DROP TABLE IF EXISTS #tmpPartVQuarters    
DROP TABLE IF EXISTS #AllocationPercentage704c
DROP TABLE IF EXiSTS #TempYearlyLines
DROP TABLE IF EXiSTS #quarters
DROP TABLE IF EXISTS #YearlyData
DROP TABLE IF EXISTS #TempFilteredTransfersAdjCostDefaultPercentage
DROP TABLE IF EXISTS #DefaultAllocationRuleSetup
DROP TABLE IF EXISTS #MapDefaultAllocRuleToLineItem
DROP TABLE IF EXISTS #Mappings
DROP TABLE IF EXISTS #CostPercentage_Function
DROP TABLE IF EXISTS #CostPercentage704cValues
DROP TABLE IF EXISTS #CostPercentage_Snapshot_UnPivoted
DROP TABLE IF EXISTS #CostPercentage_Snapshot_UnPivotedMerged
DROP TABLE IF EXISTS #EntityTotalAmounts
DROP TABLE IF EXISTS #EntityHierarchy
DROP TABLE IF EXISTS #TempFootnoteBookEffectiveData

END

