
CREATE PROCEDURE [dbo].[uspAddAllocationLog]
@ClientID INT,
@TaxPeriodID INT,
@RunID INT = NULL,
@Category VARCHAR(50) = NULL,
@ProcessName VARCHAR(100),
@StartDate DateTime,
@EndDate DateTime = NULL,
@LogDescription VARCHAR(1000) = NULL,
@LogID int = 0 OUTPUT
AS
BEGIN

/* =================================================================================================
Author		Date		Comment  
Tai T		08/05/2011	Initial Creation.  Used to log the steps within the allocation process.
Muni M		10/27/2017	LogID included as an output parameter
Davinder	01/12/2018	TFS 2108831: Replace @@Identity with SCOPE_IDENTITY()
Xiaoping    04/06/2023  Bug 271154: Deadlocks occurs when two store procedures to update AllocationLog table at the same time.
=================================================================================================*/

SET NOCOUNT ON

INSERT INTO [AllocationLog] with(rowlock) (ClientID, TaxPeriodID, Category, LogDescription, ProcessName, StartDate, EndDate, RunID) 
VALUES (@ClientID, @TaxPeriodID, @Category, @LogDescription, @ProcessName, @StartDate, @EndDate, @RunID);

SET @LogID = SCOPE_IDENTITY();

END

