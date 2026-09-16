
CREATE PROCEDURE [dbo].[uspUpdateAllocationLog]
(
	@LogID INT,
	@EndDate DateTime = NULL
)
AS
BEGIN

/* =================================================================================================
Author		Date		Comment  
Muni M		10/27/2017	Initial Creation.  Used to update the log end date and time.
Xiaoping    04/06/2023  Bug 271154: Deadlocks occurs when two store procedures to update AllocationLog table at the same time.
=================================================================================================*/
	SET NOCOUNT ON;

	IF (@EndDate IS NULL)
		SET @EndDate = GETDATE();

	UPDATE	[AllocationLog] with (rowlock)
	SET		EndDate = @EndDate
	WHERE	LogID = @LogID;
END
