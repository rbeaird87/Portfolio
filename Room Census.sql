CREATE PROCEDURE rpt.spReloadMeditechCensusHistory_INCREMENTAL AS

------------------------------------------------------------------------------------------------------------
/* Declare date variables */
------------------------------------------------------------------------------------------------------------
DECLARE @currdate date = CAST(GETDATE() AS date) --(SELECT CAST(MAX(roomcensusdateid) AS date) FROM #census) -- CAST(GETDATE() AS date) -- '2024-06-11' --
DECLARE @currdatetime datetime = CAST(@currdate AS datetime) --(SELECT MAX(roomcensusdateid) FROM #census) -- GETDATE() -- CAST(@currdate AS datetime) --
DECLARE @prevdate date = DATEADD(DAY,-1,@currdate)
PRINT 'Current date: ' + CAST(@currdate AS varchar(20))
PRINT 'Previous day date: ' + CAST(@prevdate AS varchar(20))

------------------------------------------------------------------------------------------------------------
/* Copy Meditech census into temp table */
------------------------------------------------------------------------------------------------------------
PRINT 'Building room census table...'
DROP TABLE IF EXISTS #census
SELECT *
INTO #census
FROM MEDITECH.Testdb.[mat].[regacct_roomcensus];

------------------------------------------------------------------------------------------------------------
/* Copy Meditech regacct_main into temp table */
------------------------------------------------------------------------------------------------------------
PRINT 'Building regacct_main table...'
DROP TABLE IF EXISTS #regacct
SELECT *
INTO #regacct
FROM MEDITECH.Testdb.[mat].regacct_main

------------------------------------------------------------------------------------------------------------
/* Copy Meditech accomodations into temp table */
------------------------------------------------------------------------------------------------------------
PRINT 'Building accomodations table...'
DROP TABLE IF EXISTS #accomodations
SELECT *
INTO #accomodations
FROM MEDITECH.Testdb.[npr].[dmisaccommodation];

------------------------------------------------------------------------------------------------------------
/* Build Pivoted ADT Event table */
------------------------------------------------------------------------------------------------------------
PRINT 'Building ADT event table...'
DROP TABLE IF EXISTS #adtpivot
SELECT
	CONCAT([ADTEventVisitID],'|',CAST([ADTEventDatetime] AS date))	AS ADTVisitDateID
	,[ADTEventVisitID]
	,CAST([ADTEventDatetime] AS date)								AS ADTEventDate
	,[ADTEventDatasourceID]
	,[ADTEventLocationID]
	,[ADTEventRoomID]
	,SUM(CASE WHEN (CASE
		WHEN CAST(ADTEventDatetime AS date) = @currdate AND ADTEventSubtype = 'Admit'
			THEN 1
		ELSE 0
	END) > 0 THEN 1 ELSE 0 END)										AS CensusIsAdmit
	,SUM(CASE WHEN (CASE
		WHEN CAST(ADTEventDatetime AS date) = @currdate AND ADTEventSubtype = 'LOA Out'
			THEN 1
		ELSE 0
	END) > 0 THEN 1 ELSE 0 END)										AS [CensusIsLOABegin]
	,SUM(CASE WHEN (CASE
		WHEN CAST(ADTEventDatetime AS date) = @currdate AND ADTEventSubtype = 'LOA In'
			THEN 1
		ELSE 0
	END) > 0 THEN 1 ELSE 0 END)										AS [CensusIsLOAReturn]
	,SUM(CASE WHEN (CASE
		WHEN CAST(ADTEventDatetime AS date) = @currdate AND ADTEventSubtype = 'Discharge'
			THEN 1
		ELSE 0
	END) > 0 THEN 1 ELSE 0 END)										AS [CensusIsDischarge]
	,MAX([ADTEventTransferTo])										AS [ADTEventTransferTo]
	,MAX([ADTEventTransferFrom])									AS [ADTEventTransferFrom]
	,MAX(CASE 
		WHEN ADTEventSubtype = 'Discharge' 
			THEN ADTEventDatetime 
		ELSE NULL 
	END)															AS DischargeDatetime
INTO #adtpivot
FROM [fact].[ADTEvents]

WHERE 1=1
	AND CAST(ADTEventDatetime AS date) = @currdate
	AND ADTEventDatasourceID = 7

GROUP BY 
	CONCAT([ADTEventVisitID],'|',CAST([ADTEventDatetime] AS date))
	,[ADTEventVisitID]
	,CAST([ADTEventDatetime] AS date)
	,[ADTEventDatasourceID]
	,[ADTEventLocationID]
	,[ADTEventRoomID];

------------------------------------------------------------------------------------------------------------
/* Delete potential duplicate rows and mark previous date census as inactive */
------------------------------------------------------------------------------------------------------------
PRINT 'Deleting current day records...';
DELETE [EDW].[rpt].[CensusHistory] WHERE CensusDate = @currdate AND CensusDatasourceID = 7;
PRINT 'Marking previous day census as inactive...';
UPDATE [EDW].[rpt].[CensusHistory] SET CensusIsActive = 0 WHERE CensusDate = @prevdate AND CensusDatasourceID = 7;
------------------------------------------------------------------------------------------------------------
/* CTE to index patients  */
------------------------------------------------------------------------------------------------------------
WITH EmptyDischargedRooms AS(
	SELECT
		rr.RoomName, pdcc.visitid, adtt.CensusIsDischarge, adtt.ADTEventDate
	FROM dim.Rooms rr

	/* Join current day's census  */
	LEFT JOIN #census cc
		ON rr.RoomName = CONCAT(cc.roomcensusroom_misrmid,'|',cc.roomcensusbed)
			AND CAST(cc.roomcensusdateid AS date) = @currdate
	
	/* Join previous day's census  */
	LEFT JOIN #census pdcc
		ON rr.RoomName = CONCAT(pdcc.roomcensusroom_misrmid,'|',pdcc.roomcensusbed)
			AND CAST(pdcc.roomcensusdateid AS date) = @prevdate
	
	/* Join current day Discharge adt events for patients in the previous day census  */
	LEFT JOIN #adtpivot adtt
		ON pdcc.visitid = adtt.ADTEventVisitID
			AND adtt.ADTEventDate = @currdate
			
	WHERE 1=1
		AND cc.visitid IS NULL -- room is empty at the end of the current day
		AND rr.RoomDatasourceID = 7
		AND adtt.CensusIsDischarge = 1 -- patients was discharged during the current day
)

------------------------------------------------------------------------------------------------------------
/* ////////////////////////////////////////////////////////////////////////////////////////////////////// */
------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------
/* Insert new records */
------------------------------------------------------------------------------------------------------------
INSERT INTO [EDW].[rpt].[CensusHistory](
	[CensusID]
	,[CensusDatasourceID]
	,[CensusDate]
	,[CensusMidnight]
	,[CensusLocationID]
	,[CensusRoomID]
	,[CensusPatientID]
	,[CensusVisitID]
	,[CensusVisitReferenceNumber]
	,[CensusVisitAdmitDatetime]
	,[CensusVisitDischargeDatetime]
	,[CensusCurrentServiceLine]
	,[CensusCurrentLevelOfCare]
	,[CensusIsRoomLicensed]
	,[CensusIsAdmit]
	,[CensusIsLOABegin]
	,[CensusIsLOA]
	,[CensusIsLOAReturn]
	,[CensusIsDischarge]
	,[CensusIsActive]
	,[CensusCount]
	,[CensusCountOverride]
	,[CensusCountOverrideDatetime]
	,[CensusUpdatedDatetime]
)


------------------------------------------------------------------------------------------------------------
/* Query for non-discharged census */
------------------------------------------------------------------------------------------------------------
SELECT
	CONCAT(r.RoomID,'|',@currdate)									AS [CensusID]
	,7																AS [CensusDatasourceID]
	,@currdate														AS [CensusDate] --CAST(pdc.roomcensusdateid AS date)
	,DATEADD(millisecond, -3, @currdatetime)						AS [CensusMidnight]
	,r.RoomLocationID												AS [CensusLocationID]
	,r.RoomID														AS [CensusRoomID]
	,a.patientid													AS [CensusPatientID]
	,a.visitid														AS [CensusVisitID]
	,a.accountnumber												AS [CensusVisitReferenceNumber]
	,a.admitdatetime												AS [CensusVisitAdmitDatetime]
	,NULL															AS [CensusVisitDischargeDatetime]
	,rt.[name]														AS [CensusCurrentServiceLine] -- location description?
	,acc.[name]														AS [CensusCurrentLevelOfCare] -- care description?
	,r.RoomIsLicensed												AS [CensusIsRoomLicensed]
	,adt.CensusIsAdmit												AS [CensusIsAdmit]
	,adt.CensusIsLOABegin											AS [CensusIsLOABegin]
	,adt.CensusIsLOAReturn											AS [CensusIsLOAReturn]
	,CASE
		WHEN a.loastatus = 'Y'
			THEN 1
		ELSE 0
	END																AS [CensusIsLOA]
	,0																AS [CensusIsDischarge]
	,1																AS [CensusIsActive]
	,1																AS [CensusCount] --when IsLOA = 1 then 0?
	,NULL															AS [CensusCountOverride]
	,NULL															AS [CensusCountOverrideDatetime]
	,GETDATE()														AS [CensusUpdatedDatetime]
	--,'||||||||ADT|||||||||||'
	--,adt.*
FROM dim.Rooms r

------------------------------------------------------------------------------------------------------------
/* Join current day room census */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #census c
	ON r.RoomName = CONCAT(c.roomcensusroom_misrmid,'|',c.roomcensusbed)
		AND CAST(c.roomcensusdateid AS date) = @currdate

------------------------------------------------------------------------------------------------------------
/* Join location info (not date dependant) */
------------------------------------------------------------------------------------------------------------
LEFT JOIN dim.Locations l
	ON r.RoomLocationID = l.LocationID

------------------------------------------------------------------------------------------------------------
/* Join non-discharged acct info */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #regacct a
	ON c.visitid = a.visitid
		--AND a.registrationstatus = 'adm'

------------------------------------------------------------------------------------------------------------
/* Join non-discharged accomodation info */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #accomodations acc
	ON a.accommodation_misaccomid = acc.accommodationid

------------------------------------------------------------------------------------------------------------
/* Join current day ADT events */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #adtpivot adt
	ON a.visitid = adt.ADTEventVisitID
		AND adt.ADTEventDate = @currdate

------------------------------------------------------------------------------------------------------------
/* Join registration type information */
------------------------------------------------------------------------------------------------------------
LEFT JOIN MEDITECH.Testdb.[mat].misregtype_main rt
	ON a.registrationtype_misregtypeid = rt.misregtypeid

------------------------------------------------------------------------------------------------------------

WHERE 1=1
	AND (adt.CensusIsDischarge = 0 OR adt.CensusIsDischarge IS NULL)
	AND r.RoomDatasourceID = 7
	AND r.RoomName NOT IN ( SELECT RoomName FROM EmptyDischargedRooms)

--ORDER BY r.roomid

------------------------------------------------------------------------------------------------------------
/* ////////////////////////////////////////////////////////////////////////////////////////////////////// */
------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------
/* Union query for discharged patient census */
------------------------------------------------------------------------------------------------------------
UNION 

SELECT
	CASE
		WHEN r.RoomName IN ( SELECT RoomName FROM EmptyDischargedRooms)
			THEN CONCAT(r.RoomID,'|',@currdate)-- discharged patient, empty room
		ELSE CONCAT(r.RoomID,'|',@currdate,'|d')-- discharged patient, refilled room (will require duplicate line for room)
	END																AS [CensusID]
	,7																AS [CensusDatasourceID]
	,@currdate														AS [CensusDate]
	,DATEADD(millisecond, -3, @currdatetime)						AS [CensusMidnight]
	,r.RoomLocationID												AS [CensusLocationID]
	,r.RoomID														AS [CensusRoomID]
	,a.patientid													AS [CensusPatientID]
	,a.visitid														AS [CensusVisitID]
	,a.accountnumber												AS [CensusVisitReferenceNumber]
	,a.admitdatetime												AS [CensusVisitAdmitDatetime]
	,adt.DischargeDatetime											AS [CensusVisitDischargeDatetime]
	,rt.[name]														AS [CensusCurrentServiceLine] -- location description?
	,acc.[name]														AS [CensusCurrentLevelOfCare] -- care description?
	,r.RoomIsLicensed												AS [CensusIsRoomLicensed]
	,adt.CensusIsAdmit												AS [CensusIsAdmit]
	,adt.CensusIsLOABegin											AS [CensusIsLOABegin]
	,0																AS [CensusIsLOA]
	,adt.CensusIsLOAReturn											AS [CensusIsLOAReturn]
	,1																AS [CensusIsDischarge]
	,1																AS [CensusIsActive]
	,CASE
		WHEN r.RoomName IN ( SELECT RoomName FROM EmptyDischargedRooms)
			THEN 0 -- discharged patient, empty room
		ELSE 1 -- discharged patient, refilled room
	END																AS [CensusCount]
	,NULL															AS [CensusCountOverride]
	,NULL															AS [CensusCountOverrideDatetime]
	,GETDATE()														AS [CensusUpdatedDatetime]
	--,'|||||||ADT|||||||||||'
	--,adt.*
FROM dim.Rooms r

------------------------------------------------------------------------------------------------------------
/* Join previous day room census */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #census pdc
	ON r.RoomName = CONCAT(pdc.roomcensusroom_misrmid,'|',pdc.roomcensusbed)
		AND CAST(pdc.roomcensusdateid AS date) = @prevdate

------------------------------------------------------------------------------------------------------------
/* Join discharged acct info */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #regacct a
	ON pdc.visitid = a.visitid

------------------------------------------------------------------------------------------------------------
/* Join discharged accomodation info */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #accomodations acc
	ON a.accommodation_misaccomid = acc.accommodationid
	
------------------------------------------------------------------------------------------------------------
/* Join previous day's ADT events */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #adtpivot pdadt
	ON a.visitid = pdadt.ADTEventVisitID
		AND pdadt.ADTEventDate = @prevdate

------------------------------------------------------------------------------------------------------------
/* Join location info (not date dependant) */
------------------------------------------------------------------------------------------------------------
LEFT JOIN dim.Locations l
	ON r.RoomLocationID = l.LocationID

------------------------------------------------------------------------------------------------------------
/* Join current day ADT events */
------------------------------------------------------------------------------------------------------------
LEFT JOIN #adtpivot adt
	ON a.visitid = adt.ADTEventVisitID
		AND adt.ADTEventDate = @currdate

------------------------------------------------------------------------------------------------------------
/* Join latest time current day ADT events */
------------------------------------------------------------------------------------------------------------
LEFT JOIN [fact].[ADTEvents] dadt
	ON a.visitid = dadt.ADTEventVisitID
		AND CAST(dadt.ADTEventDatetime AS date) = @currdate

------------------------------------------------------------------------------------------------------------
/* Join registration type information */
------------------------------------------------------------------------------------------------------------
LEFT JOIN MEDITECH.Testdb.[mat].misregtype_main rt
	ON a.registrationtype_misregtypeid = rt.misregtypeid

------------------------------------------------------------------------------------------------------------
/* Self-join previous day census history */
------------------------------------------------------------------------------------------------------------
LEFT JOIN [EDW].[rpt].[CensusHistory] pdch
	ON r.RoomID = pdch.CensusRoomID
		AND a.patientid = pdch.CensusPatientID
		AND pdch.CensusDate = @prevdate

------------------------------------------------------------------------------------------------------------

WHERE r.RoomDatasourceID = 7
	AND adt.CensusIsDischarge = 1
	--AND pdc.visitid = 'BC0-B20240603095503374'

------------------------------------------------------------------------------------------------------------
/* ////////////////////////////////////////////////////////////////////////////////////////////////////// */
------------------------------------------------------------------------------------------------------------

order by r.roomid

DROP TABLE #accomodations
DROP TABLE #census
DROP TABLE #regacct