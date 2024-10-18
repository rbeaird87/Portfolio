------------------------------------------------------------------------------------------------------------
/* THIS STORED PROCEDURE LOADS HISTORIC AR (ACCOUNTS RECEIVABLE) EHR (ELECTRONIC HEALTH RECORD) DATA FROM A TABLE AND PUTS INTO A STANDARDIZED METRIC FORMAT */
------------------------------------------------------------------------------------------------------------


DECLARE @QueryLookbackDays int = 1 --use 1 month for monthly load or 1 day for daily
DECLARE @QueryBeginDate date = DATEADD(MONTH,-1,DATEFROMPARTS(YEAR(DATEADD(MONTH,-@QueryLookbackDays,GETDATE())),MONTH(DATEADD(MONTH,-@QueryLookbackDays,GETDATE())),1))  --'2024-01-01'--
DECLARE @QueryEndDate date = GETDATE()
DECLARE @DeleteMessage varchar(200) = 'Deleting duplicate metric values between ' + CAST(@QueryBeginDate AS varchar(20)) + ' & ' + CAST(@QueryEndDate AS varchar(20))
DECLARE @InsertMessage varchar(200) = 'Inserting new metric values between ' + CAST(@QueryBeginDate AS varchar(20)) + ' & ' + CAST(@QueryEndDate AS varchar(20))

BEGIN TRY
------------------------------------------------------------------------------------------------------------
/* Delete dupicate metric values */
------------------------------------------------------------------------------------------------------------

PRINT @DeleteMessage
DELETE rpt.MetricValues
	WHERE 1=1
		AND MetricValueDate >= @QueryBeginDate 
		AND MetricID = 11

------------------------------------------------------------------------------------------------------------
/* Insert new metric values */
------------------------------------------------------------------------------------------------------------

PRINT @InsertMessage
INSERT INTO rpt.MetricValues(
     [MetricID]
     ,[MetricValueDate]
     ,[DataSourceID]
     ,[LocationID]
     ,[DepartmentID]
     ,[ProviderID]
     ,[PracticeID]
     ,[PayerID]
     ,[ServiceLine]
     ,[ReportGroup1]
     ,[ReportGroup2]
     ,[ReportGroup3]
     ,[ReportGroup4]
     ,[ReportGroup5]
     ,[ReportGroup6]
     ,[ReportGroup7]
     ,[MetricValueNumerator]
     ,[MetricValueDenominator]
     ,[MetricValue]
     ,[UpdateDatetime]
)


SELECT 
	11									AS MetricID		 
	,d.FirstDayOfMonth							AS MetricValueDatE
	,ar.TransactionDataSourceID						AS DataSourceID
	,ar.TransactionLocationID						AS LocationID 
	,ar.TransactionDepartmentID						AS [DepartmentID]
	,ar.TransactionBillingProviderID					AS [ProviderID]
	,pt.PracticeID								AS [PracticeID]
	,ar.CurrentPayerID							AS PayerID
	,NULL									AS ServiceLine
	,CASE
		WHEN ar.TransactionServiceDateAge BETWEEN 0 AND 30
			THEN '0-30'
		WHEN ar.TransactionServiceDateAge BETWEEN 31 AND 60 
			THEN '31-60'
		WHEN ar.TransactionServiceDateAge BETWEEN 61 AND 90 
			THEN '61-90'
		WHEN ar.TransactionServiceDateAge BETWEEN 91 AND 120 
			THEN '91-120'
		WHEN ar.TransactionServiceDateAge > 120 
			THEN '121+'
		ELSE NULL
	END									AS [ReportGroup1] -- AR Aging Bucket, ar.TransactionARAgingBucket
	,'Snapshot'								AS [ReportGroup2] -- Snapshot vs current, ar.PeriodCategory
	,ar.ARHistoryDate							AS [ReportGroup3] -- Snapshot date
	,null									AS [ReportGroup4]
	,null									AS [ReportGroup5]
	,null									AS [ReportGroup6]
	,null									AS [ReportGroup7]
	,null									AS [MetricValueNumerator]
	,null									AS [MetricValueDenominator]
	,sum(ar.TransactionARAmountActive)					AS [MetricValue] --Active AR
	,GETDATE()								AS [UpdateDatetime]
FROM rpt.ARHistoryPB ar

LEFT JOIN dim.Dates d
	ON FORMAT(ar.ARHistoryDate, 'yyyy-dd-MM') = FORMAT(d.[Date], 'yyyy-dd-MM')
--LEFT JOIN map.vPracticeProviders pp
--	ON ar.TransactionBillingProviderID = pp.ProviderID
LEFT JOIN dim.Departments dt
	ON ar.TransactionDepartmentID = dt.DepartmentID
	left join map.ProviderLinking pl ON pl.ChildProviderID = ar.TransactionBillingProviderID
	left join map.PracticeDepartments pd ON pd.DepartmentID = ar.TransactionDepartmentID
	left join map.vPracticeProviders pp ON pp.ParentProviderID = pl.ParentProviderID
		AND pp.PracticeProviderEffectiveDate <= ar.TransactionPostDate 
		AND pp.PracticeProviderEndDate >= ar.TransactionPostDate
		AND ((ar.TransactionBillingProviderID in ('1~19898','5~126867','1~19711','5~125582') 
			AND (pp.PracticeID = pd.PracticeID OR (pd.PracticeID is null 
				AND ar.TransactionBillingProviderID = pp.ProviderID)))
			OR ar.TransactionBillingProviderID not in ('1~19898','5~126867','1~19711','5~125582'))
	left join dim.Practices pt ON pt.PracticeID = COALESCE(pd.PracticeID,pp.PracticeID)
--LEFT JOIN dim.dates gd
--	ON FORMAT(GETDATE(), 'yyyy-dd-MM') = FORMAT(gd.[Date], 'yyyy-dd-MM')

WHERE 1=1
	--AND ar.IsARCredit = 0
	AND ar.TransactionARAmountActive > 0
	AND d.[Date] between @QueryBeginDate and @QueryEndDate

GROUP BY
	d.FirstDayOfMonth				
	,ar.TransactionDataSourceID		
	,ar.TransactionLocationID		
	,ar.TransactionDepartmentID		
	,ar.TransactionBillingProviderID
	,pt.PracticeID					
	,ar.CurrentPayerID	
	,CASE
		WHEN ar.TransactionServiceDateAge BETWEEN 0 AND 30
			THEN '0-30'
		WHEN ar.TransactionServiceDateAge BETWEEN 31 AND 60 
			THEN '31-60'
		WHEN ar.TransactionServiceDateAge BETWEEN 61 AND 90 
			THEN '61-90'
		WHEN ar.TransactionServiceDateAge BETWEEN 91 AND 120 
			THEN '91-120'
		WHEN ar.TransactionServiceDateAge > 120 
			THEN '121+'
		ELSE NULL
	END
	,ar.ARHistoryDate


;END TRY
BEGIN CATCH
	PRINT ERROR_MESSAGE()
	PRINT 'ROLLING BACK DELETION'
	ROLLBACK;
END CATCH;

	
END
