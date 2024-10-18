--------------------------------------------------------------------------------------------------------------------
-- THIS STORED PROCEDURE ESTIMATES EXPECTED STAFFING LOADS BASED ON PROCEDURE ACTIVITIES FOR DIFFERENT HOSPITALS
--------------------------------------------------------------------------------------------------------------------



SELECT
	--sub.[PayPeriodStartDate]
	--,sub.[PayPeriodEndDate]
	sub.[PayDate]
	,sub.LocationName
	,sub.TargetEvents
	,sub.LaborTarget
	,sub.TargetEvents * lst.TargetVariable												AS StatsHours
FROM (

--------------------------------------------------------------------------------------------------------------------
-- 1: Pain and OR cases: querying all cases from our data warehouse and then grouping them by location/department
--------------------------------------------------------------------------------------------------------------------
	SELECT 
		c.VisitCaseLocationID													AS LocationID
		,p.PayDate																AS PayDate
		,COUNT(DISTINCT c.VisitCaseID)											AS TargetEvents -- Surgical Cases
		,CASE WHEN l.LocationName = 'ZZZ CHN PAIN' THEN 'Pain North'
			WHEN l.LocationName = 'ZZZ CHN OR' THEN 'Surgery North'
			WHEN l.LocationName = 'ZZZ CHS PAIN' THEN 'Pain South'
			WHEN l.LocationName = 'ZZZ CHS OR' THEN 'Surgery South'
			WHEN l.LocationName = 'ZZZ NWSH OR' THEN 'Surgery NWSH'
			WHEN l.LocationName = 'ZZZ CHS ENDOSCOPY' THEN NULL
			WHEN l.LocationName = 'ZZZ CHN ENDOSCOPY' THEN NULL
			ELSE NULL END														AS LocationName
		,NULL /*CASE WHEN l.LocationName = 'ZZZ CHN PAIN' THEN (COUNT(DISTINCT c.VisitCaseID)*0.9) 
			WHEN l.LocationName = 'ZZZ CHN OR' THEN (COUNT(DISTINCT c.VisitCaseID)*4.6)
			WHEN l.LocationName = 'ZZZ CHS PAIN' THEN (COUNT(DISTINCT c.VisitCaseID)*0.9)
			WHEN l.LocationName = 'ZZZ CHS OR' THEN (COUNT(DISTINCT c.VisitCaseID)*4.6)
			WHEN l.LocationName = 'ZZZ NWSH OR' THEN (COUNT(DISTINCT c.VisitCaseID)*10.0)
			WHEN l.LocationName = 'ZZZ CHS ENDOSCOPY' THEN (COUNT(DISTINCT c.VisitCaseID)*0.0)
			WHEN l.LocationName = 'ZZZ CHN ENDOSCOPY' THEN (COUNT(DISTINCT c.VisitCaseID)*0.0)
		ELSE null END*/															AS LaborTarget
	FROM fact.VisitCases c

	LEFT JOIN map.LaborPayPeriods p 
		ON cast(c.VisitCaseServiceDate as date) = P.PayPeriodDate
	LEFT JOIN dim.Locations l 
		ON l.LocationID = c.VisitCaseLocationID

	WHERE 1=1
		AND c.VisitCaseLogStatus = 'Posted'

	GROUP BY 
		c.VisitCaseLocationID
		,p.PayDate
		,l.LocationName

	--ORDER BY PayDate

--------------------------------------------------------------------------------------------------------------------
-- 2: all Med/Surg and ICU from Clarity ADT feed, bringing in all transfers in (event type 3) 
-- and then grouping by department, excluding emergency since it will be its own union
--------------------------------------------------------------------------------------------------------------------

UNION ALL

	SELECT 
		p.PayDate																			AS PayDate
		,cd.DEPARTMENT_ID																	AS LocationID
		,CASE 
			WHEN cd.DEPARTMENT_NAME = 'ZZZ CHN MED/SURG' THEN 'Med-Surg North'
			WHEN cd.DEPARTMENT_NAME = 'ZZZ CHS MED/SURG' THEN 'Med-Surg South'
			WHEN cd.DEPARTMENT_NAME = 'ZZZ CHS ICU' THEN 'ICU South'
			WHEN cd.DEPARTMENT_NAME = 'ZZZ NWSH MED/SURG' THEN 'Med-Surg NWSH'	  
			ELSE NULL 
		END																					AS LocationName --NULL
		,COUNT(DISTINCT EVENT_ID)															AS TargetEvents
		,NULL /*CASE
			WHEN cd.DEPARTMENT_NAME = 'ZZZ CHN MED/SURG' THEN (COUNT(DISTINCT EVENT_ID)*11.2) 
			WHEN cd.DEPARTMENT_NAME = 'ZZZ CHS MED/SURG' THEN (COUNT(DISTINCT EVENT_ID)*11.2) 
			WHEN cd.DEPARTMENT_NAME = 'ZZZ CHS ICU' THEN (COUNT(DISTINCT EVENT_ID)*16.8) 
			WHEN cd.DEPARTMENT_NAME = 'ZZZ NWSH MED/SURG' THEN (COUNT(DISTINCT EVENT_ID)*30.0) 
			ELSE NULL
			END		*/																		AS LaborTarget

	FROM [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].[CLARITY_ADT] adtin

	LEFT JOIN map.LaborPayPeriods p
		ON CAST(adtin.EFFECTIVE_TIME AS date) = p.PayPeriodDate
	LEFT JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].[CLARITY_DEP] cd
		ON cd.DEPARTMENT_ID = adtin.DEPARTMENT_ID
			AND cd.department_name not like '%emergency%'
	LEFT JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].PAT_ENC_HSP eh
		ON eh.PAT_ENC_CSN_ID = adtin.PAT_ENC_CSN_ID
	INNER JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].HSP_ACCOUNT a
		ON eh.PAT_ENC_CSN_ID = a.PRIM_ENC_CSN_ID
			AND a.disch_loc_id IN (43004001, 43005005, 43006001) /*ZZZ CHN, ZZZ CHS, ZZZ NWSH*/
	
	WHERE 1=1 
		AND adtin.EVENT_TYPE_C = 3 -- transfer into -- 1 admit, 3 transfer in, 4 transfer out
		
	GROUP BY 
		cd.DEPARTMENT_ID
		,p.PayDate
		,cd.DEPARTMENT_NAME
		
--------------------------------------------------------------------------------------------------------------------
-- 3: all ER from Clarity ADT feed, grouping by department 
--------------------------------------------------------------------------------------------------------------------

UNION ALL 

	SELECT 
		p.PayDate																			AS PayDate
		,cd.DEPARTMENT_ID																	AS LocationID
		,CASE WHEN cd.DEPARTMENT_NAME = 'ZZZ CHN EMERGENCY' THEN 'ER North'
			  WHEN cd.DEPARTMENT_NAME = 'ZZZ CHS EMERGENCY' THEN 'ER South'
			  WHEN cd.DEPARTMENT_NAME = 'ZZZ NWSH EMERGENCY' THEN 'ER NWSH'
			  ELSE NULL END																	AS LocationName 
		,COUNT(DISTINCT EVENT_ID)															AS TargetEvents
		,NULL /*CASE WHEN cd.DEPARTMENT_NAME like '%EMERGENCY' THEN (COUNT(DISTINCT EVENT_ID)*3.0)
		ELSE null END*/																		AS LaborTarget
	FROM [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].[CLARITY_ADT] adtin

	LEFT JOIN map.LaborPayPeriods p 
		ON adtin.EFFECTIVE_TIME = p.PayPeriodDate
	LEFT JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].[CLARITY_DEP] cd 
		ON cd.DEPARTMENT_ID = adtin.DEPARTMENT_ID
	LEFT JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].PAT_ENC_HSP eh 
		ON eh.PAT_ENC_CSN_ID = adtin.PAT_ENC_CSN_ID
	INNER JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].HSP_ACCOUNT a 
		ON eh.PAT_ENC_CSN_ID = a.PRIM_ENC_CSN_ID
	
	WHERE 1=1 
		AND adtin.EVENT_TYPE_C IN(1,3) -- transfer into -- 1 admit, 3 transfer in, 4 transfer out
		AND a.disch_loc_id IN (43004001, 43005005, 43006001) /*ZZZ CHN, ZZZ CHS, ZZZ NWSH*/
		AND cd.Department_name like '%EMERGENCY'

	GROUP BY 
		cd.DEPARTMENT_ID
		,p.PayDate
		,cd.DEPARTMENT_NAME
		
--------------------------------------------------------------------------------------------------------------------
-- 4: all CT units coming from our data warehouse
--------------------------------------------------------------------------------------------------------------------

UNION ALL 

	SELECT
		t.[TransactionDepartmentID]															AS LocationID
		,CASE	WHEN dept.DepartmentName = 'ZZZ CHN CT' THEN 'Imaging CT North'
				WHEN dept.DepartmentName = 'ZZZ CHS CT' THEN 'Imaging CT South'
				ELSE NULL 
		END																					AS LocationName
		,p.PayDate																			AS PayDate
		--,d.FirstDayOfMonth
		--,d.[Date]
		,SUM(CASE WHEN t.TransactionIsCTCharge = 'Yes' THEN t.TransactionUnits ELSE 0 END)	AS TargetEvents
		,NULL																				AS LaborTarget
	FROM [fact].[vTransactions2] t

	LEFT JOIN map.LaborPayPeriods p 
			ON cast(t.TransactionDateOfService as date) = P.PayPeriodDate
	LEFT JOIN dim.Dates d 
		ON d.[Date] = convert(date,t.TransactionDateOfService)
	LEFT JOIN dim.Departments dept 
		ON dept.DepartmentID = t.TransactionDepartmentID

	WHERE 1=1
		AND t.TransactionBillingType = 'HB'
		AND t.TransactionType = 'Charge'
		AND t.TransactionStatus = 'Active'
		AND t.TransactionIsCTCharge = 'Yes'
		AND t.TransactionDateOfService >= '1/1/2023'
	GROUP BY 
		t.[TransactionDepartmentID]
		,p.PayDate
		,dept.DepartmentName

--------------------------------------------------------------------------------------------------------------------
-- 5: all MRI units, coming from our data warehouse
--------------------------------------------------------------------------------------------------------------------

UNION ALL

	SELECT
		t.[TransactionDepartmentID]															AS LocationID
		,CASE WHEN dept.DepartmentName = 'ZZZ CHS MRI' THEN 'Imaging MRI South'
			  WHEN dept.DepartmentName = 'ZZZ CHN MRI' THEN 'Imaging MRI North'
			  ELSE NULL END																	AS LocationName
	,p.PayDate																				AS PayDate
		--,d.FirstDayOfMonth
		--,d.Date
		,SUM(CASE WHEN t.TransactionIsMRICharge = 'Yes' THEN t.TransactionUnits ELSE 0 END)	AS TargetEvents
		,NULL																				AS LaborTarget
	FROM [fact].[vTransactions2] t

	LEFT JOIN map.LaborPayPeriods p 
		ON cast(t.TransactionDateOfService as date) = P.PayPeriodDate
	LEFT JOIN dim.Dates d 
		ON d.Date = convert(date,t.TransactionDateOfService)
	LEFT JOIN dim.Departments dept 
		ON dept.DepartmentID = t.TransactionDepartmentID

	WHERE 1=1
		AND t.TransactionBillingType = 'HB'
		AND t.TransactionType = 'Charge'
		AND t.TransactionStatus = 'Active'
		AND t.TransactionIsMRICharge = 'Yes'
		AND t.TransactionDateOfService >= '1/1/2023'
	GROUP BY 
		t.[TransactionDepartmentID]
		,p.PayDate
		,dept.DepartmentName


--------------------------------------------------------------------------------------------------------------------
-- 6: all ultrasound units, coming from our data warehouse 
--------------------------------------------------------------------------------------------------------------------

UNION ALL

	SELECT
		t.[TransactionDepartmentID]															AS LocationID
		,CASE WHEN dept.DepartmentName = 'ZZZ CHS ULTRASOUND' THEN 'Imaging Ultrasound South'
			  WHEN dept.DepartmentName = 'ZZZ CHN ULTRASOUND' THEN 'Imaging Ultrasound North'
			  ELSE NULL END																	AS LocationName
		,p.PayDate																			AS PayDate
		--,d.FirstDayOfMonth
		--,d.[Date]
		,SUM(CASE WHEN t.TransactionIsUltrasoundCharge = 'Yes' THEN t.TransactionUnits ELSE 0 END)		AS TargetEvents
		,NULL 																				AS LaborTarget
	FROM [fact].[vTransactions2] t

	LEFT JOIN map.LaborPayPeriods p 
		ON cast(t.TransactionDateOfService as date) = P.PayPeriodDate
	LEFT JOIN dim.Dates d 
		ON d.Date = convert(date,t.TransactionDateOfService)
	LEFT JOIN dim.Departments dept 
		ON dept.DepartmentID = t.TransactionDepartmentID
	WHERE 1=1
		AND t.TransactionBillingType = 'HB'
		AND t.TransactionType = 'Charge'
		AND t.TransactionStatus = 'Active'
		AND t.TransactionIsUltrasoundCharge = 'Yes'
		AND t.TransactionDateOfService >= '1/1/2023'
	GROUP BY 
		t.[TransactionDepartmentID]
		,p.PayDate
		,dept.DepartmentName

--------------------------------------------------------------------------------------------------------------------
-- 7: all cases, still grouped by locations like the first query, but this time using location logic just to group into PACU North/South/NWSH
--------------------------------------------------------------------------------------------------------------------

UNION ALL

	SELECT 
		c.VisitCaseLocationID														AS LocationID
		,p.PayDate																	AS PayDate
		,COUNT(DISTINCT c.VisitCaseID)												AS TargetEvents -- Surgical Cases
		,CASE WHEN l.LocationName like '%CHN%' then 'PACU North'
			WHEN l.LocationName like '%CHS%' then 'PACU South'
			WHEN l.LocationName like '%NWSH%' then 'PACU NWSH'
			ELSE NULL end															AS LocationName 
		,NULL /*COUNT(DISTINCT c.VisitCaseID) * 1*/									AS LaborTarget
	FROM fact.VisitCases c

	LEFT JOIN map.LaborPayPeriods p
		ON c.VisitCaseServiceDate = p.PayPeriodDate
	LEFT JOIN dim.Locations l 
		ON l.LocationID = c.VisitCaseLocationID

	WHERE 1=1
		AND c.VisitCaseLogStatus = 'Posted'
		AND p.PayDate IS NOT NULL
	GROUP BY
		c.VisitCaseLocationID
		,p.PayDate
		,l.LocationName

--------------------------------------------------------------------------------------------------------------------
-- 8: all cases, still grouped by locations like the first query, but this time using location logic just to group into Sterile Processing North/South/NWSH 
--------------------------------------------------------------------------------------------------------------------

UNION ALL

	SELECT 
		c.VisitCaseLocationID															AS LocationID
		,p.PayDate																		AS PayDate
		--,c.VisitCaseService
		,COUNT(DISTINCT c.VisitCaseID)													AS TargetEvents -- Surgical Cases
		,CASE WHEN l.LocationName like '%CHN%' then 'Sterile Processing North'
			WHEN l.LocationName like '%CHS%' then 'Sterile Processing South'
			WHEN l.LocationName like '%NWSH%' then 'Sterile Processing NWSH'
			ELSE NULL end																AS LocationName
		,NULL /*COUNT(DISTINCT c.VisitCaseID) * 1*/										AS LaborTarget
	FROM fact.VisitCases c

	LEFT JOIN map.LaborPayPeriods p 
		ON cast(c.VisitCaseServiceDate AS Date) = p.PayPeriodDate
	LEFT JOIN dim.Locations l 
		ON l.LocationID = c.VisitCaseLocationID

	WHERE 1=1
		AND c.VisitCaseLogStatus = 'Posted'
		AND (l.LocationName LIKE '%CHN%' OR 
			l.LocationName LIKE '%CHS%' OR 
			l.LocationName LIKE '%NWSH%')
	GROUP BY 				   
		c.VisitCaseLocationID   
		,p.PayDate
		,l.LocationName

--------------------------------------------------------------------------------------------------------------------
-- 9: all GI coming from Clarity ADT using transfer in logic and grouping by location
--------------------------------------------------------------------------------------------------------------------

UNION ALL

	SELECT 
		p.PayDate																					AS PayDate
		,cd.DEPARTMENT_ID																			AS LocationID
		,CASE WHEN cd.DEPARTMENT_NAME = 'ZZZ CHS ENDOSCOPY' THEN 'Gastro South'
			  ELSE NULL END																			AS LocationName
		,COUNT(DISTINCT EVENT_ID)																	AS TargetEvents
		,NULL /*CASE WHEN cd.DEPARTMENT_NAME like '%endo%' THEN (COUNT(DISTINCT EVENT_ID)*0.9) 
		else null end*/																				AS LaborTarget

	FROM [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].[CLARITY_ADT] adtin

	LEFT JOIN map.LaborPayPeriods p 
		ON adtin.EFFECTIVE_TIME = p.PayPeriodDate
	LEFT JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].[CLARITY_DEP] cd
		ON cd.DEPARTMENT_ID = adtin.DEPARTMENT_ID
	LEFT JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].PAT_ENC_HSP eh 
		ON eh.PAT_ENC_CSN_ID = adtin.PAT_ENC_CSN_ID
	INNER JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[CLARITY].[ORGFILTER].HSP_ACCOUNT a  
		ON eh.PAT_ENC_CSN_ID = a.PRIM_ENC_CSN_ID
	
	WHERE 1=1 
		AND adtin.EVENT_TYPE_C IN(1,3) -- transfer into -- 1 admit, 3 transfer in, 4 transfer out
		AND a.disch_loc_id IN (43004001, 43005005, 43006001) /*ZZZ CHN, ZZZ CHS, ZZZ NWSH*/
		AND cd.Department_name like '%end%'
		AND cd.Department_name not like '%CHN%'

	GROUP BY
		cd.DEPARTMENT_ID
		,p.PayDate
		,cd.DEPARTMENT_NAME
		

) sub

LEFT JOIN [map].[LaborStatsTargets] lst
	ON sub.LocationName = lst.Department
		AND lst.Department IS NOT NULL

WHERE 1=1
	AND sub.PayDate IS NOT NULL
