
BEGIN TRY
------------------------------------------------------------------------------------------------------------
/* Delete dupicate denial values */
------------------------------------------------------------------------------------------------------------

PRINT 'Deleting old denial records...'
DELETE fact.PBDenials
	WHERE 1=1
		--AND DenialUpdateDate >= @QueryBeginDate -- testing

------------------------------------------------------------------------------------------------------------
/* Insert new denial values */
------------------------------------------------------------------------------------------------------------

PRINT 'Inserting new denial records...'
INSERT INTO fact.PBDenials(
     DenialID
	 ,DenialTransactionID
	 ,DenialTransactionChargeID
	 ,ChargePostingDate
	 ,ChargeServiceDate
	 ,DenialProcedureID
	 ,DenialPatientID
	 ,DenialLocationID
	 ,DenialDepartmentID
	 ,DenialBillingProviderID
	 ,DenialServiceProviderID
	 ,DenialPayerID
	 ,DenialPayerPlanID
	 ,DenialProcedureCode
	 ,DenialDiagnosisID
	 ,DenialInvoiceNumber
	 ,DenialReasonCode
	 ,DenialReasonDescription
	 ,DenialCategory
	 ,DenialCause
	 ,DenialRootCause
	 ,DenialReasonGroup
	 ,DenialResolutionCategory
	 ,DenialStatus
	 ,DenialIsPrimary
	 ,DenialIsFirst
	 ,DenialPostingDate
	 ,DenialCompletionDate
	 ,DenialDaysToClose
	 ,DenialAmount
	 ,DenialRecoveryAmount
	 ,DenialAdjustmentAmount
	 ,DenialBillingAmount
	 ,DenialUpdateDate
)


SELECT
	d.BDC_ID											AS DenialID
	,CONCAT('5~',d.PAYMENT_TRANSACTION_ID)				AS DenialTransactionID
	,CONCAT('5~',d.CHARGE_TRANSACTION_ID)				AS DenialTransactionChargeID
	,d.CHARGE_POST_DATE									AS ChargePostingDate
	,d.CHARGE_SERVICE_DATE								AS ChargeServiceDate
	,CONCAT('5~',d.CHARGE_PROCEDURE_ID)					AS DenialProcedureID
	,CONCAT('5~',d.PATIENT_ID)							AS DenialPatientID
	,CONCAT('5~',d.LOCATION_ID)							AS DenialLocationID
	,CONCAT('5~',d.DEPARTMENT_ID)						AS DenialDepartmentID
	,CONCAT('5~',d.BILLING_PROVIDER_ID)					AS DenialBillingProviderID
	,CONCAT('5~',d.SERVICE_PROVIDER_ID)					AS DenialServiceProviderID
	,CONCAT('5~',d.PAYER_ID)							AS DenialPayerID
	,CONCAT('5~',d.BENEFIT_PLAN_ID)						AS DenialPayerPlanID
	,d.CHARGE_PROCEDURE_ID								AS DenialProcedureCode
	,d.PRIMARY_DIAGNOSIS_ID								AS DenialDiagnosisID
	,d.INVOICE_NUMBER									AS DenialInvoiceNumber
	,dr.REASON_CODE										AS DenialReasonCode
	,dr.REASON_CODE_NAME								AS DenialReasonDescription
	,d.DENIAL_CATEGORY									AS DenialCategory
	,d.DENIAL_CAUSE										AS DenialCause
	,d.ROOT_CAUSE										AS DenialRootCause
	,d.BDC_REASON_CODE_GROUP							AS DenialReasonGroup
	,d.RESOLUTION_CATEGORY								AS DenialResolutionCategory
	,d.DENIAL_STATUS									AS DenialStatus
	,d.PRIMARY_DENIAL_FLAG								AS DenialIsPrimary
	,d.FIRST_DENIAL_FLAG								AS DenialIsFirst
	,d.DENIAL_POST_DATE									AS DenialPostingDate
	,d.DENIAL_COMPLETE_DATE								AS DenialCompletionDate
	,d.DAYS_TO_CLOSE									AS DenialDaysToClose
	,d.DENIED_AMOUNT									AS DenialAmount
	,d.RECOVERY_AMOUNT									AS DenialRecoveryAmount
	,d.ADJUSTMENT_AMOUNT								AS DenialAdjustmentAmount
	,d.BILLED_AMOUNT									AS DenialBillingAmount
	,GETDATE()											AS DenialUpdateDate
	--,d.* 
FROM [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[Clarity].[ORGFILTER].V_CUBE_F_PB_DENIALS d
LEFT JOIN [CLARITYRDBMS.CORP.ZZZZZZZZZZZZZZZZZZ.COM].[Clarity].[ORGFILTER].X_CUBE_D_REASON_CODE dr
	ON d.REASON_CODE_ID = dr.REASON_CODE_ID


--WHERE 1=1

;END TRY
BEGIN CATCH
	PRINT ERROR_MESSAGE()
	PRINT 'ROLLING BACK DELETION'
	ROLLBACK;
END CATCH;
