SET NOCOUNT ON

/****** PULL HISTORIC AND FUTURE CHURN BY ACCOUNT AND REASON CODES  ******/
/*
DATE: 20200210 
GOAL: ADD FUTURE PIPELINE CHURN REASON CODES TO THE CHURN SCORECARD

OUTPUT: 
  On Account level, per reference monthly run
	6 MONTH FUTURE CHURN REASON CODES 

*/

--CURRENT TMK RUN 

DECLARE @TMK AS INT
DECLARE @TMK_6_PRIOR AS INT
DECLARE @TMK_6_FUTURE AS INT
SET @TMK = (SELECT LEFT(CONVERT(varchar(6),DATEADD(MONTH, -1,  GETDATE()),112),6))
SET @TMK_6_PRIOR =(SELECT LEFT(CONVERT(varchar(6),DATEADD(MONTH, -7,  GETDATE()),112),6))
SET @TMK_6_FUTURE = (SELECT LEFT(CONVERT(varchar(6),DATEADD(MONTH, 5,  GETDATE()),112),6))

IF OBJECT_ID('tempdb..#RC') IS NOT NULL DROP TABLE #RC;

SELECT DISTINCT
    Account_number
    ,churn_type
	,churn_type + ' | ' + CASE WHEN [reason_level_1] like '%|%'THEN LEFT([reason_level_1], CHARINDEX(' |', [reason_level_1]) ) ELSE [reason_level_1] end as reason_code
INTO #RC
FROM [480075-EA-REP\EA_REP].[Churn].[dbo].[Churn_System_Pull_Rolling_6_Months]
WHERE  ([month] >  @TMK and [month] <= @TMK_6_FUTURE )
and churn_type <> 'Migration'

SELECT DISTINCT
Account_number
,  SUBSTRING(
	(
	SELECT  '| ' +   Reason_code AS 'data()' 
	FROM  #RC r where r.Account_Number = a.Account_Number  FOR XML PATH('')
	), 2 , 9999) as reason_code
FROM [480075-EA-REP\EA_REP].[Churn].[dbo].[Churn_System_Pull_Rolling_6_Months] a
WHERE ([month] >  @TMK and [month] <= @TMK_6_FUTURE )
	and churn_type <> 'Migration'
order by cast(account_number as int)
