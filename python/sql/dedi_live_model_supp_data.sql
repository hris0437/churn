SET NOCOUNT ON


--=====================================================================================================================================
--base table for output
--=====================================================================================================================================
select account_number
	,time_month_key
	,time_month_key_dt
	,[total_invoiced_in_month]
	,[annualrevenue_original] as annualrevenue
	,[num_distinct_account_manager]
	,[num_distinct_account_bdc]
	,[num_opportunities_last_3_months]
	,[pct_opportunities_won_last_3_months]
	,[pct_of_devices_with_contract_status_eq_in_contract_risk_of_lapse_90_days] as pct_devices_90d
	,[average_active_device_tenure_months]
	,[acc_tenure_in_months]
	,[does_account_have_mpc]
	,[does_account_have_ospc]
into #a
from [CustomerRetention].[dbo].[00FINAL_dedicated] aa
where time_month_key = (select max(time_month_key) from [CustomerRetention].[dbo].[00FINAL_dedicated])



--=====================================================================================================================================
-- account name, AM, team name
--=====================================================================================================================================
select *
into #d
from
(
	SELECT *,  rowno = row_number() over(partition by account_number order by tmk desc)
	from (
		select tmk
			,account_name
			,account_number
			,account_team_name
			,account_manager
		FROM [CustomerRetention].[dbo].[zz_staging_account_data_all_dw] with (nolock)
		where [account_type_new] = 'dedicated'
--		group by [Account_Name], Account_Number, Account_Team_Name, Account_Manager
		) c
) b
where rowno=1



--=====================================================================================================================================
-- region
--=====================================================================================================================================
Select [name]
	,[region]
into #b
FROM [ods].[SS_DB_ODS].[dbo].[team_all] with (nolocK)
where deleted_at = '1970-01-01 00:00:01.000'




--=====================================================================================================================================
-- team subsegment
--=====================================================================================================================================
select distinct account_number
	,time_month_key
	,team_sub_segment
into #s
from [480072-EA].[Net_Revenue].[dbo].[Net_Revenue_Detail] with (nolock)
where line_of_business='DEDICATED'



--=====================================================================================================================================
--NPS scores (with nulls)
--=====================================================================================================================================
select *
into #n
from
[CustomerRetention].[dbo].[zz_staging_nps_04_dedicated] n





--=====================================================================================================================================
--open pipeline opportunities by account
--=====================================================================================================================================
---Pulling 12 month forward looking pipeline 
select distinct 
	OpportunityId = dop.Opportunity_Id,
	OpportunityStage = dop.Opportunity_Stage_Name,
	OpportunityFinalTypePlatform = dop.Opportunity_Final_Type_Platform,
	AccountNumber = da.Account_Number
into 
	#Table2 
from	
	[ebi-datamart].[dwh_db].[dbo].[Fact_SF_Rpt_Opportunity_State] os with(nolock)
		inner join 
	[ebi-datamart].[dwh_db].[dbo].[Dim_SF_Opportunity_Amounts] a with(nolock)
		on os.opportunity_amounts_key = a.opportunity_amounts_key
		inner join 
	[ebi-datamart].[dwh_db].[dbo].[Dim_SF_Opportunity] dop with(nolock)
		on os.opportunity_key = dop.opportunity_key
		inner join 
	[ebi-datamart].[dwh_db].[dbo].[Dim_SF_User] do with(nolock)
		on os.owner_key = do.owner_key
		inner join 
	[ebi-datamart].[dwh_db].[dbo].[Dim_SF_Account] da with(nolock)
		on os.account_key = da.account_key
		inner join 
	[ebi-datamart].[dwh_db].[dbo].[dim_time] dd with(nolock)
		on os.close_date_key = dd.time_Key
		inner join
	[ebi-datamart].[dwh_db].[dbo].[Dim_SF_User] dpo with(nolock)
		on os.partner_owner_key = dpo.owner_key
		inner join
	[ebi-datamart].[dwh_db].[dbo].[Dim_SF_CurrencyConversion] dcr with(nolock)
		ON dd.time_full_date >= dcr.startdate
		AND dd.time_full_date < dcr.nextstartdate
		AND dop.opportunity_currency_iso_code = dcr.Currency_FromISOCode
		AND dcr.Currency_ToISOCode = 'USD'
where 
	isnull(dop.Opportunity_Category,'') <> 'Renewal'
	and dop.Opportunity_Stage_Name <>'Stage 0'
	and dop.Opportunity_Forecast_Category not in ('Closed','Omitted')
	and do.Owner_Role not in ('us fanatical aws specialist','us inactive','LATAM - SDR','LATAM - Transactional','US VP - Enterprise SE','')
	and year(dd.Time_Full_Date)*100+month(dd.Time_Full_Date) between CONVERT(VARCHAR(6),GETDATE(),112) and CONVERT(VARCHAR(6),DATEADD(YYYY,1,GETDATE()),112)
	and dcr.Currency_Delete_Flag = 'N'
	and dop.Opportunity_Deleted = 'N'
	and da.Account_Name not like '%Test Account%'
	and Opportunity_Non_Countable = 'false'
	and os.Current_Record = 1

--Pulling clean up opps to clean the pipeline

Select  distinct
	Opportunity_ID, 'Yes' as Cleanup
into
	#Cleanup
from 
	[ods].[Operational_Reporting_SFDC].[dbo].[QOPPORTUNITY] with (nolock)
where 
	Delete_Flag = 'N'
and 
	[WHY_DID_WE_LOSE] like 'Cleanup%';

--Removing clean up opps and filtering on dedicated opps	
select
	op.AccountNumber,
	Number_Of_Opportunities_In_pipeline = isnull(count(*),0)
into #o 
from #Table2 op
left join
#Cleanup clup
on op.opportunityID = clup.opportunity_id

where OpportunityStage like '%Stage%'
	 and (case when clup.Opportunity_Id is null then 0 else 1 end) = 0
	 and op.OpportunityFinalTypePlatform = 'Dedicated'
group by op.AccountNumber
order by op.AccountNumber


--=====================================================================================================================================
-- team name, business unit and segment
--=====================================================================================================================================
select team_name_oracle
	,churn_level_1
	,churn_level_2
	,churn_level_3
	into #q
from [480072-EA].[Report_Tables].[dbo].[Dim_Support_Team_Hierarchy]
where Legacy_Flag = 0

    

--=====================================================================================================================================
-- minimum device online date (for true account tenure)
--=====================================================================================================================================
SELECT [Device_Assigned_Account_Number] as account_number
		,min([Device_Online_Date]) as min_date
into #u
FROM [ebi-datamart].[Corporate_DMART].[dbo].[Dim_Device]
where [Current_Record]=1
and device_number>10
and Device_Online_Date not in ('1900-01-01 00:00:00.000','1969-12-31 18:00:00.000')
group by Device_Assigned_Account_Number



--=====================================================================================================================================
--3 months historical renewal opportunities
--=====================================================================================================================================
select AccountNumber
	,renewal_opportunities_closed_historical_3months = sum(closedopps)
	,renewal_opportunities_won_historical_3months = sum(wonopps)
	,pct_Renewal_opportunities_won_historical_3months = cast(sum(wonopps) as float)/nullif(count(wonopps),0)
into #r1
from 
	(
		select distinct 
		AccountNumber = qa.Account_Number,
		closedopps = count(distinct(o.Opportunity_Id)),
		wonopps = case when o.iswon = 'true' then 1 else 0 end
        
		from [ODS].[Operational_Reporting_SFDC].[dbo].[QOPPORTUNITY] o with(nolock)
		left join [ODS].[Operational_Reporting_SFDC].[dbo].[QACCOUNTS] qa WITH(NOLOCK)
		on o.ACCOUNTX = qa.ID
		where o.Delete_Flag = 'N'
		--and a.Delete_Flag = 'N'
		and year(o.CloseDate)*100+month(o.CloseDate) between year(dateadd(m,-4,getdate()))*100+month(dateadd(m,-4,getdate())) and year(dateadd(m,-1,getdate()))*100+month(dateadd(m,-1,getdate())) 
		and ISCLOSED in ('true')
		and o.Non_Bookable_Revenue = 'False'
		and o.category  in ('Renewal', 'Recompete - Downgrade', 'Recompete - Straight')
		group by qa.Account_Number, o.iswon
	) ro
		group by ro.AccountNumber



--=====================================================================================================================================
-- renewal opportunities in pipeline
--=====================================================================================================================================
select 
	ac.Account_Number
	,count(distinct(a.Opportunity_Id)) as renewal_opportunities_in_pipeline
	,count(case when lower(stagename) in ('stage 3 - proposal & quote','stage 4 - negotiation & quote mod','stage 5 - closing the business') then 1 else 0 end)
		as stage3_plus_cnt
into #r2
from [ODS].[Operational_Reporting_SFDC].[dbo].[QOPPORTUNITY] a with(nolock)
left join [ODS].[Operational_Reporting_SFDC].[dbo].[QACCOUNTS] ac WITH(NOLOCK)
	on a.ACCOUNTX = ac.ID
where a.Delete_Flag = 'N'
	and ISCLOSED in ('false','no')
	and a.Non_Bookable_Revenue = 'False'
	and a.category  in ('Renewal', 'Recompete - Downgrade', 'Recompete - Straight')
	and ac.Account_Number is not null
group by ac.Account_Number



--=====================================================================================================================================
--churn forecast dollars next 6 months
--=====================================================================================================================================
SELECT 
	fa.account_number
	,abs(sum(adjust_full_proj_dollar_amt)) as future_6m_churn
into #f
FROM [480075-EA-REP\EA_REP].[Churn].[dbo].[Churn_System_Pull_Rolling_6_Months] fa
inner join
(
	select account_number, max(time_month_key) as time_month_key from [CustomerRetention].[dbo].[AMT_FINAL_AMT_dedicated_live_allvariables]
	group by Account_Number
) fb
on fa.Account_Number = fb.Account_Number
WHERE churn_type <> 'Migration'
	and fa.month > fb.time_month_key 
	and fa.month <= (cast(year((dateadd(month, 6, convert(date,LEFT(fb.time_month_key,4)+'-'+RIGHT(fb.time_month_key,2)+'-01',120)))) as varchar) 
	+ convert(varchar(2), (dateadd(month, 6, convert(date,LEFT(fb.time_month_key,4)+'-'+RIGHT(fb.time_month_key,2)+'-01',120))), 101))
GROUP BY fa.account_number




--=====================================================================================================================================
--churn historic dollars 6 months
--=====================================================================================================================================
select 
	ha.account_number
	,abs(sum([final_total_churn])) as historic_6m_churn
into #h
FROM [480075-EA-REP\EA_REP].[Churn].[dbo].[report_churn_history_v2] ha
inner join
(
	select account_number, max(time_month_key) as time_month_key from [CustomerRetention].[dbo].[AMT_FINAL_AMT_dedicated_live_allvariables]
	group by Account_Number
) hb
on ha.Account_Number = hb.Account_Number
WHERE churn_type <> 'Migration'
	and ha.time_month_key <= hb.time_month_key 
	and ha.time_month_key >= (cast(year((dateadd(month, -5, convert(date,LEFT(hb.time_month_key,4)+'-'+RIGHT(hb.time_month_key,2)+'-01',120)))) as varchar) 
	+ convert(varchar(2), (dateadd(month, -5, convert(date,LEFT(hb.time_month_key,4)+'-'+RIGHT(hb.time_month_key,2)+'-01',120))), 101))
GROUP BY ha.account_number




--=====================================================================================================================================
--Total discount data
--=====================================================================================================================================
select a.core_account_number
	, a.close_date
	,  a. created_at as contract_created
	--, c.device_number
	--, '' core_platform_type
	,(c.previous_mrr- c.mrr)  as discount_amt
	,case when (c.previous_mrr <>0 )and c.mrr < c.previous_mrr 
			then ((c.previous_mrr- c.mrr) /c.previous_mrr)
			else 0 end as discount_perc
	,d.code
	,ea.exchange_rate_exchange_rate_value
into #raptor
from [ods].[raptor_ods].[dbo].[contracts] a
join [ods].[raptor_ods].[dbo].[work_items] b 
on a.id = b.workable_item_id and b.workable_item_type = 'contract'

join [ods].[raptor_ods].[dbo].[devices] c 
on b.id = c.work_item_id

join [ods].[raptor_ods].[dbo].[currencies] d 
on a.currency_id = d.id

join 
(
	select distinct core_account_number
	, max(close_date) as close_date 
	from [ods].[raptor_ods].[dbo].[contracts] c 
	where core_account_number in 
	(
		select distinct account_number 
		from [customerretention].[dbo].[amt_final_amt_dedicated_live_allvariables]
	)
	and c.category = 'renewal' and workflow_state = 'complete'
	group by core_account_number
) dt 
on dt. close_date = a.close_date 

left join 
(
	select [exchange_rate_from_currency_code]
		,[exchange_rate_from_currency_description]
		,[exchange_rate_to_currency_code]
		,[exchange_rate_exchange_rate_value]
	from [480072-ea].[net_revenue].[dbo].[report_exchange_rate]
	where exchange_rate_time_month_key = convert(varchar(6),dateadd(m,-1,getdate()),112)
	and exchange_rate_to_currency_description = 'US Dollar'
	and exchange_rate_from_currency_code in ('USD','GBP','EUR','AUD','HKD')
) ea
on ea.[exchange_rate_from_currency_code] = d.code	
where a.category = 'renewal' and workflow_state = 'complete'

union all

select a.core_account_number
	,a.close_date
	,a. created_at contract_created
	--, '' device_number
	--, e.name as core_platform_type
	,(c.previous_mrr- c.mrr)  as discount_amt
	,case when (c.previous_mrr <>0 )and c.mrr < c.previous_mrr 
		then ((c.previous_mrr- c.mrr) /c.previous_mrr)
		else 0 end as discount_perc
	,d.code
	,ea.exchange_rate_exchange_rate_value
from  [ods].[raptor_ods].[dbo].[contracts] a
		
join  [ods].[raptor_ods].[dbo].[work_items] b 
on a.id = b.workable_item_id and b.workable_item_type = 'contract'

join  [ods].[raptor_ods].[dbo].[products] c 
on b.id = c.work_item_id

join  [ods].[raptor_ods].[dbo].[product_types] e
on c.product_type_id = e.id

join [ods].[raptor_ods].[dbo].[currencies] d 
on a.currency_id = d.id
		
join 
(
	select distinct core_account_number
	, max(close_date) as close_date 
	from   [ods].[raptor_ods].[dbo].[contracts] c 
	where c.category = 'renewal' and workflow_state = 'complete'
	group by core_account_number
) dt 
on dt. close_date = a.close_date 

left join 
(
	select [exchange_rate_from_currency_code]
		,[exchange_rate_from_currency_description]
		,[exchange_rate_to_currency_code]
		,[exchange_rate_exchange_rate_value]
	from [480072-ea].[net_revenue].[dbo].[report_exchange_rate]
	where exchange_rate_time_month_key = convert(varchar(6),dateadd(m,-1,getdate()),112)
	and exchange_rate_to_currency_description = 'US Dollar'
	and exchange_rate_from_currency_code in ('USD','GBP','EUR','AUD','HKD')
) ea
on ea.[exchange_rate_from_currency_code] = d.code	
where a.category = 'renewal' and workflow_state = 'complete'



select core_account_number as account_number
	, max(close_date) as close_date
	, max(contract_created) as contract_created
	, sum(discount_amt*exchange_rate_exchange_rate_value) as discount_amt
	, avg(discount_perc) as avg_discount_rate	
into #dd
from #raptor
group by core_account_number

--3mo discount data
select core_account_number as account_number
	, max(close_date) as close_date
	, max(contract_created) as contract_created
	, sum(discount_amt*exchange_rate_exchange_rate_value) as discount_amt
	, avg(discount_perc) as avg_discount_rate	
into #dd3
from #raptor
where close_date >= convert(varchar(10),dateadd(dd,-(day(dateadd(mm,-3,getdate()))-1),dateadd(mm,-3,getdate())),101)
group by core_account_number

--6mo discount data
select core_account_number as account_number
	, max(close_date) as close_date
	, max(contract_created) as contract_created
	, sum(discount_amt*exchange_rate_exchange_rate_value) as discount_amt
	, avg(discount_perc) as avg_discount_rate	
into #dd6
from #raptor
where close_date >= convert(varchar(10),dateadd(dd,-(day(dateadd(mm,-6,getdate()))-1),dateadd(mm,-6,getdate())),101)
and close_date < convert(varchar(10),dateadd(dd,-(day(dateadd(mm,-3,getdate()))-1),dateadd(mm,-3,getdate())),101)
group by core_account_number


-- Discounts and contract renewals
select distinct base.account_number, 
case when convert(varchar(6),dd.contract_created,112) is not null then  convert(varchar(6),dd.contract_created,112) end recent_contract_renewed_tmk,
case when convert(double precision,(round(dd.discount_amt,2))) is not null 
	and  convert(double precision,(round(dd.discount_amt,2))) > 0
	then convert(double precision,(round(dd.discount_amt,2))) 
	end recent_contract_renewal_discount_amt,
case when convert(double precision,(round(dd.avg_discount_rate,2))) is not null 
	and   convert(double precision,(round(dd.avg_discount_rate,2))) > 0
	then convert(double precision,(round(dd.avg_discount_rate,2)))  
	end recent_renewal_discount_perc,
case when 	convert(double precision,(round(dd6.discount_amt,2))) is not null 
	and  convert(double precision,(round(dd6.discount_amt,2))) > 0
	then convert(double precision,(round(dd6.discount_amt,2))) 
	end renewal_discount_amt_6_mos,	
case when 	convert(double precision,(round(dd6.avg_discount_rate,2))) is not null 
	and  convert(double precision,(round(dd6.avg_discount_rate,2))) > 0
	then convert(double precision,(round(dd6.avg_discount_rate,2))) 
	end renewal_discount_perc_6_mos,
case when 	convert(double precision,(round(dd3.discount_amt,2))) is not null 
	and  convert(double precision,(round(dd3.discount_amt,2))) > 0
	then convert(double precision,(round(dd3.discount_amt,2))) 
	end renewal_discount_amt_3_mos,	
case when 	convert(double precision,(round(dd3.avg_discount_rate,2))) is not null 
	and  convert(double precision,(round(dd3.avg_discount_rate,2))) > 0
	then  convert(double precision,(round(dd3.avg_discount_rate,2))) 
	end renewal_discount_perc_3_mos
into #e
from 
(
	select distinct Account_number from #a
) base
left join #dd dd on dd.account_number=base.account_number
left join #dd3 dd3 on dd3.account_number=base.account_number
left join #dd6 dd6 on dd6.account_number=base.account_number


--=====================================================================================================================================
--Ticket Scores
--=====================================================================================================================================
Select
	Account_Number,
	Resolution_Score,
	Response_Score,
	Movement_Score
INTO #ts
FROM [ticket_scoring].[dbo].[core_ticket_scores]
where time_month_key = (select max(time_month_key) from #a)


--=====================================================================================================================================
--Account open/closed status
--=====================================================================================================================================
select number, account_status, account_status_date
into #c
from
(
	select number
		,lower(accountstatus) as account_status
		,updateddate as account_status_date
		,row_number() over(partition by number order by updateddate desc) as rn
	from [ods].[CMS_ODS].[dbo].[Account_History]
	where lower(type)='managed_hosting'
) c
where rn=1


--=====================================================================================================================================
--=====================================================================================================================================
--=====================================================================================================================================
--=====================================================================================================================================
-- ASSEMBLE FINAL RESULT
--=====================================================================================================================================
--=====================================================================================================================================
--=====================================================================================================================================
--=====================================================================================================================================

select
	a.[account_number]
	,d.account_name
	,a.time_month_key
	,d.account_team_name as [account team name]
	,d.account_manager as [customer success manager]
	,s.[team_sub_segment]
	,a.[total_invoiced_in_month]
	,a.[annualrevenue] as [company annual revenue]
	,a.[num_distinct_account_manager] as [number of csms]
	,a.[num_distinct_account_bdc] as [number of bdcs]
	,[acc_tenure_in_months] as [account tenure in months]
	,coalesce(a.[num_opportunities_last_3_months],0) as [num opportunities last 3 months]
	,coalesce(r1.renewal_opportunities_closed_historical_3months,0) as renewal_opportunities_closed_historical_3months
	,coalesce(r1.renewal_opportunities_won_historical_3months,0) as renewal_opportunities_won_historical_3months
	,coalesce(r1.pct_renewal_opportunities_won_historical_3months,0) as pct_renewal_opportunities_won_historical_3months
	,coalesce(r2.renewal_opportunities_in_pipeline,0) as renewal_opportunities_in_pipeline
	,r2.stage3_plus_cnt
	,a.pct_devices_90d as [pct devices 90d]
	,a.average_active_device_tenure_months as [average active device tenure months]
	,datediff(mm,u.[min_date],a.time_month_key_dt)/12 as [account tenure in years] --gcp won't do floor trunc
	,b.region
	--		,q.churn_level_1 as region
	,q.churn_level_2 as business_unit
	,q.churn_level_3 as segment
	,n.last_survey_score as [latest nps response in last 12 mos]
	,coalesce(o.[number_of_opportunities_in_pipeline],0) as [num opps in pipeline for next 12 mos]
	,[does_account_have_mpc]
	,[does_account_have_ospc]
	,f.future_6m_churn
	,h.historic_6m_churn
	,e.recent_contract_renewed_tmk
	,e.recent_contract_renewal_discount_amt
	,e.recent_renewal_discount_perc
	,e.renewal_discount_amt_6_mos
	,e.renewal_discount_perc_6_mos
	,e.renewal_discount_amt_3_mos
	,e.renewal_discount_perc_3_mos
	,ts.resolution_score as ticket_resolution_score
	,ts.response_score as ticket_response_score
	,ts.movement_score as ticket_movement_score
	,c.account_status
	,c.account_status_date
        
	-- data check for duplicates in team_sub_segment
	--, row_number() over(partition by a.account_number order by team_sub_segment) as rn

	from #a a
    left outer join #d d
    on a.account_number=d.account_number

	left outer join #b b
	on d.account_team_name=b.name

	-- sub_segment
	left join #s s
	on a.account_number = s.account_number
	and a.time_month_key = s.time_month_key

	left join #n n
	on n.account_number=a.account_number and n.time_month_key=a.time_month_key
	
	left join #o o
	on o.accountnumber = a.account_number

	left join #q q
    on d.account_team_name = q.team_name_oracle
    
	left join #u u
	on u.account_number=a.account_number

	--3 months historical renewal opportunities
	left join #r1 r1
	on r1.accountnumber = a.account_number

	-- renewal opportunities in pipeline
	left join #r2 r2
	on r2.account_number = a.account_number

	--churn forecast dollars next 6 months
	left join #f f
	on f.account_number=a.account_number

	--churn historic dollars 6 months
	left join #h h
	on h.account_number=a.account_number

	left join #e e
	on e.account_number=a.account_number

	left join #ts ts
	on ts.account_number = a.account_number

	left join #c c
	on c.number = a.account_number

	--add to remove RAS-TCS accounts, which are cloud accounts, using dummy device (this will be fixed later in amt_base_dedicated table)
--where d.account_team_name<>'RAS-TCS'
order by cast(a.account_number as int)

/*
drop table #a
drop table #b
drop table #d
drop table #s
drop table #n
drop table #o
drop table #q
drop table #u
drop table #r1
drop table #r2
drop table #f
drop table #h
drop table #e
drop table #table2
drop table #cleanup
drop table #raptor
drop table #dd
drop table #dd3
drop table #dd6
drop table #ts
drop table #c
*/