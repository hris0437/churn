USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_sample_large_tables_dedicated]    Script Date: 3/20/2020 5:11:32 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_sample_large_tables_dedicated]
as



/*this script prepares a number of raw tables for use in subsequent scripts.
collect raw data and join to only accounts in base table list.
any additional transformations to columns in the raw tables (such as date conversions) are included in this script.
the sql from the siss packages that ran up to oct 2019 is incorporated directly in this stored procedure.
*/

--;
declare @date as varchar(6)
declare @monthstoload as integer= -60
select  @date= convert(varchar(6),dateadd(m,@monthstoload ,getdate()-1),112)
declare @year as integer=year(dateadd(year,-5,(dateadd(month,-1,getdate()-1))))

select distinct account_number,account_type
into [customerretention].[dbo].[zz_staging_acct_list]
from [customerretention].[dbo].[base_dedicated]
order by account_number

--==================================================================================================================================
--==================================================================================================================================
-- sample the revenue data table 
--==================================================================================================================================
--==================================================================================================================================

-- code moved to base table stored proc, to only hit nrd once

-- this stored proc assumes you've run base stored proc first

drop table [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_dedicated]

select a.* 
into [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_dedicated]
from [customerretention].[dbo].[zz_staging_revenue] a
inner join [customerretention].[dbo].[zz_staging_acct_list] b
on a.account_number = b.account_number


create nonclustered index ix_revenue_data_withnewkeys_sampled_dedicated
on [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_dedicated] ([account_number],[time_month_key])
include ([product_group],[transaction_type])
;

--==================================================================================================================================
--==================================================================================================================================
-- sample the sku data table (dedicated only)
--==================================================================================================================================
--==================================================================================================================================

drop table [customerretention].[dbo].[zz_staging_device_sku_data_sampled_dedicated] 

select distinct right(a.[time_month_key],len(a.[time_month_key]-2))  as [time_month_key]
      ,a.[account_number]
	  ,time_month_key_dt = cast('' as date)
      ,cast(a.[account_name] as varchar(255)) as [account_name]
      ,[device_number]
      ,[sku_number]
      ,[sku_name]
      ,[sku_description]
into [customerretention].[dbo].[zz_staging_device_sku_data_sampled_dedicated] 
from [ebi-datamart].[corporate_dmart].[dbo].[vw_sku_assignment] a  
inner join [customerretention].[dbo].[zz_staging_acct_list] b
	on cast(a.account_number as varchar) = cast(b.account_number as varchar)
where [time_month_key]> = @date

update [customerretention].[dbo].[zz_staging_device_sku_data_sampled_dedicated] 
set time_month_key_dt = convert(date, cast(time_month_key as varchar) + '01')

create nonclustered index ix_device_sku_data_sampled_dedicated
on [customerretention].[dbo].[zz_staging_device_sku_data_sampled_dedicated] ([account_number])
include ([time_month_key],[sku_number])
;


--==================================================================================================================================
--==================================================================================================================================
-- ticketing
--==================================================================================================================================
--==================================================================================================================================


				-- sample ticket events data 
				--drop table [ticket_events_data_sampled_dedicated] 
				--select a.accountnumber,
				--	a.account_type,
				--	a.ticketsourcesystem,
				--	a.ticketnumber,
				--	ticketcreateddate = cast(a.ticketcreateddate as datetime),
				--	a.ticketsubject,
				--	a.ticketprivateflag,
				--	a.ticketcreatedby,
				--	a.ticketcreatedsource,
				--	a.ticketauthorsso,
				--	a.ticketnpst_rating,
				--	a.ticketnpst_score,
				--	a.ticketeventtype,
				--	a.ticketeventdescription,
				--	ticketeventdate = cast(a.ticketeventdate as datetime)
				--into [ticket_events_data_sampled_dedicated] 
				--from [ticket_events_dw] a
				--inner join [customerretention].[dbo].[zz_staging_acct_list] b 
				--on cast(a.accountnumber as varchar) = cast(b.account_number as varchar) 
				--	and a.account_type = b.account_type
				--where ticketcreatedby <> 'auto'
				--;

				--create nonclustered index ix_ticket_event_data_sample_dedicated
				--on [ticket_events_data_sampled_dedicated] ([accountnumber],[account_type], [ticketeventdate], [ticketnumber])
				--include (ticketeventtype,ticketeventdescription)
				--;
	
--==================================================================================================================================
--==================================================================================================================================
-- account device data
--==================================================================================================================================
--==================================================================================================================================

-- above sections took 3:28:00 to run
-- 2:50:00

drop table [customerretention].[dbo].[zz_staging_account_device_data_sampled_dedicated]
select distinct a.[time_month_key]
      ,a.[time_month_key_dt]
	  ,a.[account_number]
      ,b.[account_type]
	  ,[account_status]
      ,[account_sla_type]
      ,[account_sla_type_desc]
      ,[account_business_type]
      ,[account_team_name]
      ,[account_manager]
      ,[account_bdc]
      ,[account_primary_contact]
      ,[account_region]
      ,[account_billing_city]
      ,[account_billing_state]
      ,[account_billing_postal_code]
      ,[account_billing_country]
      ,[account_geographic_location]
      ,[account_created_date]
      ,[account_sub_type]
      ,[team_name]
      ,[device_number]
      ,[device_os_name]
      ,[device_os]
      ,[device_type]
      ,[device_status]
      ,[device_assigned_account_number]
      ,[device_datacenter_abbr]
      ,[device_contract_term]
      ,[device_online_date]
      ,[device_offline_date]
      ,[device_bandwidth_subscription]
      ,[device_create_date]
      ,[device_contract_received_date]
      ,[device_contract_end_date]
      ,[device_sales_rep_1]
      ,[device_active_status]
      ,[device_online_status]
      ,[device_make_model]
      ,[device_usage_type]
	  ,[contract_status]
	  ,[contractable_device]
into [customerretention].[dbo].[zz_staging_account_device_data_sampled_dedicated]
from openquery([ebi-datamart],
'select a.[time_month_key]
      ,time_month_key_dt = convert(date, cast(time_month_key as varchar) + ''01'')
	  ,a.[account_number]
	  ,[account_type]
	  ,[account_status]
      ,[account_sla_type]
      ,[account_sla_type_desc]
      ,[account_business_type]
      ,[account_team_name]
      ,[account_manager]
      ,[account_bdc]
      ,[account_primary_contact]
      ,[account_region]
      ,[account_billing_city]
      ,[account_billing_state]
      ,[account_billing_postal_code]
      ,[account_billing_country]
      ,[account_geographic_location]
      ,[account_created_date]
      ,[account_sub_type]
      ,[team_name]
      ,[device_number]
      ,[device_os_name]
      ,[device_os]
      ,[device_type]
      ,[device_status]
      ,[device_assigned_account_number]
      ,[device_datacenter_abbr]
      ,[device_contract_term]
      ,case when [device_online_date] in (''1969-12-31 18:00:00.000'', ''1900-01-01 00:00:00.000'') then [device_create_date] else [device_online_date] end as [device_online_date]
      ,[device_offline_date]
      ,[device_bandwidth_subscription]
      ,[device_create_date]
      ,[device_contract_received_date]
      ,[device_contract_end_date]
      ,[device_sales_rep_1]
      ,[device_active_status]
      ,[device_online_status]
      ,[device_make_model]
      ,[device_usage_type]
	  ,contract_status = case when lower(device_status) = ''computer no longer active'' then ''device offline''
			when lower(device_status) <> ''computer no longer active'' and isnull(device_contract_end_date,0) > convert(date, cast(time_month_key as varchar) + ''01'') and dateadd(month, 3, convert(date, cast(time_month_key as varchar) + ''01'')) >= device_contract_end_date then ''in contract - risk of lapse 90 days''
			when lower(device_status) <> ''computer no longer active'' and device_contract_end_date > convert(date, cast(time_month_key as varchar) + ''01'') then ''in contract''
			when lower(device_status) <> ''computer no longer active'' and device_contract_end_date between ''1970-01-01'' and convert(date, cast(time_month_key as varchar) + ''01'') then ''out of contract - mtm''
			else ''no contract status'' end
	  ,contractable_device = case when lower(device_type) in (''cloud consolidation'',''configuration'',''custom monitoring'',''license inventory holder'',''microsoft public cloud'',
		''microsoft public cloud - csp aviator'',''n/a'',''noteworthy'',''replication'',''sql dba service'',''ssl-accelerator'',''storage cluster'',
		''unknown'',''usb device'',''virtual server cluster'',''vm'',
		''cabinet cross-connect'',''colocation'',''leased line platform'',''ps engagement'',''virtual server'') then 0 else 1 end
from [corporate_dmart].[dbo].[vw_account_device] a 
where lower(account_source_system_name) like ''salesforce''
	and a.device_number not in (''1'')
;') a
inner join [customerretention].[dbo].[zz_staging_acct_list] b 
	on a.account_number = b.account_number 
where a.[time_month_key] >= @date --put inside openquery?

create nonclustered index ix_account_device_data_sampled_dedicated
on [customerretention].[dbo].[zz_staging_account_device_data_sampled_dedicated] ([account_number], [time_month_key_dt], [time_month_key])
;


--==================================================================================================================================
--==================================================================================================================================
-- created account_data_all_dw table - this section takes about ~40-45 min
--==================================================================================================================================
--==================================================================================================================================

if object_id('tempdb.. #distinct ') <> 0
       drop table  #distinct 
select distinct account_number,
	account_type
into #distinct 
from [ebi-datamart].[corporate_dmart].[dbo].[dim_account] a 
where current_record = 1
	and lower(account_source_system_name) in ('salesforce','mailtrust','hostingmatrix','hostingmatrix_uk') 
    and lower(account_status) <> 'unknown' 
    and isnull(account_id,0) <> 0 
    and account_id <> 0 
; 


if object_id('tempdb.. #last ') <> 0
       drop table  #last 

select a.account_number,
	a.account_type,
	tmk = convert(varchar(6),rec_updated,112),
	last_date = max(rec_updated)
into #last
from [ebi-datamart].[corporate_dmart].[dbo].[dim_account] a 
inner join #distinct b 
on a.account_number = b.account_number
	and a.account_type = b.account_type
where lower(account_source_system_name) in ('salesforce','mailtrust','hostingmatrix','hostingmatrix_uk') 
    and lower(account_status) <> 'unknown' 
    and isnull(account_id,0) <> 0 
    and account_id <> 0 
--	and a.account_number = '956262'
group by a.account_number,
	a.account_type,
	convert(varchar(6),rec_updated,112) 
; 

drop table [customerretention].[dbo].[zz_staging_account_data_all_dw]

select distinct b.tmk
	,a.account_number
	,a.account_name
	,a.account_lead_tech
	,a.account_website
	,a.account_team_name
	,a.account_status
	,a.account_sla_type
	,a.account_business_type
	,a.account_manager
	,a.account_bdc
	,a.account_primary_contact
	,a.account_region
	,a.account_billing_city
	,a.account_billing_state
	,a.account_billing_postal_code
	,a.account_billing_country
	,a.account_geographic_location
	,a.account_created_date
	,a.account_sub_type
	,account_type_new = case when lower(a.account_source_system_name) in ('hostingmatrix','hostingmatrix_uk') then 'cloud' 
		when lower(a.account_source_system_name) = 'salesforce' then 'dedicated' 
		when lower(a.account_source_system_name) = 'mail'  then 'mail' end 
into [customerretention].[dbo].[zz_staging_account_data_all_dw] 
from [ebi-datamart].[corporate_dmart].[dbo].[dim_account] a  
inner join #last b
on a.account_number = b.account_number
	and a.account_type = b.account_type
	and a.rec_updated = b.last_date
where lower(a.account_source_system_name) in ('salesforce','mailtrust','hostingmatrix','hostingmatrix_uk') 
    and lower(a.account_status) <> 'unknown' 
    and isnull(a.account_id,0) <> 0 
    and a.account_id <> 0 
	and year(a.rec_updated) >=  @year
; 

create nonclustered index ix_account_data_all_dw
on [customerretention].[dbo].[zz_staging_account_data_all_dw] ([account_number], [tmk])
;
-- combine account_data_all_dw with account_device_data_sampled_dedicated

drop table [customerretention].[dbo].[zz_staging_account_data_sampled_dedicated] 
select distinct time_month_key_dt
	,a.time_month_key
	,a.account_number
	,b.account_name
	,a.account_status
	,a.account_sla_type
	,a.account_sla_type_desc
	,b.account_business_type
	,a.account_team_name
	,a.account_manager
	,b.account_bdc
	,case when b.account_lead_tech is not null and (lower(b.account_lead_tech) <> 'n/a' or lower(b.account_lead_tech) <> 'n/a') then '1' else '0' end lead_tech_flag   -- nr: 3/7/2019 adding lead tech flag
	,b.account_primary_contact
	,b.account_region
	,b.account_billing_city
	,b.account_billing_state
	,b.account_billing_postal_code
	,b.account_billing_country
	,b.account_geographic_location
	,b.account_created_date
	,b.account_website
	,b.account_sub_type
	,a.team_name
into [customerretention].[dbo].[zz_staging_account_data_sampled_dedicated] --this was previously joined to base table, so no need for that join here
from [customerretention].[dbo].[zz_staging_account_device_data_sampled_dedicated] a
left join [customerretention].[dbo].[zz_staging_account_data_all_dw] b
on a.account_number = b.account_number
	and a.account_type = b.account_type_new
	and a.time_month_key = b.tmk
;

create nonclustered index ix_account_data_sampled_dedicated
on [customerretention].[dbo].[zz_staging_account_data_sampled_dedicated] ([account_number], [time_month_key_dt], [time_month_key])
;

--==================================================================================================================================
--==================================================================================================================================
-- create nps table
--==================================================================================================================================
--==================================================================================================================================

drop table [customerretention].[dbo].[zz_staging_nps_r] 
select time_month_key_dt = convert(date, cast(time_month_key as varchar) + '01'), 
	a.*
into [customerretention].[dbo].[zz_staging_nps_r]
from 
(
select [account_number],
	account_type,
	time_month_key = [survey_response_yyyymmdd]/100,
	[survey_score],
	[survey_rating] 
from [nps].[nps_rpt].[rpt].[vw_nps_foundation]
where reportable_flag = 1
	and survey_score is not null
	and [survey_response_yyyymmdd]/100 >= @date
) a
--inner join [customerretention].[dbo].[zz_staging_acct_list] b 
--on a.account_number = b.account_number
;

--==================================================================================================================================
--==================================================================================================================================
-- opportunities (this code may be pushed to opportunities stored proc)
--==================================================================================================================================
--==================================================================================================================================

drop table [customerretention].[dbo].[zz_staging_sfopp_dw]
select a.*
into [customerretention].[dbo].[zz_staging_sfopp_dw]
from openquery([ods],
	'select a.account_number
	,a.ddi
	,b.accountid
	,b.amount
	,b.category
	,b.closedate
	,b.lastmodifieddate
	,b.stagename
	,b.support_unit
	,b.typex

	,b.allow_quote
	,b.bucket_influence
	,b.bucket_source
	,b.commission_role
	,b.competitors
	,b.contract_length
	,b.cvp_verified
	,b.data_quality_description
	,b.data_quality_score
	,b.econnect_received
	,b.focus_area
	,b.forecastcategory
	,b.forecastcategoryname
	,b.iswon
	,b.leadsource
	,b.live_call
	,b.market_source
	,b.nutcase_deal_probability
	,b.on_demand_reconciled
	,b.pain_point
	,b.probability
	,b.requested_products
	,b.ticket_type
	,b.what_did_we_do_well
	,b.why_did_we_lose
	from [operational_reporting_sfdc].[dbo].[qaccounts] a with(nolock)
	left join [operational_reporting_sfdc].[dbo].[qopportunity] b with(nolock)
    on a.id = b.accountx
	where b.isclosed = ''true''
		and lower(b.delete_flag) = ''n''
		and lower(b.category) not in (''renewal'',''recompete - downgrade'',''recompete - straight'')
		and lower(b.why_did_we_lose) not like ''%cleanup%''
		and lower(b.why_did_we_lose) not like ''duplicate%''
		and lower(b.why_did_we_lose) not like ''existing opp%''
		and lower(b.non_bookable_revenue) =''false''
	;') a
;
-- remove cleanup opps, non-countable opps, where opportunity_deleted='n' (or similar name)
-- include count of duplicate opps

-- get account id to ddi mapping
drop table [customerretention].[dbo].[zz_staging_sales_account_id_acc_number_mapping_dedicated]
select *
into [customerretention].[dbo].[zz_staging_sales_account_id_acc_number_mapping_dedicated]
from (
		select accountid, 
			account_number = account_number, 
			rn = row_number() over(partition by account_number order by lastmodifieddate asc)
		from [customerretention].[dbo].[zz_staging_sfopp_dw]
		) x 
where rn = 1
;

drop table [customerretention].[dbo].[zz_staging_salesforce_account_dedicated]
select distinct a.account_name, 
	a.account_number, 
	a.account_type, 
	a.account_segment, 
	a.time_month_key, 
	a.revenue_segment, 
	a.churn_flag, 
	shippingcountrycode = case when lower(c.shippingcountrycode) in ('us','gb','ca','nl','au','in','ae','hk','dk','se')
		then c.shippingcountrycode else 'other' end, 
	industry = case when lower(c.industry) in ('consulting','technology','retail','manufacturing','communications','telecommunications','finance'
		,'not for profit','education','banking','healthcare','entertainment','transportation','insurance','apparel'
		,'engineering','construction','electronics','hospitality') then c.industry else 'other' end, 
	annualrevenue = convert(float, c.annualrevenue), 
	c.numberofemployees, 
	[ownership] = case when lower(c.[ownership]) = 'government' then null else c.[ownership] end, 
	c.[site], 
	naicsdesc = case when lower(c.naicsdesc) in 
		('computer systems design services','custom computer programming services','unclassified establishments','all other business support services'
		,'administrative management and general management consulting services','advertising agencies','software publishers','data processing, hosting, and related services'
		,'all other support services','electronic shopping and mail-order houses','other management consulting services','offices of real estate agents and brokers'
		,'all other personal services','all other publishers','all other miscellaneous schools and instruction','engineering services'
		,'all other miscellaneous ambulatory health care services','periodical publishers','insurance agencies and brokerages','all other nondepository credit intermediation') 
		then c.naicsdesc else 'other' end, 
	company_age = year(getdate()) - c.yearstarted, 
	c.number_of_accounts, 
	c.company_review_priority, 
	c.number_of_customer_accounts, 
	c.number_of_cloud_accounts, 
	c.company_priority
into [customerretention].[dbo].[zz_staging_salesforce_account_dedicated]
from [customerretention].[dbo].[base_dedicated] a
left join [customerretention].[dbo].[zz_staging_sales_account_id_acc_number_mapping_dedicated] b 
on a.account_number = b.account_number 
left join 
(
select aaa.* from openquery([ods],
    'select a.[account_number]
	,b.[shippingcountrycode]
	,b.[industry]
	,b.[annualrevenue]
	,b.[numberofemployees]
	,b.[ownership]
	,b.[site]
	,b.[naicsdesc]
	,b.[number_of_accounts]
	,b.[company_review_priority]
	,b.[number_of_customer_accounts]
	,b.[number_of_cloud_accounts]
	,b.[company_priority]
	,b.[yearstarted]
	from [operational_reporting_sfdc].[dbo].[qaccounts] a 
	left join [operational_reporting_sfdc].[dbo].[qaccount] b
	on a.company_name = b.id
	where lower(a.delete_flag) = ''n''
    ;') aaa --equivalent of old [sfacct_dw] with only needed columns
) c 
on b.account_number = c.account_number
	and b.accountid is not null

-- cleanup staging tables no longer needed after stored proc is done running
drop table [customerretention].[dbo].[zz_staging_acct_list]
