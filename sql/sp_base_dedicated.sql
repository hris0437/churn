USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_base_dedicated]    Script Date: 6/8/2020 9:57:31 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_base_dedicated] 
/*this script creates the dedicated model base table which the historic analytical modeling table is based on.*/

as

declare @tmk varchar(6);
set @tmk = convert(varchar(6),dateadd(mm,-1,getdate()),112);

declare @tmk2 varchar(6);
set @tmk2 = convert(varchar(6),dateadd(mm,-2,getdate()),112);

declare @tmk3 varchar(6);
set @tmk3 = left(convert(varchar(6),dateadd(mm,-60,getdate()),112),4)*100+'01';

/* changes 2/20/2020 AJW: 
- pushed product type and product grouping logic into openquery statement, in order to consolidate to "one" step the creation of the sampled 
revenue table, versus old method of grabbing all the data from the remote server, then filtering on the copy. when this section is moved to BigQuery,
we can join the openquery portion of the code directly to the account list, skipping the need for the wrapper around the openquery, and save results 
directly to the staging table used by other stored procs.

- old query returned account_number and core_account_number fields; they are identical when they are dedicated customers, therefore drop core_account_number

FUTURE STATE ON GCP:
	- remove openquery, join directly to account list
	- remove union of rolling NRD and NRD_historical, these should be merged into one table in GCP
*/

drop table [customerretention].[dbo].[zz_staging_revenue]
select distinct a.trx_line_gl_dist_id
		,a.account_number
		,a.time_month_key
		,time_month_key_dt = convert(date,left(a.time_month_key,4)+'-'+right(a.time_month_key,2)+'-01',120)
		,a.transaction_type
		,a.gl_account_group
		,a.gl_product_focus_area
		,a.gl_product_focus_area_name
		,a.product_group
		,a.team_sub_segment
		,a.line_of_business
		,a.account_type
		,a.account_name
		,a.account_sub_type
		,a.device_number
		,a.internal_flag
		,a.total_invoiced_normalized
		,a.product_type_cleaned
		,a.product_grouping
		,a.is_cloud_rackconnect_linked
		,a.is_cloud_consolidated
		,a.is_cloud_legally_linked
into [customerretention].[dbo].[zz_staging_revenue]
from openquery([480072-ea],
'select trx_line_gl_dist_id
	,account_number,time_month_key,gl_account_group,gl_product_focus_area,gl_product_focus_area_name,product_group,team_sub_segment
	,line_of_business,transaction_type,account_type,account_name,account_sub_type,device_number,internal_flag,total_invoiced_normalized
	,is_cloud_rackconnect_linked,is_cloud_consolidated,is_cloud_legally_linked
	,product_type_cleaned = CASE
when product_group=''AWS'' and patindex(''%[(]%'', product_type) > 0 then stuff(product_type, patindex(''%[(]%'', product_type) -1, len(product_type), '''') 
when product_group=''Azure'' and patindex(''%[(]%'', product_type) > 0 then stuff(product_type, patindex(''%[(]%'', product_type) -1, len(product_type), '''') 
when product_group=''Google'' and patindex(''%[(]%'', product_type) > 0 then stuff(product_type, patindex(''%[(]%'', product_type) -1, len(product_type), '''') 
when product_group=''Cloud Backup'' then replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', '''')
when product_group=''Cloud Block Storage'' then replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', '''')
when product_group=''Cloud Databases'' then replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', '''')
when product_group=''Cloud Files'' then replace(replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', ''''), ''US '', '''')
when product_group=''Cloud Glance'' then replace(replace(replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', ''''), ''US '', ''''), ''AUS '', '''')
when product_group=''Cloud Load Balancer'' then replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', ''''), ''US '', ''''), ''AUS '', ''''), ''  '', '' ''), '' HKG'', ''''), '' SYD'', '''')
when product_group=''Cloud Monitoring'' and product_type in (''Cloud Monitoring - Checks'', ''Cloud Monitoring  Checks'', ''Cloud Monitoring  Checks'') then ''Cloud Monitoring - Checks'' 
else product_type end
,product_grouping = CASE when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''Data_Stores'',''RAS'')) OR gl_product_focus_area_internal in (''DATA_STORES '',''MGD_PCF'',''MGD_SEC '',''RAS'') then ''Apps & Cross Platform''
when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''MGD_Hosting'')) OR gl_product_focus_area_internal in (''Ded_Net'',''DED_STOR'',''MGD_HOSTING '',''OTH_NON_COMPUTE '',''RAX_MGD_HOST'',''Sec_Tool'') then ''Managed Hosting''
when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''MSPC'',''OPEN_PVT_CLD '',''VMWARE'')) OR gl_product_focus_area_internal in (''MSPC '',''OPEN_PVT_CLD '',''RAX_VMWARE'',''VMWARE '') then ''Private Clouds''
when gl_product_focus_area_internal in (''RAX_AWS'',''RAX_AZURE'',''MGD_GCP '') then ''Managed Public Clouds''
when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''OPC'')) OR gl_product_focus_area_internal in (''OPC'',''CLOUD_SITES'') then ''OpenStack Public Cloud''
when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''Mailgun'',''CLD_Office'')) OR gl_product_focus_area_internal in (''CLD_OFFICE'',''MAILGUN'') then ''Cloud Office''
when gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area = ''Unknown'' and GL_Account_group=''Cloud Hosting'' then ''OpenStack Public Cloud''
else gl_product_focus_area_internal end
from [net_revenue].[dbo].[net_revenue_detail] with (nolock)
where gl_account_group <> ''One Time''
union
select trx_line_gl_dist_id
	,account_number,time_month_key,gl_account_group,gl_product_focus_area,gl_product_focus_area_name,product_group,team_sub_segment
	,line_of_business,transaction_type,account_type,account_name,account_sub_type,device_number,internal_flag,total_invoiced_normalized
	,is_cloud_rackconnect_linked,is_cloud_consolidated,is_cloud_legally_linked
	,product_type_cleaned = CASE
when product_group=''AWS'' and patindex(''%[(]%'', product_type) > 0 then stuff(product_type, patindex(''%[(]%'', product_type) -1, len(product_type), '''') 
when product_group=''Azure'' and patindex(''%[(]%'', product_type) > 0 then stuff(product_type, patindex(''%[(]%'', product_type) -1, len(product_type), '''') 
when product_group=''Google'' and patindex(''%[(]%'', product_type) > 0 then stuff(product_type, patindex(''%[(]%'', product_type) -1, len(product_type), '''') 
when product_group=''Cloud Backup'' then replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', '''')
when product_group=''Cloud Block Storage'' then replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', '''')
when product_group=''Cloud Databases'' then replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', '''')
when product_group=''Cloud Files'' then replace(replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', ''''), ''US '', '''')
when product_group=''Cloud Glance'' then replace(replace(replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', ''''), ''US '', ''''), ''AUS '', '''')
when product_group=''Cloud Load Balancer'' then replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(product_type, ''Backups'', ''Backup''), '' - AUD'', ''''), '' - GBP'', ''''), '' - EUR'',''''), '' - USD'',''''), ''UK '', ''''), ''US '', ''''), ''AUS '', ''''), ''  '', '' ''), '' HKG'', ''''), '' SYD'', '''')
when product_group=''Cloud Monitoring'' and product_type in (''Cloud Monitoring - Checks'', ''Cloud Monitoring  Checks'', ''Cloud Monitoring  Checks'') then ''Cloud Monitoring - Checks'' 
else product_type end
,product_grouping = CASE when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''Data_Stores'',''RAS'')) OR gl_product_focus_area_internal in (''DATA_STORES '',''MGD_PCF'',''MGD_SEC '',''RAS'') then ''Apps & Cross Platform''
when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''MGD_Hosting'')) OR gl_product_focus_area_internal in (''Ded_Net'',''DED_STOR'',''MGD_HOSTING '',''OTH_NON_COMPUTE '',''RAX_MGD_HOST'',''Sec_Tool'') then ''Managed Hosting''
when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''MSPC'',''OPEN_PVT_CLD '',''VMWARE'')) OR gl_product_focus_area_internal in (''MSPC '',''OPEN_PVT_CLD '',''RAX_VMWARE'',''VMWARE '') then ''Private Clouds''
when gl_product_focus_area_internal in (''RAX_AWS'',''RAX_AZURE'',''MGD_GCP '') then ''Managed Public Clouds''
when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''OPC'')) OR gl_product_focus_area_internal in (''OPC'',''CLOUD_SITES'') then ''OpenStack Public Cloud''
when (gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area in (''Mailgun'',''CLD_Office'')) OR gl_product_focus_area_internal in (''CLD_OFFICE'',''MAILGUN'') then ''Cloud Office''
when gl_product_focus_area_internal = ''UNKNOWN'' and gl_product_focus_area = ''Unknown'' and GL_Account_group=''Cloud Hosting'' then ''OpenStack Public Cloud''
else gl_product_focus_area_internal end
from [net_revenue].[dbo].[net_revenue_detail_historical] with (nolock)
where gl_account_group <> ''One Time''
and time_month_key <> convert(varchar(6),dateadd(mm,-15,getdate()),112)
and time_month_key >= convert(varchar(6),dateadd(mm,-72,getdate()),112)
and total_invoiced <> ''2061582367.68'' and total_invoiced <> ''-2061582367.68''
	;') a
;
-- create trigger for invoice exceeding X amount

create nonclustered index ix_revenue
on [customerretention].[dbo].[zz_staging_revenue] ([account_number],[time_month_key])
include ([product_group],[transaction_type])
;

-- calculate the total revenue per month per account, excluding one-off spend
drop table [customerretention].[dbo].[zz_staging_base_01_dedicated]
select time_month_key, 
	account_number, 
	account_type = line_of_business, 
	account_name, 
	account_sub_type,
	sum_total_invoiced = case when account_number = '813446' and time_month_key = '201708' then 21197.66 else sum(total_invoiced_normalized) end
into [customerretention].[dbo].[zz_staging_base_01_dedicated]
from [customerretention].[dbo].[zz_staging_revenue] with (nolock)  
where lower(gl_account_group) not in ('one time','setup fees','bandwidth overages','balance sheet account','credits')
	and lower(line_of_business) = 'dedicated'
	and time_month_key <= @tmk
	and lower(transaction_type) = 'inv'
	and lower(team_sub_segment) not in ('mail')
group by time_month_key, 
	account_number, 
	line_of_business, 
	account_name, 
	account_sub_type
;


						-----start check 1-----
						select time_month_key,
							time_month_key_dt = convert(date,left(time_month_key,4)+'-'+right(time_month_key,2)+'-01',120),
							total = sum(sum_total_invoiced)
						into #tot --drop table #tot
						from [customerretention].[dbo].[zz_staging_base_01_dedicated]
						group by time_month_key
						;

						select std = stdev(total)*3,
							[avg] = avg(total)
						into #std  --drop table #std
						from #tot
						; 

						select a.time_month_key,
							a.time_month_key_dt,
							a.total,
							c.[avg],
							diff = c.[avg] - a.total,
							std_flag = case when c.[avg] - a.total >= c.std then 1 
								when c.[avg] - a.[total] <= -1*(c.std) then 1
								else 0 end,
							min_flag = case when a.total < 99000000 then 1 else 0 end,
							max_flag = case when a.total > 125000000 then 1 else 0 end
						into #test --drop table #test
						from #tot a 
						cross join #std c
						order by 1
						;

						drop table flag
						select flag = case when (sum(case when (std_flag > 0 or min_flag > 0 or max_flag > 0)
										and time_month_key_dt >= dateadd(yy,-3,getdate()) 
										then 1 else 0 end)) > 0 
									then 1 else 0 end
						into flag 
						from #test
						;

						declare @flag int;
						set @flag = (select flag from flag);

						if (@flag = 1)
						begin
						raiserror(15600,-1,-1,'datacheck1')
						end
						-----end check 1-----

else
begin

-- get defected churn accounts and month of defection
drop table [customerretention].[dbo].[base_dedicated_defected]
select account_number, 
	account_type, 
	account_name, 
	account_sub_type,
	defection_month = max(time_month_key)
into [customerretention].[dbo].[base_dedicated_defected]
from [customerretention].[dbo].[zz_staging_base_01_dedicated]
group by account_number, 
	account_type, 
	account_name, 
	account_sub_type
having max(time_month_key) < @tmk2
;



-- drop table #tmks
select time_month_key,  
	account_number,	
	account_type,	
	account_name,	
	account_sub_type, 
	sum_total_invoiced,
	tmk_precede_12 = convert(varchar(6),dateadd(mm,-12,(cast(time_month_key as varchar) + '01')),112),
	tmk_precede_6 = convert(varchar(6),dateadd(mm,-6,(cast(time_month_key as varchar) + '01')),112),
	tmk_precede_1 = convert(varchar(6),dateadd(mm,-1,(cast(time_month_key as varchar) + '01')),112),
	tmk_follow_1 = convert(varchar(6),dateadd(mm,+1,(cast(time_month_key as varchar) + '01')),112),
	tmk_follow_6 = convert(varchar(6),dateadd(mm,+6,(cast(time_month_key as varchar) + '01')),112),
	tmk_follow_9 = convert(varchar(6),dateadd(mm,+9,(cast(time_month_key as varchar) + '01')),112),
	tmk_follow_12 = convert(varchar(6),dateadd(mm,+12,(cast(time_month_key as varchar) + '01')),112)
into #tmks  
from [customerretention].[dbo].[zz_staging_base_01_dedicated]
;

-- "average revenue" and "count of months" for last 12 months and next 6 months churn
-- "average revenue" and "count of months" for last 6 months and next 9 months growth
-- drop table #calctmk
select t1.time_month_key,  
	t1.account_number,	
	t1.account_type,	
	t1.account_name,	
	t1.account_sub_type, 
	average_invoiced_last_12_months = ( --churn
		select avg(sum_total_invoiced) 
		from #tmks t2 
		where t1.account_number = t2.account_number 
				and t2.time_month_key between t1.tmk_precede_12 and t1.tmk_precede_1),
	average_invoiced_next_6_months = ( --churn
		select avg(sum_total_invoiced) 
		from #tmks t2 
		where t1.account_number = t2.account_number 
				and t2.time_month_key between t1.tmk_follow_1 and t1.tmk_follow_6),
	num_months_last_12_months = ( --churn
		select count(distinct t2.time_month_key) 
		from #tmks t2 
		where t1.account_number = t2.account_number 
			and t2.time_month_key between t1.tmk_precede_12 and t1.tmk_precede_1),
	num_months_next_6_months = ( --churn
		select count(distinct t2.time_month_key) 
		from #tmks t2 
		where t1.account_number = t2.account_number 
			and t2.time_month_key between t1.tmk_follow_1 and t1.tmk_follow_6),
--	baseline_period_start = ( --churn
--		select min(t2.time_month_key) 
--		from #tmks t2
--		 where t1.account_number = t2.account_number 
--			and t2.time_month_key between t1.tmk_precede_12 and t1.tmk_precede_1),
--	baseline_period_end = ( --churn
--		select max(t2.time_month_key) 
--		from #tmks t2 
--		where t1.account_number = t2.account_number 
--			and t2.time_month_key between t1.tmk_precede_12 and t1.tmk_precede_1),
--	evaluation_period_start = ( --churn
--		select min(t2.time_month_key) 
--		from #tmks t2 
--		where t1.account_number = t2.account_number 
--			and t2.time_month_key between t1.tmk_follow_1 and t1.tmk_follow_6),
--	evaluation_period_end = ( --churn
--		select  max(t2.time_month_key) from #tmks t2 where t1.account_number = t2.account_number 
--			and t2.time_month_key between t1.tmk_follow_1 and t1.tmk_follow_6),
	past_6mo_baseline = ( --growth
		select avg(sum_total_invoiced) 
		from #tmks t2 
		where t1.account_number = t2.account_number 
			and t2.time_month_key between t1.tmk_precede_6 and t1.tmk_precede_1),
	avg_9mo_eval = ( --growth
		select avg(sum_total_invoiced) 
		from #tmks t2 
		where t1.account_number = t2.account_number 
			and t2.time_month_key between t1.tmk_follow_1 and t1.tmk_follow_9),
	num_months_last_6_months = ( --growth
		select count(distinct t2.time_month_key) 
		from #tmks t2 
		where t1.account_number = t2.account_number 
			and t2.time_month_key between t1.tmk_precede_6 and t1.tmk_precede_1),
	num_months_next_9_months = ( --growth
		select count(distinct t2.time_month_key) 
		from #tmks t2 
		where t1.account_number = t2.account_number 
			and t2.time_month_key between t1.tmk_follow_1 and t1.tmk_follow_9) 		 
into #calctmk 
from #tmks t1
order by t1.time_month_key
;

drop table [customerretention].[dbo].[zz_staging_base_02_dedicated]
select a.time_month_key,  
	a.account_number,	
	a.account_type,	
	a.account_name,	
	a.account_sub_type,
	account_segment = a.account_type,
	revenue_segment = cast('' as varchar),
	total_invoiced_in_month = a.sum_total_invoiced,
	time_month_key_dt = convert(date, cast(a.time_month_key as varchar) + '01'),
	time_month_key_eo_last_month_dt = dateadd(day, -1, dateadd(month, 1, convert(date, cast(a.time_month_key as varchar) + '01'))),
	b.average_invoiced_last_12_months, 
	b.average_invoiced_next_6_months, 
	b.num_months_last_12_months, 
	b.num_months_last_6_months, 
	b.num_months_next_6_months, 
	b.num_months_next_9_months, 
--================================================================
--test if removing these affects any tables besides final table
	--b.baseline_period_start, 
	--b.baseline_period_end, 
	--b.evaluation_period_start, 
	--b.evaluation_period_end,
--================================================================
	month_order = row_number() over (partition by a.account_name, a.account_number order by a.time_month_key asc),
	month_order_desc = row_number() over (partition by a.account_name, a.account_number order by a.time_month_key desc),
	churn_pct_change = cast(0 as float),
	churn_flag = cast('' as varchar),
	churn_target = cast(0 as float),
	b.past_6mo_baseline, 
	b.avg_9mo_eval, 
	growth_pct_change = cast(0 as float),
	growth_target = cast(0 as float)
into [customerretention].[dbo].[zz_staging_base_02_dedicated]
from #tmks a 
left join #calctmk b
on b.account_number = a.account_number 
	and b.time_month_key = a.time_month_key 
order by a.account_number, 
	a.time_month_key
;


-- filter previous tables for accounts that match the modeling criteria
drop table [customerretention].[dbo].[base_dedicated]
select *
into [customerretention].[dbo].[base_dedicated]
from [customerretention].[dbo].[zz_staging_base_02_dedicated]
where month_order > 6 
	and average_invoiced_last_12_months > 0 
	and lower(account_sub_type) not in ('employee', 'internal') -- exclude employee and internal rackspace accounts


-- set revenue bands based on the last 12 months of spend
update [customerretention].[dbo].[base_dedicated]
set revenue_segment = case 
	when average_invoiced_last_12_months > 100000 then '1. $100k+ p/m'
	when average_invoiced_last_12_months > 25000 and average_invoiced_last_12_months <= 100000 then '2. $25k - $100k p/m'
	when average_invoiced_last_12_months > 10000 and average_invoiced_last_12_months <= 25000 then '3. $10k - $25k p/m'
	when average_invoiced_last_12_months > 5000  and average_invoiced_last_12_months <= 10000 then '4. $5k - $10k p/m'
	when average_invoiced_last_12_months > 1000  and average_invoiced_last_12_months <= 5000 then '5. $1k - $5k p/m'
	when average_invoiced_last_12_months <= 1000 then '6. <= $1k p/m' end,
	churn_pct_change = average_invoiced_next_6_months/nullif(average_invoiced_last_12_months, 0) - 1,
	growth_pct_change = avg_9mo_eval/nullif(past_6mo_baseline, 0) - 1


update [customerretention].[dbo].[base_dedicated]
set churn_flag = 'significant drop'
where (churn_pct_change <= -0.3)

update [customerretention].[dbo].[base_dedicated]
set churn_flag = 'significant increase'
where churn_pct_change >= 0.3

update [customerretention].[dbo].[base_dedicated]
set churn_flag = 'no significant change'
where churn_pct_change between -0.3 and 0.3

update [customerretention].[dbo].[base_dedicated]
set churn_target = case when churn_pct_change<= -0.3 then 1 else 0 end

-- set target flags to 1 when growth exceeds 10%, 9 month evaluation window
update [customerretention].[dbo].[base_dedicated]
set growth_target = case when growth_pct_change >= 0.1 then 1 else 0 end
;



end

						-----start check 2-----
						select time_month_key,
							time_month_key_dt = convert(date,left(time_month_key,4)+'-'+right(time_month_key,2)+'-01',120),
							ct = count(*)
						into #ct --drop table #ct
						from [customerretention].[dbo].[base_dedicated]
						group by time_month_key
						;

						select std = stdev(ct)*2,
							[avg] = avg(ct)
						into #std_ct  --drop table #std_ct
						from #ct
						; 

						select a.time_month_key,
							a.time_month_key_dt,
							a.ct,
							c.[avg],
							diff = c.[avg] - a.ct,
							--std_flag = case when c.[avg] - a.ct >= c.std then 1 
								--when c.[avg] - a.ct <= -1*(c.std) then 1
								--else 0 end,
							min_flag = case when a.ct < 8000 then 1 else 0 end,  ---- ***please read note below*** ---- 
							---- altered by nr from 9800 to 9000 as the accounts count has actually reduced less than 9800 and procedure halts
							----  we need to discuss about way to formulate this number instead of a hard coded value.
							max_flag = case when a.ct > 15000 then 1 else 0 end
						into #test2 --drop table #test2
						from #ct a 
						cross join #std_ct c
						order by 1
						;

						drop table flag2
						select flag = case when (sum(case when (min_flag > 0 or max_flag > 0)--std_flag > 0 or 
										and time_month_key_dt >= dateadd(yy,-3,getdate()) 
										then 1 else 0 end)) > 0 
									then 1 else 0 end
						into flag2 
						from #test2
						;

						declare @flag2 int;
						set @flag2 = (select flag from flag2);

						if (@flag2 = 1)
						begin
						raiserror(15600,-1,-1,'datachecks2')
						end
						-----end check 2-----


