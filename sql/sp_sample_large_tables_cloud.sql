USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_sample_large_tables_cloud]    Script Date: 3/18/2020 3:51:40 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_sample_large_tables_cloud]
as

/*this script prepares a number of raw tables for use in subsequent scripts.
first is identfies the unique accounts used in the base table, then filters raw tables to include only those accounts.
any additional transformations to columns in the raw tables (such as date conversions) are included in this script.*/


select distinct account_number, 
	account_type, 
	account_segment
into #account_sample_cloud
from [customerretention].[dbo].[base_cloud]

--==================================================================================================================================
--==================================================================================================================================
-- cloud revenue data (dependent upon sp_base_dedicated running first, to create zz_staging_revenue table)
--==================================================================================================================================
--==================================================================================================================================

drop table [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_cloud]

select a.*
into [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_cloud]
from [customerretention].[dbo].[zz_staging_revenue] a with (nolock)
inner join #account_sample_cloud b 
on a.account_number = b.account_number
where lower(a.gl_account_group) <> 'one time' 
	and lower(a.line_of_business) in ('cloud','cloud uk')



create nonclustered index ix_revenue_data_withnewkeys_sampled_cloud
on [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_cloud] ([account_number],[account_type],[time_month_key_dt])
include ([time_month_key],[product_group],[total_invoiced_normalized])


----==================================================================================================================================
----==================================================================================================================================
---- sample ticket events data 
----==================================================================================================================================
----==================================================================================================================================
--drop table [ticket_events_data_sampled_cloud] 
--select distinct a.accountnumber,
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
--into [ticket_events_data_sampled_cloud] 
--from [ticket_events_dw] a
--inner join #account_sample_cloud b 
--on cast(a.accountnumber as varchar) = cast(b.account_number as varchar) 
--	and a.account_type = b.account_type
--where ticketcreatedby <> 'auto'
--;
	
--create nonclustered index ix_ticket_event_data_sample_cloud
--on [ticket_events_data_sampled_cloud] ([accountnumber],[account_type], [ticketeventdate], [ticketnumber])
--include (ticketeventtype,ticketeventdescription)


--==================================================================================================================================
--==================================================================================================================================
-- sample account information from account table (cloud only)
--==================================================================================================================================
--==================================================================================================================================

select a.*
into #cloud_account_changes 
from [customerretention].[dbo].[zz_staging_account_data_all_dw] a
inner join #account_sample_cloud b 
on a.account_number = b.account_number
where lower(a.account_type_new) = 'cloud'
;
		
select distinct time_month_key
into #tmk_cloud
from  [customerretention].[dbo].[zz_staging_revenue] with (nolock)
;


----drop table #tmp2_cloud
select time_month_key, 
	most_up_to_date_tmk = coalesce(max(case when tmk <= time_month_key then tmk else null end), min(tmk)), 
	account_number, 
	account_type_new as account_type
into #tmp2_cloud
from #cloud_account_changes a 
cross join #tmk_cloud b
group by time_month_key, 
	account_number, 
	account_type_new
;

drop table [customerretention].[dbo].[zz_staging_account_data_cloud_sampled]
select *
into [customerretention].[dbo].[zz_staging_account_data_cloud_sampled]
from (
		select rn = row_number() over (partition by a.account_number, time_month_key order by a.account_number), 
			a.time_month_key, 
			b.*
		from #tmp2_cloud a
		left join #cloud_account_changes b 
		on a.account_number = b.account_number
			and (a.most_up_to_date_tmk = b.tmk) 
		left join (
					select account_number, 
						first_time_month_key = min(time_month_key)
					from [customerretention].[dbo].[base_cloud]
					group by account_number
					) c 
		on a.account_number = c.account_number 
		where a.time_month_key >= c.first_time_month_key
		) x
where rn = 1
;	

--mi_mo flag
select distinct c.account_number, tmk = c.time_month_key, tmk_dt = cast(left(c.time_month_key,4) + '-' + right(c.time_month_key,2)+'-01' as date),
	mi_mo = case 
		when lower(a.[sla_name]) = 'infrastructure' and lower(a.[sla_type]) = 'sysops' then 'mi' 
		when lower(a.[sla_name]) = 'infrastructure' and lower(a.[sla_type]) = 'legacy' then 'legacy core' 
		when lower(a.[sla_name]) = 'managed' and lower(a.[sla_type]) in ('sysops','devops') then 'mo' 
		when lower(a.[sla_name]) = 'managed' and lower(a.[sla_type]) = 'legacy' then 'legacy managed'
		end
into #sla
from [customerretention].[dbo].[zz_staging_account_data_cloud_sampled] c with (nolock)
left join (
			select a.[account_id], a.[sla_name], a.[sla_type], b.tmk, b.last_date
			from [480072-ea].[cloud_usage].[dbo].[brm_sla_account_profile_history] a with (nolock)
			inner join (
						select account_number = [account_id],
							tmk = case when convert(varchar(6),[profile_startdate],112) < b.tmk then b.tmk else convert(varchar(6),[profile_startdate],112) end,
							last_date = max([profile_startdate])
						from [480072-ea].[cloud_usage].[dbo].[brm_sla_account_profile_history] a with (nolock)
						inner join (
									select account_number,
										tmk = min(time_month_key)
									from [customerretention].[dbo].[zz_staging_account_data_cloud_sampled] with (nolock)
									--where account_number = '322653'
									group by account_number
									) b
						on a.[account_id] = b.account_number
						--where account_number = '322653'
						group by [account_id],
							case when convert(varchar(6),[profile_startdate],112) < b.tmk then b.tmk else convert(varchar(6),[profile_startdate],112) end
						) b
			on a.[account_id] = b.account_number
				and a.[profile_startdate] = b.last_date
			) a
on c.account_number = a.[account_id] 
	and c.time_month_key = a.tmk
left join (
			select a.[account_id], a.[sla_name], a.[sla_type], b.tmk
			from [480072-ea].[cloud_usage].[dbo].[brm_sla_account_profile_history] a with (nolock)
			inner join (
						select account_number = [account_id],
							tmk = case when b.tmk > convert(varchar(6),[profile_startdate],112) then b.tmk else convert(varchar(6),[profile_startdate],112) end,
							last_date = max([profile_startdate])
						from [480072-ea].[cloud_usage].[dbo].[brm_sla_account_profile_history] a with (nolock)
						inner join (
									select account_number,
										tmk = max(time_month_key)
									from [customerretention].[dbo].[zz_staging_account_data_cloud_sampled] with (nolock)
									group by account_number
									) b
						on a.[account_id] = b.account_number
						where is_current_record = 1
						group by [account_id],
							case when b.tmk > convert(varchar(6),[profile_startdate],112) then b.tmk else convert(varchar(6),[profile_startdate],112) end
						) b
			on a.[account_id] = b.account_number
				and a.[profile_startdate] = b.last_date
			) d
on c.account_number = d.[account_id] 
	and c.time_month_key = d.tmk
--where account_number = '322653'
order by 2
; 

select distinct account_number,
	tmk,
	tmk_dt,
	mi_mo,
	rownum = row_number() over(partition by account_number order by tmk)
into #sla1
from #sla a with (nolock)
;

select * 
into #test
from #sla1 
--where account_number in ('329288','357942','657860')
order by account_number, tmk

select * 
into #test2
from #test a
where mi_mo is not null

select distinct identity (int) as row_id,
	account_number,
	effectivedate = dateadd(mm,1,tmk_dt),
	report_enddate = case when day(getdate()) =1  
		then cast(dateadd(month, datediff(month, -1, getdate())-1, -1) as datetime)
		else convert(varchar(10),getdate(),120) end
	,mi_mo
into #tempsla  
from #test2 a  with (nolock) 
where dateadd(mm,1,tmk_dt) <= case when day(getdate()) =1 
	then cast(dateadd(month, datediff(month, -1, getdate())-1, -1) as datetime)
	else getdate() end 
order by account_number,effectivedate
;

select s1.account_number,
	s1.mi_mo,
	sla_sdate = s1.effectivedate,
	sla_edate = isnull(s2.effectivedate,s1.report_enddate),
	s1.report_enddate
into #slaranges
from #tempsla s1  with (nolock) 
left join #tempsla s2 
on s1.account_number = s2.account_number
	and s2.row_id = s1.row_id+1
order by s1.row_id ,s1.account_number
;

select a.account_number,
	mi_mo = case when a.mi_mo is not null then a.mi_mo else b.mi_mo end,
	time_month_key = a.tmk,
	a.tmk_dt
into #sla_fix
from #test a
left join #slaranges b
on a.account_number = b.account_number
	and a.tmk_dt >= b.sla_sdate 
	and a.tmk_dt < b.sla_edate
;

--select account_number,
--	tmk,
--	mi_mo = isnull(mi_mo, (select top 1 mi_mo from #sla1 where row_id < t.row_id and mi_mo is not null order by row_id desc))
--into #sla_fix
--from #sla1 t
----where account_number in ('322653','324197','10046902')
--;


--cloud historical tam
--get the tam data
select  [ddi] = a.[number],
        r.[created_ts],
        r.[deleted_at],
        [created_tmk] = convert(varchar(6), r.[created_ts], 112),
        [deleted_tmk] = case when r.[deleted_at] = '1970-01-01 00:00:01.000' then convert(varchar(6),dateadd(m, -2, getdate()),112)
                             else convert(varchar(6), r.[deleted_at], 112) end,
        [tam] = u.[name]
into #tam
from [ods].[ss_db_ods].[dbo].[accounts_users_roles_all] r with (nolock)
inner join [ods].[ss_db_ods].[dbo].[user_all] u with (nolock)
    on r.[user_id] = u.[user_id]
inner join [ods].[ss_db_ods].[dbo].[account_all] a with (nolock)
    on r.[account_id] = a.[account_id]
    and lower(a.[type]) = 'cloud'
where r.[role_id] = 9
order by r.[created_ts]
;


--expand the tam data to have at least one row for each month
select distinct d.[time_month_key]
        ,t.[created_tmk]
        ,t.[created_ts]
        ,t.[ddi]
        ,t.[deleted_at]
        ,t.[deleted_tmk]
        ,t.[tam]
into #tam_expanded
from #tmk_cloud d
inner join #tam t
    on d.[time_month_key] between t.[created_tmk] and t.[deleted_tmk]
;



--find the max value for a month
select  [time_month_key],
        [ddi],
        [max_date] = max([created_ts])
into #tam_max
from #tam_expanded
group by [time_month_key], 
	[ddi]
;

--get tam per ddi per month
select  t1.[time_month_key],
        [keizan_cloud_ddi] = t1.[ddi],
        [keizan_cloud_tam] = t1.[tam]
into #tamfinal
from #tam_expanded t1
inner join #tam_max t2
    on t1.[time_month_key] = t2.[time_month_key]
    and t1.[ddi] = t2.[ddi]
    and t1.[created_ts] = t2.[max_date]
;

--get tam changes per month
select distinct account_number = [keizan_cloud_ddi], 
	time_month_key,
	tam = [keizan_cloud_tam], 
	row_number() over(partition by [keizan_cloud_ddi] order by [keizan_cloud_ddi], time_month_key asc) as sno
 into #am_data_sno  -- drop table #am_data_sno
 from #tamfinal
 ;
    
    
 select distinct am1.account_number, 
	am1.time_month_key, 
	am_changed = case when am1.tam <> am2.tam 
		then  'y' else 'n' end
 into #am_change 
 from #am_data_sno am1
 left join #am_data_sno  am2 
 on  am1.account_number = am2.account_number 
 and am1.sno+1 = am2.sno 
 ; 
 
 -- account_am_ratio
 select distinct tam , 
	act_am_ratio= count(distinct account_number), 
	time_month_key
 into #am_act_tmk  -- drop table #am_act_tmk
 from #am_data_sno
 group by tam, 
	time_month_key
 ;
 
-- change in am to act ratio
  
 select distinct tam, 
	time_month_key, 
	act_am_ratio, 
	sno = row_number() over(partition by tam order by tam, time_month_key, act_am_ratio asc)
 into #am_act_tmk_sno  -- drop table #am_act_tmk
 from #am_act_tmk
 group by tam, 
	time_month_key, 
	act_am_ratio                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
 ;

 
  select distinct am1.tam, 
	am1.time_month_key, 
	am_ratio_changed = case when am1.act_am_ratio <> am2.act_am_ratio 
		then 'y' else 'n' end
 into #am_ratio_change 
 from #am_act_tmk_sno  am1
 left join #am_act_tmk_sno  am2 
 on  am1.tam = am2.tam 
	and am1.sno+1 = am2.sno 
;

/*
-- aw removed 3/12/2020 not in final model
*/
--select distinct a.account_number, 
--	purecloud_flag = 0,
--	a.time_month_key, 
--	[risk segment] = case when a.churn_probability >=.5 then '1' else '0' end
-- into #dedibox
-- from dbo.dedicated_scores a
-- inner join revenue_data_withnewkeys_sampled_cloud b
-- on a.account_number = b.account_number
--	and a.time_month_key = b.time_month_key
--;	
	

drop table [customerretention].[dbo].[zz_staging_account_data_sampled_cloud] 
select distinct time_month_key_dt = convert(date, cast(a.time_month_key as varchar) + '01'),
	a.time_month_key,
	a.account_number,
	a.account_name,
	a.account_status,
	a.account_sla_type,
	a.account_business_type,
	a.account_team_name,
	a.account_manager,
	a.account_bdc,
	a.account_primary_contact,
	a.account_region,
	a.account_billing_city,
	a.account_billing_state,
	a.account_billing_postal_code,
	a.account_billing_country,
	a.account_geographic_location,
	a.account_created_date,
	a.account_website,
	a.account_sub_type,
	tam = g.keizan_cloud_tam,
	h.am_changed,
	f.mi_mo,
	c.act_am_ratio,
	d.am_ratio_changed
	--dedicated_risk = e.[risk segment] -- aw removed 3/12/2020 not in final model
into [customerretention].[dbo].[zz_staging_account_data_sampled_cloud] 
from [customerretention].[dbo].[zz_staging_account_data_cloud_sampled] a
inner join #account_sample_cloud b 
on a.account_number = b.account_number 
left join #sla_fix f
on a.account_number = f.account_number
	and a.time_month_key = f.time_month_key
left join #tamfinal g
on a.account_number = g.keizan_cloud_ddi
	and a.time_month_key = g.time_month_key
left join #am_change h
on a.account_number = h.account_number
	and a.time_month_key = h.time_month_key
left join #am_act_tmk c
on g.keizan_cloud_tam = c.tam 
	and g.time_month_key = c.time_month_key
left join #am_ratio_change d
on g.keizan_cloud_tam = d.tam 
	and g.time_month_key = d.time_month_key
--left join #dedibox e
--on a.account_number = e.account_number
--	and a.time_month_key = e.time_month_key
order by account_number,
	time_month_key
;

create nonclustered index ix_account_data_sampled_cloud
on [customerretention].[dbo].[zz_staging_account_data_sampled_cloud] ([account_number], [time_month_key_dt])


--==================================================================================================================================
--==================================================================================================================================
-- get account id to ddi mapping
--==================================================================================================================================
--==================================================================================================================================
drop table [customerretention].[dbo].[zz_staging_sales_account_id_acc_number_mapping_cloud]
select *
into [customerretention].[dbo].[zz_staging_sales_account_id_acc_number_mapping_cloud]
from (
		select accountid, 
			account_number = ddi, 
			rn = row_number() over (partition by ddi order by lastmodifieddate asc)
		from [customerretention].[dbo].[zz_staging_sfopp_dw]
		--where ddi is not null
	) x 
where rn = 1
;


drop table [customerretention].[dbo].[zz_staging_salesforce_account_cloud]
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
into [customerretention].[dbo].[zz_staging_salesforce_account_cloud]
from [customerretention].[dbo].[base_cloud] a
left join [customerretention].[dbo].[zz_staging_sales_account_id_acc_number_mapping_cloud] b 
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
	from [operational_reporting_sfdc].[dbo].[qaccounts] a with (nolock)
	left join [operational_reporting_sfdc].[dbo].[qaccount] b with (nolock)
	on a.company_name = b.id
	where lower(a.delete_flag) = ''n''
    ;') aaa --equivalent of old [sfacct_dw] with only needed columns
) c 
on b.account_number = c.account_number
	and b.accountid is not null