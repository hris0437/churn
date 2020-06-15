USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_base_cloud]    Script Date: 3/18/2020 3:47:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_base_cloud] 

as

declare @tmk varchar(6);
set @tmk = convert(varchar(6),dateadd(mm,-1,getdate()),112);

declare @tmk2 varchar(6);
set @tmk2 = left(convert(varchar(6),dateadd(mm,-60,getdate()),112),4)*100+'01';;

--removing thrid party cloud 1/18/2019
select distinct account_number 
into #acct 
from [customerretention].[dbo].[zz_staging_revenue] with (nolock) 
where lower(gl_product_focus_area) = 'opc'
	and lower(line_of_business) in ('cloud','cloud uk')
	and time_month_key <= @tmk
	and time_month_key >= @tmk2
	and internal_flag = 0
	and lower(transaction_type) = 'inv'
;

create nonclustered index ix_acct
on #acct ([account_number])
;

select a.time_month_key, 
	a.account_number, 
	account_type = a.line_of_business, 
	a.account_name, 
	a.account_sub_type,
	sum_total_invoiced = sum(a.total_invoiced_normalized)
into [customerretention].[dbo].[zz_staging_base_01_cloud]
from [customerretention].[dbo].[zz_staging_revenue] a with (nolock)  
inner join #acct b
on a.account_number = b.account_number
where /*a.gl_account_group not in ('one time','setup fees','bandwidth overages','balance sheet account','credits') */ -- tm 4/8/2019 flag not working for cloud
	lower(a.line_of_business) in ('cloud','cloud uk')
	and a.time_month_key <= @tmk
	and a.internal_flag = 0
	and lower(a.transaction_type) = 'inv'
group by a.time_month_key, 
	a.account_number, 
	a.account_name, 
	a.account_sub_type,
	a.line_of_business
;


--drop table #tmks
select time_month_key,  
	account_number,	
	account_type,	
	account_name,	
	account_sub_type, 
	sum_total_invoiced,
	tmk_precede_12 = convert(varchar(6),dateadd(mm,-12,(cast(time_month_key as varchar) + '01')),112),
	tmk_precede_1 = convert(varchar(6),dateadd(mm,-1,(cast(time_month_key as varchar) + '01')),112),
	tmk_follow_1 = convert(varchar(6),dateadd(mm,+1,(cast(time_month_key as varchar) + '01')),112),
	tmk_follow_6 = convert(varchar(6),dateadd(mm,+6,(cast(time_month_key as varchar) + '01')),112)
into #tmks  
from [customerretention].[dbo].[zz_staging_base_01_cloud]
;

 ---  average revenue
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
			and t2.time_month_key between t1.tmk_follow_1 and t1.tmk_follow_6)
into #calctmk 
from #tmks t1
order by t1.time_month_key
;

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
	b.num_months_next_6_months, 
	month_order = row_number() over (partition by a.account_name, a.account_number order by a.time_month_key asc),
	month_order_desc = row_number() over (partition by a.account_name, a.account_number order by a.time_month_key desc),
	pct_change = cast(0 as float),
	churn_flag = cast('' as varchar),
	target = cast(0 as float)
into [customerretention].[dbo].[zz_staging_base_02_cloud]
from #tmks a 
left join #calctmk b
on b.account_number = a.account_number 
	and b.time_month_key = a.time_month_key 
order by a.account_number, 
	a.time_month_key
;

drop table [customerretention].[dbo].[base_cloud]
select *
into [customerretention].[dbo].[base_cloud]
from [customerretention].[dbo].[zz_staging_base_02_cloud]
where month_order > 12 -- add filter of >24 months when selecting rows for model training
	and average_invoiced_last_12_months > 0 -- account has non-zero invoicing over last 12 months
	and time_month_key <= @tmk
	

update [customerretention].[dbo].[base_cloud]
	set revenue_segment = case 
		when average_invoiced_last_12_months > 100000 then '6. $100k+ p/m'
		when average_invoiced_last_12_months > 25000 and average_invoiced_last_12_months <= 100000 then '5. $25k - $100k p/m'
		when average_invoiced_last_12_months > 10000 and average_invoiced_last_12_months <= 25000 then '4. $10k - $25k p/m'
		when average_invoiced_last_12_months > 5000  and average_invoiced_last_12_months <= 10000 then '3. $5k - $10k p/m'
		when average_invoiced_last_12_months > 1000  and average_invoiced_last_12_months <= 5000 then '2. $1k - $5k p/m'
		when average_invoiced_last_12_months <= 1000 then '1. <= $1k p/m' end 

update [customerretention].[dbo].[base_cloud]
	set pct_change = case when (average_invoiced_last_12_months is null or average_invoiced_last_12_months = 0) and average_invoiced_next_6_months >0 then 1.00
		when (average_invoiced_last_12_months is null or average_invoiced_last_12_months = 0) and average_invoiced_next_6_months = 0 then 0.00
		when (average_invoiced_last_12_months is null or average_invoiced_last_12_months = 0) and average_invoiced_next_6_months < 0 then -1.00
		when average_invoiced_last_12_months > 0 and average_invoiced_next_6_months is null then -1.00
		else average_invoiced_next_6_months / nullif(average_invoiced_last_12_months, 0) - 1 end

-- set flags - 60%
update [customerretention].[dbo].[base_cloud]
	set churn_flag = 'significant drop'
	where pct_change <= -0.6

update [customerretention].[dbo].[base_cloud]
	set churn_flag = 'significant increase'
	where pct_change >= 0.6

update [customerretention].[dbo].[base_cloud]
	set churn_flag = 'no significant change'
	where pct_change between -0.6 and 0.6

update [customerretention].[dbo].[base_cloud]
	set target = case when pct_change <= -0.6 then 1 else 0 end


drop table [customerretention].[dbo].[zz_staging_base_01_cloud]
drop table [customerretention].[dbo].[zz_staging_base_02_cloud]
