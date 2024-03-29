USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_revenue_cloud]    Script Date: 3/18/2020 3:51:03 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_revenue_cloud]
as

/*
	this script pulls in information on revenue with a number of different splits and calculations, e.g. changes in spend over different time periods, distribution of revenue by product,
	whether or not certain products are present etc.
*/

select *
into #rackconnect
from (
		select *,
			rowno = row_number() over(partition by account_number, time_month_key order by account_type desc)--is_cloud_rackconnect_linked desc)
		from (
				select distinct account_number,
					time_month_key,
					account_type = line_of_business,
					is_cloud_rackconnect_linked,
					is_cloud_consolidated,
					is_cloud_legally_linked
				from [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_cloud] with (nolock)
				 ) a
		) a
where rowno = 1
;

drop table [customerretention].[dbo].[zz_staging_revenue_01_cloud]
select a.account_name, 
	a.account_number, 
	a.account_type, 
	a.time_month_key, 
	a.revenue_segment, 
	a.churn_flag, 
	c.is_cloud_rackconnect_linked, 
	c.is_cloud_consolidated, 
	c.is_cloud_legally_linked, 
	[total_invoiced_in_last_6_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and a.time_month_key_dt then b.total_invoiced_normalized else null end), 0), 
	[avg_per_line_item_invoiced_in_last_6_months] = isnull(avg(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and a.time_month_key_dt then b.total_invoiced_normalized else null end), 0) / 6, 
	[avg_mthly_num_of_invoiced_items_in_last_6_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 0) / 6, 
	[avg_credit_memo_in_last_6_months] = isnull(avg(case when lower(b.transaction_type) = 'cm' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and a.time_month_key_dt then b.total_invoiced_normalized else null end), 0),
	[total_invoiced_in_last_6_months_vs_prior_6_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and a.time_month_key_dt  then b.total_invoiced_normalized else null end) / nullif(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -11, a.time_month_key_dt ) and dateadd(month, -6, a.time_month_key_dt  )  then b.total_invoiced_normalized else null end), 0), -1), 
	[avg_invoiced_in_last_6_months_vs_prior_6_months] = isnull(avg(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and a.time_month_key_dt  then b.total_invoiced_normalized else null end) / nullif(avg(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -11, a.time_month_key_dt ) and dateadd(month, -6, a.time_month_key_dt  )  then b.total_invoiced_normalized else null end), 0), -1), 
	[ratio_mthly_num_of_invoiced_items_in_last_6_months_vs_prior_6_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and a.time_month_key_dt  then 1.00 else null end) / nullif(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -11, a.time_month_key_dt ) and dateadd(month, -6, a.time_month_key_dt  )  then 1.00 else null end), 0), -1), 
	[total_invoiced_in_last_3_months_vs_prior_3_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -2, a.time_month_key_dt) and a.time_month_key_dt  then b.total_invoiced_normalized else null end) / nullif(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and dateadd(month, -3, a.time_month_key_dt  )  then b.total_invoiced_normalized else null end), 0), -1), 
	[avg_invoiced_in_last_3_months_vs_prior_3_months] = isnull(avg(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -2, a.time_month_key_dt) and a.time_month_key_dt  then b.total_invoiced_normalized else null end) / nullif(avg(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and dateadd(month, -3, a.time_month_key_dt  )  then b.total_invoiced_normalized else null end), 0), -1), 
	[ratio_mthly_num_of_invoiced_items_in_last_3_months_vs_prior_3_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -2, a.time_month_key_dt) and a.time_month_key_dt  then 1.00 else 0 end) / nullif(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt) and dateadd(month, -3, a.time_month_key_dt  )  then 1.00 else 0 end), 0), -1)
into [customerretention].[dbo].[zz_staging_revenue_01_cloud]
from [customerretention].[dbo].[base_cloud] a
left join [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_cloud] b
on a.account_number = b.account_number 
	and a.account_type = b.line_of_business 
	and b.time_month_key_dt <= a.time_month_key_dt
left join #rackconnect c
on a.account_number = c.account_number
	and a.time_month_key = c.time_month_key
	and a.account_type = c.account_type 
group by a.account_name, 
	a.account_number, 
	a.account_type, 
	a.time_month_key, 
	a.revenue_segment, 
	a.churn_flag,
	c.is_cloud_rackconnect_linked, 
	c.is_cloud_consolidated, 
	c.is_cloud_legally_linked
;

drop table [customerretention].[dbo].[zz_staging_revenue_02_cloud]
select a.account_name, 
	a.account_number, 
	a.account_type, 
	a.time_month_key, 
	a.revenue_segment, 
	a.churn_flag,
	-- does account have
	[does_account_have_cloud_files] = case when isnull(sum(case when lower(b.product_group) = 'cloud files' then 1 else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_first_gen_servers] = case when isnull(sum(case when lower(b.product_group) = 'first gen servers' then 1 else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_next_gen_servers] = case when isnull(sum(case when lower(b.product_group) = 'next gen servers' then 1 else 0 end), 0) > 0 then 'y' else 'n' end, 
	[does_account_have_cloud_sites] = case when isnull(sum(case when lower(b.product_group) = 'cloud sites' then 1 else 0 end), 0) > 0 then 'y' else 'n' end, 
	--[does account have_credit memo] = case when isnull(sum(case when lower(b.product_group) = 'credit memo' then 1 else 0 end), 0) <> 0 then 'y' else 'n' end,
	[does_account_have_cloud_load_balancer] = case when isnull(sum(case when lower(b.product_group) = 'cloud load balancer' then 1 else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_cloud_block_storage] = case when isnull(sum(case when lower(b.product_group) = 'cloud block storage' then 1 else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_cloud_backup] = case when isnull(sum(case when lower(b.product_group) = 'cloud backup' then 1 else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_total_outgoing_bw] = case when isnull(sum(case when lower(b.product_group) = 'total outgoing bw' then 1 else 0 end), 0) > 0 then 'y' else 'n' end, 
	[does_account_have_cloud_monitoring] = case when isnull(sum(case when lower(b.product_group) = 'cloud monitoring' then 1 else 0 end), 0) > 0 then 'y' else 'n' end, 
	[does_account_have_cloud_databases] = case when isnull(sum(case when lower(b.product_group) = 'cloud databases' then 1 else 0 end), 0) > 0 then 'y' else 'n' end, 
	[does_account_have_cloud_queues] = case when isnull(sum(case when lower(b.product_group) = 'cloud queues' then 1 else 0 end), 0) > 0 then 'y' else 'n' end, 
	-- unit pricing
	[avg_unit_price_cloud_files] = isnull(avg(case when lower(b.product_group) = 'cloud files'	then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_first_gen_servers] = isnull(avg(case when lower(b.product_group) = 'first gen servers'	then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_next_gen_servers] = isnull(avg(case when lower(b.product_group) = 'next gen servers' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_sites] = isnull(avg(case when lower(b.product_group) = 'cloud sites'	then b.total_invoiced_normalized else null end), 0),
	--[avg_unit_price_credit_memo] = isnull(avg(case when lower(b.product_group) = 'credit memo'	then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_load_balancer] = isnull(avg(case when lower(b.product_group) = 'cloud load balancer'	then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_block_storage] = isnull(avg(case when lower(b.product_group) = 'cloud block storage'	then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_backup] = isnull(avg(case when lower(b.product_group) = 'cloud backup' then b.total_invoiced_normalized else null end), 0), 
	[avg_unit_price_total_outgoing_bw] = isnull(avg(case when lower(b.product_group) = 'total outgoing bw' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_monitoring] = isnull(avg(case when lower(b.product_group) = 'cloud monitoring' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_databases] = isnull(avg(case when lower(b.product_group) = 'cloud databases'	then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_queues] = isnull(avg(case when lower(b.product_group) = 'cloud queues' then b.total_invoiced_normalized else null end), 0), 
	-- how many units
	[how_many_units_cloud_files] = isnull(sum(case when lower(b.product_group) ='cloud files' then 1.00 else 0 end)/6, 0), 
	[how_many_units_first_gen_servers] = isnull(sum(case when lower(b.product_group) ='first gen servers'	then 1.00 else 0 end)/6, 0),
	[how_many_units_next_gen_servers] = isnull(sum(case when lower(b.product_group) ='next gen servers' then 1.00 else 0 end)/6, 0), 
	[how_many_units_cloud_sites] = isnull(sum(case when lower(b.product_group) ='cloud sites'	then 1.00 else 0 end)/6, 0), 
	--[how many units_credit memo] = isnull(sum(case when lower(b.product_group) ='credit memo'then 1.00 else 0 end)/6, 0),
	[how_many_units_cloud_load_balancer] = isnull(sum(case when lower(b.product_group) ='cloud load balancer' then 1.00 else 0 end)/6, 0),
	[how_many_units_cloud_block_storage] = isnull(sum(case when lower(b.product_group) ='cloud block storage' then 1.00 else 0 end)/6, 0),
	[how_many_units_cloud_backup] = isnull(sum(case when lower(b.product_group) ='cloud backup' then 1.00 else 0 end)/6, 0), 
	[how_many_units_total_outgoing_bw] = isnull(sum(case when lower(b.product_group) ='total outgoing bw' then 1.00 else 0 end)/6, 0), 
	[how_many_units_cloud_monitoring] = isnull(sum(case when lower(b.product_group) ='cloud monitoring' then 1.00 else 0 end)/6, 0), 
	[how_many_units_cloud_databases] = isnull(sum(case when lower(b.product_group) ='cloud databases' then 1.00 else 0 end)/6, 0),
	[how_many_units_cloud_queues] = isnull(sum(case when lower(b.product_group) ='cloud queues' then 1.00 else 0 end)/6, 0), 
	--pct of invoice
	[pct_of_invoice_cloud_files] = isnull(sum(case when lower(b.product_group) = 'cloud files'	then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end), 0), 0)   , 
	[pct_of_invoice_first_gen_servers] = isnull(sum(case when lower(b.product_group) = 'first gen servers'	then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end), 0), 0)	, 
	[pct_of_invoice_next_gen_servers] = isnull(sum(case when lower(b.product_group) = 'next gen servers' then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end), 0), 0)	, 
	[pct_of_invoice_cloud_sites] = isnull(sum(case when lower(b.product_group) = 'cloud sites'	then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end), 0), 0)	, 
	--[pct of invoice_credit memo] = isnull(sum(case when lower(b.product_group) = 'credit memo' then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end ), 0), 0)	, 
	[pct_of_invoice_cloud_load_balancer] = isnull(sum(case when lower(b.product_group) = 'cloud load balancer'	then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end ), 0), 0)	, 
	[pct_of_invoice_cloud_block_storage] = isnull(sum(case when lower(b.product_group) = 'cloud block storage'	then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end ), 0), 0)	, 
	[pct_of_invoice_cloud_backup] = isnull(sum(case when lower(b.product_group) = 'cloud backup' then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end ), 0), 0)	, 
	[pct_of_invoice_total_outgoing_bw] = isnull(sum(case when lower(b.product_group) = 'total outgoing bw' then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end ), 0), 0)	, 
	[pct_of_invoice_cloud_monitoring] = isnull(sum(case when lower(b.product_group) = 'cloud monitoring' then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end ), 0), 0)	, 
	[pct_of_invoice_cloud_databases] = isnull(sum(case when lower(b.product_group) = 'cloud databases' then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end ), 0), 0)	, 
	[pct_of_invoice_cloud_queues] = isnull(sum(case when lower(b.product_group) = 'cloud queues' then b.total_invoiced_normalized else 0 end) / nullif(sum( case when b.total_invoiced_normalized > 0 then  b.total_invoiced_normalized else 0 end ), 0), 0)	,
	[number_of_different_product_groups] = count(distinct b.product_group)
into [customerretention].[dbo].[zz_staging_revenue_02_cloud]
from [customerretention].[dbo].[base_cloud] a
left join [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_cloud] b with (nolock)
on a.account_number = b.account_number 
	and a.account_type = b.line_of_business 
	and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt ) and a.time_month_key_dt
	and lower(b.transaction_type) = 'inv'
group by a.account_name, 
	a.account_number, 
	a.account_type, 
	a.time_month_key, 
	a.revenue_segment, 
	a.churn_flag
;