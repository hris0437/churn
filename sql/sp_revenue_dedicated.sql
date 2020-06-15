USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_revenue_dedicated]    Script Date: 3/18/2020 3:51:31 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_revenue_dedicated]
as

/*	this script pulls in information on revenue with a number of different splits and calculations, e.g. changes in spend over different time periods, distribution of revenue by product,
	whether or not certain products are present etc.*/

drop table [customerretention].[dbo].[zz_staging_revenue_01_dedicated]
select a.account_number, 
	a.time_month_key, 
	-- changed date logic- shifted forward 1 month due to error in date ranges
	[total_invoiced_in_last_6_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and a.time_month_key_dt then b.total_invoiced_normalized else null end), 0),
	[avg_per_line_item_invoiced_in_last_6_months] = isnull(avg(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and a.time_month_key_dt then b.total_invoiced_normalized else null end), 0),
	[avg_mthly_num_of_invoiced_items_in_last_6_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and a.time_month_key_dt then 1.00 else 0 end), 0),
	[avg_credit_memo_in_last_6_months] = isnull(avg(case when lower(b.transaction_type) = 'cm'
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and a.time_month_key_dt then b.total_invoiced_normalized else null end), 0),
	[total_invoiced_in_last_6_months_vs_prior_6_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and a.time_month_key_dt then b.total_invoiced_normalized else null end) / 
		nullif(sum(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -11, a.time_month_key_dt) 
		and dateadd(month, -6, a.time_month_key_dt) then b.total_invoiced_normalized else null end), 0), -1),
	[avg_invoiced_in_last_6_months_vs_prior_6_months] = isnull(avg(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and a.time_month_key_dt then b.total_invoiced_normalized else null end) / 
		nullif(avg(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -11, a.time_month_key_dt) 
		and dateadd(month, -6, a.time_month_key_dt) then b.total_invoiced_normalized else null end), 0), -1),
	[ratio_num_of_invoiced_items_in_last_6_months_vs_prior_6_months] = isnull(sum(case when lower(b.transaction_type) = 'inv'  --renamed col from avg to ratio
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and a.time_month_key_dt then 1.00 else null end) / 
		nullif(sum(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -11, a.time_month_key_dt) 
		and dateadd(month, -6, a.time_month_key_dt  )  then 1.00 else null end), 0), -1),
	[total_invoiced_in_last_3_months_vs_prior_3_months] = isnull(sum(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -2, a.time_month_key_dt) 
		and a.time_month_key_dt then b.total_invoiced_normalized else null end) / 
		nullif(sum(case when lower(b.transaction_type) = 'inv' and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt ) 
		and dateadd(month, -3, a.time_month_key_dt) then b.total_invoiced_normalized else null end), 0), -1),
	[avg_invoiced_in_last_3_months_vs_prior_3_months] = isnull(avg(case when lower(b.transaction_type) = 'inv'  --renamed col from avg to ratio
		and b.time_month_key_dt between dateadd(month, -2, a.time_month_key_dt) 
		and a.time_month_key_dt then b.total_invoiced_normalized else null end) / 
		nullif(avg(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and dateadd(month, -3, a.time_month_key_dt) then b.total_invoiced_normalized else null end), 0), -1),
	[ratio_num_of_invoiced_items_in_last_3_months_vs_prior_3_months] = isnull(sum(case when lower(b.transaction_type) = 'inv'  --renamed col from avg to ratio
		and b.time_month_key_dt between dateadd(month, -2, a.time_month_key_dt) 
		and a.time_month_key_dt then 1.00 else 0 end) / 
		nullif(sum(case when lower(b.transaction_type) = 'inv' 
		and b.time_month_key_dt between dateadd(month, -5, a.time_month_key_dt) 
		and dateadd(month, -3, a.time_month_key_dt)  then 1.00 else 0 end), 0), -1)
into [customerretention].[dbo].[zz_staging_revenue_01_dedicated]
from [customerretention].[dbo].[base_dedicated] a with (nolock)
left join [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_dedicated] b with (nolock)
on a.account_number = b.account_number 
	and a.account_type = b.line_of_business 
	and b.time_month_key_dt <= a.time_month_key_dt
where total_invoiced_normalized <> '2061582367.68'
	and total_invoiced_normalized <> '-2061582367.68'
group by a.account_number, 
	a.time_month_key
;


------
-- product group
------

drop table [customerretention].[dbo].[zz_staging_revenue_02_dedicated]
select a.account_number, 
	a.time_month_key, 
-- cloud
	[has_cloud] = case when isnull(sum(case when lower(b.product_grouping) in ('managed public clouds','openstack public cloud') then 1 else 0 end), 0) > 0 then 1 else 0 end, --pw: 3/4/2019
	[does_account_have_mpc] = case when isnull(sum(case when lower(b.gl_product_focus_area_name) = 'managed public clouds' then 1 else 0 end), 0) > 0 then 'y' else 'n' end, --aw: 2/11/2020 faws, fazure, gcp
	[does_account_have_ospc] = case when isnull(sum(case when lower(b.gl_product_focus_area_name) = 'openstack public cloud' then 1 else 0 end), 0) > 0 then 'y' else 'n' end, --aw: 2/11/2020
	[does_account_have_next_gen_servers] = case when isnull(sum(case when lower(b.product_group) = 'next gen servers' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_aws] = case when isnull(sum(case when lower(b.product_group) = 'aws' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_cloud_block_storage] = case when isnull(sum(case when lower(b.product_group) = 'cloud block storage' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_cloud_files] = case when isnull(sum(case when lower(b.product_group) = 'cloud files' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_azure] = case when isnull(sum(case when lower(b.product_group) = 'azure' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
-- dedicated & mail
	[does_account_have_managed_exchange] = case when isnull(sum(case when lower(b.product_group) = 'managed exchange' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_rackspace_email] = case when isnull(sum(case when lower(b.product_group) = 'rackspace email' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
-- dedicated
	[does_account_have_server] = case when isnull(sum(case when lower(b.product_group) = 'server' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_virtual_hosting] =  case when isnull(sum(case when lower(b.product_group) = 'virtual hosting' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_virtualization] = case when isnull(sum(case when lower(b.product_group) = 'virtualization' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_dedicated_san] = case when isnull(sum(case when lower(b.product_group) = 'dedicated san' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_san] = case when isnull(sum(case when lower(b.product_group) = 'san' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_firewall] = case when isnull(sum(case when lower(b.product_group) = 'firewall' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_load_balancer] = case when isnull(sum(case when lower(b.product_group) = 'load balancer' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_rpc_core] = case when isnull(sum(case when lower(b.product_group) = 'rpc core' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_switch] = case when isnull(sum(case when lower(b.product_group) = 'switch' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_managed_storage] = case when isnull(sum(case when lower(b.product_group) = 'managed storage' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_threat_manager] = case when isnull(sum(case when lower(b.product_group) = 'threat manager' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,
	[does_account_have_bandwidth_overages] = case when isnull(sum(case when lower(b.gl_account_group) = 'bandwidth overages' then b.total_invoiced_normalized else 0 end), 0) > 0 then 'y' else 'n' end,

/* column names changed to "avg unit price" instead of "per unit price" based on the actual logic used */ -- code updated by nr by 201909- pushing this for sep 2019 re-fit

-- cloud
	[avg_unit_price_next_gen_servers] = isnull(avg(case when lower(b.product_group) = 'next gen servers' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_aws] = isnull(avg(case when lower(b.product_group) = 'aws' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_block_storage] = isnull(avg(case when lower(b.product_group) = 'cloud block storage' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_cloud_files] = isnull(avg(case when lower(b.product_group) = 'cloud files' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_azure] = isnull(avg(case when lower(b.product_group) = 'azure' then b.total_invoiced_normalized else null end), 0),
-- dedicated & mail
	[avg_unit_price_managed_exchange] = isnull(avg(case when lower(b.product_group) = 'managed exchange' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_rackspace_email] = isnull(avg(case when lower(b.product_group) = 'rackspace email' then b.total_invoiced_normalized else null end), 0),
-- dedicated
	[avg_unit_price_server] = isnull(avg(case when lower(b.product_group) = 'server' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_virtual_hosting] = isnull(avg(case when lower(b.product_group) = 'virtual hosting' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_virtualization] = isnull(avg(case when lower(b.product_group) = 'virtualization' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_dedicated_san] = isnull(avg(case when lower(b.product_group) = 'dedicated san' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_san] = isnull(avg(case when lower(b.product_group) = 'san' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_firewall] = isnull(avg(case when lower(b.product_group) = 'firewall' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_load_balancer] = isnull(avg(case when lower(b.product_group) = 'load balancer' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_rpc_core] = isnull(avg(case when lower(b.product_group) = 'rpc core' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_switch] = isnull(avg(case when lower(b.product_group) = 'switch' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_managed_storage] = isnull(avg(case when lower(b.product_group) = 'managed storage' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_threat_manager] = isnull(avg(case when lower(b.product_group) = 'threat manager' then b.total_invoiced_normalized else null end), 0),
	[avg_unit_price_bandwidth_overages] = isnull(avg(case when lower(b.gl_account_group) = 'bandwidth overages' then b.total_invoiced_normalized else null end), 0),
	
	-- cloud
--	[per unit price - next gen servers] = isnull(avg(case when lower(b.product_group) = 'next gen servers' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - aws] = isnull(avg(case when lower(b.product_group) = 'aws' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - cloud block storage] = isnull(avg(case when lower(b.product_group) = 'cloud block storage' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - cloud files] = isnull(avg(case when lower(b.product_group) = 'cloud files' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - azure] = isnull(avg(case when lower(b.product_group) = 'azure' then b.total_invoiced_normalized else null end), 0),
---- dedicated & mail
--	[per unit price - managed exchange] = isnull(avg(case when lower(b.product_group) = 'managed exchange' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - rackspace email] = isnull(avg(case when lower(b.product_group) = 'rackspace email' then b.total_invoiced_normalized else null end), 0),
---- dedicated
--	[per unit price - server] = isnull(avg(case when lower(b.product_group) = 'server' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - virtual hosting] = isnull(avg(case when lower(b.product_group) = 'virtual hosting' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - virtualization] = isnull(avg(case when lower(b.product_group) = 'virtualization' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - dedicated san] = isnull(avg(case when lower(b.product_group) = 'dedicated san' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - san] = isnull(avg(case when lower(b.product_group) = 'san' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - firewall] = isnull(avg(case when lower(b.product_group) = 'firewall' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - load balancer] = isnull(avg(case when lower(b.product_group) = 'load balancer' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - rpc core] = isnull(avg(case when lower(b.product_group) = 'rpc core' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - switch] = isnull(avg(case when lower(b.product_group) = 'switch' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - managed storage] = isnull(avg(case when lower(b.product_group) = 'managed storage' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - threat manager] = isnull(avg(case when lower(b.product_group) = 'threat manager' then b.total_invoiced_normalized else null end), 0),
--	[per unit price - bandwidth overages] = isnull(avg(case when lower(b.gl_account_group) = 'bandwidth overages' then b.total_invoiced_normalized else null end), 0),
	
-- cloud
	[how_many_units_next_gen_servers] = isnull(sum(case when lower(b.product_group) = 'next gen servers' then 1.00 else 0 end)/6, 0),
	[how_many_units_aws] = isnull(sum(case when lower(b.product_group) = 'aws' then 1.00 else 0 end)/6, 0),
	[how_many_units_cloud_block_storage] = isnull(sum(case when lower(b.product_group) = 'cloud block storage' then 1.00 else 0 end)/6, 0),
	[how_many_units_cloud_files] = isnull(sum(case when lower(b.product_group) = 'cloud files' then 1.00 else 0 end)/6, 0),
	[how_many_units_azure] = isnull(sum(case when lower(b.product_group) = 'azure' then 1.00 else 0 end)/6, 0),
-- dedicated & mail
	[how_many_units_managed_exchange] = isnull(sum(case when lower(b.product_group) = 'managed exchange' then 1.00 else 0 end)/6, 0),
	[how_many_units_rackspace_email] = isnull(sum(case when lower(b.product_group) = 'rackspace email' then 1.00 else 0 end)/6, 0),
-- dedicated
	[how_many_units_server] = isnull(sum(case when lower(b.product_group) = 'server' then 1.00 else 0 end)/6, 0),
	[how_many_units_virtual_hosting] = isnull(sum(case when lower(b.product_group) = 'virtual hosting' then 1.00 else 0 end)/6, 0),
	[how_many_units_virtualization] = isnull(sum(case when lower(b.product_group) = 'virtualization' then 1.00 else 0 end)/6, 0),
	[how_many_units_dedicated_san] = isnull(sum(case when lower(b.product_group) = 'dedicated san' then 1.00 else 0 end)/6, 0),
	[how_many_units_san] = isnull(sum(case when lower(b.product_group) = 'san' then 1.00 else 0 end)/6, 0),
	[how_many_units_firewall] = isnull(sum(case when lower(b.product_group) = 'firewall' then 1.00 else 0 end)/6, 0),
	[how_many_units_load_balancer] = isnull(sum(case when lower(b.product_group) = 'load balancer' then 1.00 else 0 end)/6, 0),
	[how_many_units_rpc_core] = isnull(sum(case when lower(b.product_group) = 'rpc core' then 1.00 else 0 end)/6, 0),
	[how_many_units_switch] = isnull(sum(case when lower(b.product_group) = 'switch' then 1.00 else 0 end)/6, 0),
	[how_many_units_managed_storage] = isnull(sum(case when lower(b.product_group) = 'managed storage' then 1.00 else 0 end)/6, 0),
	[how_many_units_threat_manager] = isnull(sum(case when lower(b.product_group) = 'threat manager' then 1.00 else 0 end)/6, 0),
	[how_many_units_bandwidth_overages] = isnull(sum(case when lower(b.gl_account_group) = 'bandwidth overages' then 1.00 else 0 end)/6, 0),
	
-- cloud
	[pct_of_invoice_next_gen_servers] = isnull(sum(case when lower(b.product_group) = 'next gen servers' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized ), 0), 0),
	[pct_of_invoice_aws] = isnull(sum(case when lower(b.product_group) = 'aws' then b.total_invoiced_normalized else 0 end) / nullif(sum( b.total_invoiced_normalized ), 0), 0),
	[pct_of_invoice_cloud_block_storage] = isnull(sum(case when lower(b.product_group) = 'cloud block storage' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized ), 0), 0),
	[pct_of_invoice_cloud_files] = isnull(sum(case when lower(b.product_group) = 'cloud files' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized ), 0), 0),
	[pct_of_invoice_azure] = isnull(sum(case when lower(b.product_group) = 'azure' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized ), 0), 0),
-- dedicated & mail																															 
	[pct_of_invoice_managed_exchange] = isnull(sum(case when lower(b.product_group) = 'managed exchange' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized ), 0), 0),
	[pct_of_invoice_rackspace_email] = isnull(sum(case when lower(b.product_group) = 'rackspace email' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized ), 0), 0),
-- dedicated																																	  
	[pct_of_invoice_server] = isnull(sum(case when lower(b.product_group) = 'server' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_virtual_hosting] = isnull(sum(case when lower(b.product_group) = 'virtual hosting' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_virtualization] = isnull(sum(case when lower(b.product_group) = 'virtualization' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_dedicated_san] = isnull(sum(case when lower(b.product_group) = 'dedicated san' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_san] = isnull(sum(case when lower(b.product_group) = 'san' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_firewall] = isnull(sum(case when lower(b.product_group) = 'firewall' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_load_balancer] = isnull(sum(case when lower(b.product_group) = 'load balancer' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_rpc_core] = isnull(sum(case when lower(b.product_group) = 'rpc core' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_switch] = isnull(sum(case when lower(b.product_group) = 'switch' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_managed_storage] = isnull(sum(case when lower(b.product_group) = 'managed storage' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_threat_manager] = isnull(sum(case when lower(b.product_group) = 'threat manager' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0),
	[pct_of_invoice_bandwidth_overages] = isnull(sum(case when lower(b.gl_account_group) = 'bandwidth overages' then b.total_invoiced_normalized else 0 end) / nullif(sum(b.total_invoiced_normalized), 0), 0)
into [customerretention].[dbo].[zz_staging_revenue_02_dedicated]
from [customerretention].[dbo].[base_dedicated] a
left join [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_dedicated] b 
on a.account_number = b.account_number 
--	and a.account_type = b.line_of_business  #removed by aw 10/16/2019 because including it will not allow cloud-related information into the table
--	and b.time_month_key_dt  between  dateadd(month, -6, a.time_month_key_dt ) and dateadd(month, -1, a.time_month_key_dt  ) #logic was off by 1 month aw 10/16/2019
	and b.time_month_key_dt  between  dateadd(month, -5, a.time_month_key_dt ) and a.time_month_key_dt
	and lower(b.transaction_type) = 'inv'
where total_invoiced_normalized <> '2061582367.68'
	and total_invoiced_normalized <> '-2061582367.68'
group by a.account_number, 
	a.time_month_key 
;



