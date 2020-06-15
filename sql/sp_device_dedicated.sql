USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_device_dedicated]    Script Date: 3/18/2020 3:48:57 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_device_dedicated]
as


/*this script calculates metrics around dedicated customer's devices, contract status and changes in devices.*/

drop table [customerretention].[dbo].[zz_staging_device_table_01_dedicated]

select a.account_number, 
	a.time_month_key, 
	a.time_month_key_dt, 
	[number_of_devices_last_month] = count(a.device_number), 
	[number_of_device_status_eq_online_complete] = sum(case when lower(device_status) = 'online/complete' then 1 else 0 end),
	[number_of_device_status_eq_computer_no_longer_active] = sum(case when lower(device_status) = 'computer no longer active' then 1 else 0 end),
	[number_of_device_status_eq_support_maintenance] = sum(case when lower(device_status) = 'support maintenance' then 1 else 0 end), --pw/nr - 03/07/2019
	[number_of_other_device_status] = sum(case when lower(device_status) not in ('online/complete', 'computer no longer active','support maintenance') then 1 else 0 end),
	[pct_of_device_status_eq_online_complete] = sum(case when lower(device_status) = 'online/complete' then 1 else 0 end) / cast(count(*) as float), 
	[pct_of_device_status_eq_computer_no_longer_active] = sum(cast(case when lower(device_status) = 'computer no longer active' then 1 else 0 end as float)) / cast(count(*) as float), 
	[pct_of_device_status_eq_support_maintenance] = sum(cast(case when lower(device_status) = 'support maintenance' then 1 else 0 end as float)) / cast(count(*) as float), --pw/nr - 03/07/2019
	[pct_of_other_device_status] = sum(cast(case when lower(device_status) not in ('online/complete', 'computer no longer active','support maintenance') then 1 else 0 end as float)) / cast(count(*) as float), 
	[average_active_device_tenure_months] = avg(case when lower(device_status) = 'online/complete' then datediff(month, device_online_date, a.time_month_key_dt) else null end), 
	[longest_active_device_tenure_months] = max(case when lower(device_status) = 'online/complete' then datediff(month, device_online_date, a.time_month_key_dt) else null end), 
	[shortest_active_device_tenure_months] = min(case when lower(device_status) = 'online/complete' then datediff(month, device_online_date, a.time_month_key_dt) else null end), 
	[pct_of_devices_with_contract_status_eq_out_of_contract_mtm] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and contract_status = 'out of contract - mtm' and contractable_device = 1 then 1.000 else 0 end as float)) / nullif(sum(cast(case when lower(device_status) = 'online/complete' and contractable_device = 1 then 1.000 else 0 end as float)), 0), 0), 
	[pct_of_devices_with_contract_status_eq_no_contract_status] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and contract_status = 'no contract status' and contractable_device = 1 then 1.000 else 0 end as float)) / nullif(sum(cast(case when lower(device_status) = 'online/complete' and contractable_device = 1 then 1.000 else 0 end as float)), 0), 0), 
	[pct_of_devices_with_contract_status_eq_in_contract] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and contract_status = 'in contract' and contractable_device = 1 then 1.000 else 0 end as float)) / nullif(sum(cast(case when lower(device_status) = 'online/complete' and contractable_device = 1 then 1.000 else 0 end as float)), 0), 0), 
	[pct_of_devices_with_contract_status_eq_in_contract_risk_of_lapse_90_days] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and contract_status = 'in contract - risk of lapse 90 days' and contractable_device = 1 then 1.000 else 0 end as float)) / nullif(sum(cast(case when lower(device_status) = 'online/complete' and contractable_device = 1 then 1.000 else 0 end as float)), 0), 0), 
	[pct_of_revenue_with_contract_status_eq_out_of_contract_mtm] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and contract_status = 'out of contract - mtm' and contractable_device = 1 then  b.total_invoiced_normalized else 0 end as float)) / nullif(sum(cast(case when lower(device_status) = 'online/complete' and contractable_device = 1 then b.total_invoiced_normalized else 0 end as float)), 0), 0), 
	[pct_of_revenue_with_contract_status_eq_no_contract_status] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and contract_status = 'no contract status'	and contractable_device = 1 then  b.total_invoiced_normalized else 0 end as float)) / nullif(sum(cast(case when lower(device_status) = 'online/complete' and contractable_device = 1 then b.total_invoiced_normalized else 0 end as float)), 0), 0), 
	[pct_of_revenue_with_contract_status_eq_in_contract] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and contract_status = 'in contract' and contractable_device = 1 then  b.total_invoiced_normalized else 0 end as float))	 / nullif(sum(cast(case when lower(device_status) = 'online/complete' and contractable_device = 1 then b.total_invoiced_normalized else 0 end as float)), 0), 0), 
	[pct_of_revenue_with_contract_status_eq_in_contract_risk_of_lapse_90_days] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and contract_status = 'in contract - risk of lapse 90 days' and contractable_device = 1 then  b.total_invoiced_normalized else 0 end as float)) / nullif(sum(cast(case when lower(device_status) = 'online/complete' and contractable_device = 1 then b.total_invoiced_normalized else 0 end as float)), 0), 0), 
	[number_of_devices_os_firewall] = sum(case when lower(device_status) = 'online/complete' and device_os in  ('firewall - cisco asa', 'firewall - cisco pix') then 1 else 0 end), 
	[number_of_devices_os_load_balancer] = sum(case when lower(device_status) = 'online/complete' and device_os in  ('load-balancer') then 1 else 0 end), 
	[number_of_devices_os_switch] = sum(case when lower(device_status) = 'online/complete' and device_os in  ('switch') then 1 else 0 end), 
	[number_of_devices_os_name_linux] = sum(case when lower(device_status) = 'online/complete' and device_os_name in  ('linux') then 1 else 0 end), 
	[pct_of_devices_bandwidth_included] = isnull(sum(cast(case when lower(device_status) = 'online/complete' and device_bandwidth_subscription in ('included bandwidth') then 1.00 else 0 end as float)) / nullif(sum(cast(case when lower(device_status) = 'online/complete' then 1.00 else 0 end as float)), 0), 0), 
	[number_of_devices_active_status_active] = sum(case when lower(device_status) = 'online/complete' and device_active_status in ('active') then 1 else 0 end), 
	[number_of_devices_online_status_online] = sum(case when lower(device_status) = 'online/complete' and device_online_status in ('online') then 1 else 0 end)
into [customerretention].[dbo].[zz_staging_device_table_01_dedicated]
from [customerretention].[dbo].[zz_staging_account_device_data_sampled_dedicated] a
left join [customerretention].[dbo].[zz_staging_revenue_data_withnewkeys_sampled_dedicated] b  
on cast(a.account_number as varchar) = cast(b.account_number as varchar) 
	and b.line_of_business = 'dedicated' 
	and a.time_month_key = b.time_month_key
	and cast(a.device_number as varchar) = cast(b.device_number as varchar)
group by a.account_number, 
	a.time_month_key, 
	a.time_month_key_dt
;


drop table [customerretention].[dbo].[zz_staging_device_table_02_dedicated]
select a.account_number, 
	a.account_type, 
	a.time_month_key, 
	a.time_month_key_dt,
	last_month.[number_of_devices_last_month],
	last_month.[number_of_device_status_eq_online_complete],
	last_month.[number_of_device_status_eq_computer_no_longer_active],
	last_month.[number_of_device_status_eq_support_maintenance], --pw/nr - 03/07/2019
	last_month.[number_of_other_device_status],
	last_month.[pct_of_device_status_eq_online_complete],
	last_month.[pct_of_device_status_eq_computer_no_longer_active],
	last_month.[pct_of_device_status_eq_support_maintenance], --pw/nr - 03/07/2019
	last_month.[pct_of_other_device_status],
	last_month.[average_active_device_tenure_months],
	last_month.[longest_active_device_tenure_months],
	last_month.[shortest_active_device_tenure_months],
	last_month.[pct_of_devices_with_contract_status_eq_out_of_contract_mtm],
	last_month.[pct_of_devices_with_contract_status_eq_no_contract_status],
	last_month.[pct_of_devices_with_contract_status_eq_in_contract],
	last_month.[pct_of_devices_with_contract_status_eq_in_contract_risk_of_lapse_90_days],
	last_month.[pct_of_revenue_with_contract_status_eq_out_of_contract_mtm],
	last_month.[pct_of_revenue_with_contract_status_eq_no_contract_status],
	last_month.[pct_of_revenue_with_contract_status_eq_in_contract],
	last_month.[pct_of_revenue_with_contract_status_eq_in_contract_risk_of_lapse_90_days],
	last_month.[number_of_devices_os_firewall],
	last_month.[number_of_devices_os_load_balancer],
	last_month.[number_of_devices_os_switch],
	last_month.[number_of_devices_os_name_linux],
	[3_mth_pct_change_number_of_device_status_eq_computer_no_longer_active] = isnull(last_month.[number_of_device_status_eq_computer_no_longer_active]*1.00 / nullif(three_months_ago.[number_of_device_status_eq_computer_no_longer_active], 0) -1 , 0),
	[3_mth_pct_change_longest_active_device_tenure_months] = isnull(last_month.[longest_active_device_tenure_months]*1.00 / nullif(three_months_ago.[longest_active_device_tenure_months], 0) -1 , 0),
	[3_mth_pct_change_number_of_devices_active_status_active] = isnull(last_month.[number_of_devices_active_status_active]*1.00 / nullif(three_months_ago.[number_of_devices_active_status_active], 0) -1 , 0),
	[3_mth_pct_change_number_of_devices_online_status_online] = isnull(last_month.[number_of_devices_online_status_online]*1.00 / nullif(three_months_ago.[number_of_devices_online_status_online], 0) -1 , 0)
into [customerretention].[dbo].[zz_staging_device_table_02_dedicated]
from [customerretention].[dbo].[base_dedicated] a with (nolock)
left join [customerretention].[dbo].[zz_staging_device_table_01_dedicated] last_month  with (nolock)
on a.account_number =  last_month.account_number 
	and a.time_month_key_dt = dateadd(month, 1, last_month.time_month_key_dt)
left join [customerretention].[dbo].[zz_staging_device_table_01_dedicated] three_months_ago  with (nolock)
on a.account_number =  three_months_ago.account_number 
	and a.time_month_key_dt = dateadd(month, 3, three_months_ago.time_month_key_dt)
;
