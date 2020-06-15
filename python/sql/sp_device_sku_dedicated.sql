USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_device_sku_dedicated]    Script Date: 4/23/2020 2:13:59 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_device_sku_dedicated]
as

/*
	this script calculates metrics around individual skus on an account.
*/

	drop table [customerretention].[dbo].[zz_staging_device_sku_01_dedicated]
	select a.account_name
	, a.account_number
	, a.account_type
	, a.time_month_key

	, sum(case when lower(b.sku_description) = 'privatenet' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_description_eq_privatenet_last_6mo]
	, sum(case when lower(b.sku_description) = 'weekly full + daily incremental' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_description_eq_weekly_full_daily_incremental_last_6mo]
	, sum(case when lower(b.sku_description) = 'raid 1' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_description_eq_raid_1_last_6mo]
	, sum(case when lower(b.sku_name) = 'included bandwidth' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_name_eq_included_bandwidth_last_6mo]
	, sum(case when lower(b.sku_name) = 'hard drive size' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_name_eq_hard_drive_size_last_6mo]
	, sum(case when lower(b.sku_name) = 'advanced_networking' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_name_eq_advanced_networking_last_6mo]
	, sum(case when lower(b.sku_description) = 'included bandwidth' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_description_eq_included_bandwidth_last_6mo]
	, sum(case when lower(b.sku_name) = 'high availability' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_name_eq_high_availability_last_6mo]
	, sum(case when lower(b.sku_name) = 'ip allocation' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_name_eq_ip_allocation_last_6mo]
	, sum(case when lower(b.sku_name) = 'load-balancer required' then 1.00 else 0 end)/6 
		as [avg_monthly_number_of_sku_name_eq_load_balancer_required_last_6mo]

	, case when sum(case when lower(b.sku_name) = 'monitoring' then 1.00 else 0 end) > 0 then 1 else 0 end 
		as [account_has_sku_name_eq_monitoring_last_6mo]
	, case when sum(case when lower(b.sku_name) = 'included bandwidth' then 1.00 else 0 end) > 0 then 1 else 0 end 
		as [account_has_sku_name_eq_included_bandwidth_last_6mo]
	, case when sum(case when lower(b.sku_name) = 'dell servers' then 1.00 else 0 end) > 0 then 1 else 0 end 
		as [account_has_sku_name_eq_dell_servers_last_6mo]
	, case when sum(case when lower(b.sku_name) = 'hard drive' then 1.00 else 0 end) > 0 then 1 else 0 end 
		as [account_has_sku_name_eq_hard_drive_last_6mo]
	, case when sum(case when lower(b.sku_name) = 'ip allocation' then 1.00 else 0 end) > 0 then 1 else 0 end 
		as [account_has_sku_name_eq_ip_allocation_last_6mo]
	, case when sum(case when lower(b.sku_name) = 'support' then 1.00 else 0 end) > 0 then 1 else 0 end
		as [account_has_sku_name_eq_support_last_6mo]

	into [customerretention].[dbo].[zz_staging_device_sku_01_dedicated]
	from [customerretention].[dbo].[base_dedicated] a with (nolock)
	left join [customerretention].[dbo].[zz_staging_device_sku_data_sampled_dedicated] b with (nolock)
	on a.account_number = b.account_number 
		and b.[time_month_key_dt] between dateadd(month, -5, a.[time_month_key_dt]) and a.[time_month_key_dt]
	group by a.account_name
	, a.account_number
	, a.account_type
	, a.time_month_key

