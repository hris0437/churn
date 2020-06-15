USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_account_cloud]    Script Date: 6/8/2020 2:17:22 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_account_cloud]
as
--as
drop table [customerretention].[dbo].[zz_staging_account_data_01_cloud]
select a.account_name, 
	a.account_number, 
	a.account_type, 
	b.mi_mo,
	tam_changed = b.am_changed,
	tam_acct_ratio = b.act_am_ratio,
	tam_ratio_changed = b.am_ratio_changed,
	--b.dedicated_risk, -- removed 3/20/2020 AJW not in final model
	a.time_month_key, 
	a.time_month_key_dt, 
	a.revenue_segment, 
	a.churn_flag,
	b.account_sla_type,
	account_business_type = case when lower(b.account_business_type) in ('other','it services/software','business services/management consulting',
		'retail/wholesale trade','advertising/marketing services','financial services','publishing/media/social media/content portal','healthcare',
		'education/recruitment','web design/development') 
		then lower(b.account_business_type)
		else 'misc smaller business type' end,
	acc_tenure_in_months = datediff(month, c.account_created_date, a.time_month_key_dt),
	[website_tld] = case when replace(parsename(lower(b.account_website), 1), '/', '') in ('unknown','com','uk','net','org',',au','nl','edu','ie') 
		then replace(parsename(lower(b.account_website), 1), '/', '') 
		else 'other' end 
into [customerretention].[dbo].[zz_staging_account_data_01_cloud]
from [customerretention].[dbo].[base_cloud] a
left join [customerretention].[dbo].[zz_staging_account_data_sampled_cloud] b 
on a.account_number = b.account_number 
	and b.time_month_key_dt = a.time_month_key_dt
left join (
			select distinct number,
				account_created_date = [createddate]
			from [ods].[cms_ods].[dbo].[customer_account] with (nolock)
			where lower([type]) = 'cloud'
			) c
on a.account_number = c.number
;

drop table [customerretention].[dbo].[zz_staging_account_data_02_cloud]
select a.account_name, 
	a.account_number, 
	a.account_type,
	a.time_month_key, 
	a.revenue_segment, 
	a.churn_flag, 
	[num_distinct_account_sla_type] = count(distinct b.account_sla_type),
	--[num_distinct_account_sla_type_desc] = count(distinct b.account_sla_type_desc), 
	[num_distinct_account_business_type] = count(distinct b.account_business_type), 
	[num_distinct_account_team_name] = count(distinct b.account_team_name), 
	[num_distinct_account_manager] = count(distinct b.account_manager), 
	[num_distinct_account_bdc] = count(distinct b.account_bdc), 
	[num_distinct_account_primary_contact] = count(distinct b.account_primary_contact), 
	[num_distinct_account_region] = count(distinct b.account_region), 
	[num_distinct_account_billing_street] = count(distinct b.account_billing_street), 
	[num_distinct_account_billing_city] = count(distinct b.account_billing_city), 
	[num_distinct_account_billing_state] = count(distinct b.account_billing_state), 
	[num_distinct_account_billing_postal_code] = count(distinct b.account_billing_postal_code), 
	[num_distinct_account_billing_country] = count(distinct b.account_billing_country), 
	[num_distinct_account_geographic_location] = count(distinct b.account_geographic_location) 
into [customerretention].[dbo].[zz_staging_account_data_02_cloud]
from [customerretention].[dbo].[base_cloud] a
left join [customerretention].[dbo].[account_data_sampled_cloud] b 
on a.account_number = b.account_number 
	and b.time_month_key_dt between dateadd(month, -11, a.time_month_key_dt) and a.time_month_key_dt
group by a.account_name, 
	a.account_number, 
	a.account_type, 
	a.time_month_key, 
	a.revenue_segment, 
	a.churn_flag
;

