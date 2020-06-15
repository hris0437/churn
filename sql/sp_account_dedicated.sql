USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_account_dedicated]    Script Date: 3/18/2020 3:47:44 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_account_dedicated]
as

drop table [customerretention].[dbo].[zz_staging_account_01_dedicated]
select distinct a.account_name, 
	a.account_number, 
	a.account_type, 
	a.time_month_key, 
	b.account_sla_type,
	account_business_type = case when lower(b.account_business_type) in ('other','it services/software','business services/management consulting','retail/wholesale trade','advertising/marketing services','financial services','publishing/media/social media/content portal','healthcare','education/recruitment','web design/development')
		then lower(b.account_business_type) else 'misc smaller business type' end,
	acc_tenure_in_months = datediff(month, b.account_created_date, a.time_month_key_dt),
	[website tld] = case when replace(parsename(lower(b.account_website), 1), '/', '') in ('unknown','com','uk','net','org',',au','nl','edu','ie') 
		then replace(parsename(lower(b.account_website), 1), '/', '') else 'other' end,
	lead_tech_flag -- nr: 3/7/2019 adding lead_tech flag	
into [customerretention].[dbo].[zz_staging_account_01_dedicated]
from [customerretention].[dbo].[base_dedicated] a with (nolock)
left join [customerretention].[dbo].[zz_staging_account_data_sampled_dedicated] b with (nolock)
on a.account_number = b.account_number 
	and b.time_month_key_dt = a.time_month_key_dt
;

drop table [customerretention].[dbo].[zz_staging_account_02_dedicated]
select distinct a.account_name, 
	a.account_number, 
	a.account_type, 
	a.time_month_key, 
	[num_distinct_account_sla_type] = count(distinct b.account_sla_type),
	[num_distinct_account_business_type] = count(distinct b.account_business_type), 
	[num_distinct_account_team_name] = count(distinct b.account_team_name), 
	[num_distinct_account_manager] = count(distinct b.account_manager), 
	[num_distinct_account_bdc] = count(distinct b.account_bdc), 
	[num_distinct_account_primary_contact] = count(distinct b.account_primary_contact), 
	[num_distinct_account_region] = count(distinct b.account_region), 
	[num_distinct_account_billing_city] = count(distinct b.account_billing_city), 
	[num_distinct_account_billing_state] = count(distinct b.account_billing_state), 
	[num_distinct_account_billing_postal_code] = count(distinct b.account_billing_postal_code), 
	[num_distinct_account_billing_country] = count(distinct b.account_billing_country), 
	[num_distinct_account_geographic_location] = count(distinct b.account_geographic_location) 
into [customerretention].[dbo].[zz_staging_account_02_dedicated]
from [customerretention].[dbo].[base_dedicated] a with (nolock)
left join [customerretention].[dbo].[zz_staging_account_data_sampled_dedicated] b with (nolock)
on a.account_number = b.account_number 
	and b.time_month_key_dt between dateadd(month, -11, a.time_month_key_dt) and a.time_month_key_dt
group by a.account_name, 
	a.account_number, 
	a.account_type, 
	a.time_month_key 
;

