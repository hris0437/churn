USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_opportunity_cloud]    Script Date: 3/18/2020 3:50:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_opportunity_cloud] as

 
/*
	this script pulls in information in the sales/opportunities table including account level information.
*/

drop table [customerretention].[dbo].[zz_staging_opportunity_data_01_cloud]
select a.account_name, 
	a.account_number, 
	a.account_type, 
	a.account_segment, 
	a.time_month_key, 
	a.time_month_key_dt, 
	a.revenue_segment, 
	a.churn_flag, 
	[num_opportunities_last_6_months] = sum(case when closedate between dateadd(mm, -5, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 
	[num_opportunities_won_last_6_months] = sum(case when lower(stagename) = 'closed won' and closedate between dateadd(mm, -5, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 
	[num_opportunities_lost_last_6_months] = sum(case when lower(stagename) = 'closed lost' and closedate between dateadd(mm, -5, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 
	[pct_opportunities_won_last_6_months] = isnull(sum(case when lower(stagename) = 'closed won' and closedate between dateadd(mm, -5, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end) / 
			nullif(sum(case when closedate between dateadd(mm, -5, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 0), 0), 
	[pct_opportunities_lost_last_6_months] = isnull(sum(case when lower(stagename) = 'closed lost' and closedate between dateadd(mm, -6, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end) /
			nullif(sum(case when closedate between dateadd(mm, -5, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 0), 0), 
	[num_opportunities_last_3_months] = sum(case when closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 
	[num_opportunities_won_last_3_months] = sum(case when lower(stagename) = 'closed won' and closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 
	[num_opportunities_lost_last_3_months] = sum(case when lower(stagename) = 'closed lost' and closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 
	[pct_opportunities_won_last_3_months] = isnull(sum(case when lower(stagename) = 'closed won' and closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end) / 
			nullif(sum(case when closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 0), 0), 
	[pct_opportunities_lost_last_3_months] = isnull(sum(case when lower(stagename) = 'closed lost' and closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end) /
			nullif(sum(case when closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then 1.00 else 0 end), 0), 0), 
	[total_value_of_opportunities_last_6_months] = sum(case when closedate between dateadd(mm, -5, a.time_month_key_dt) and a.time_month_key_dt then amount else 0 end), 
	[total_value_of_opportunities_last_3_months] = sum(case when closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then amount else 0 end), 
	[average_value_of_opportunities_last_6_months] = avg(case when closedate between dateadd(mm, -5, a.time_month_key_dt) and a.time_month_key_dt then amount else null end), 
	[average_value_of_opportunities_last_3_months] = avg(case when closedate between dateadd(mm, -2, a.time_month_key_dt) and a.time_month_key_dt then amount else null end)
into [customerretention].[dbo].[zz_staging_opportunity_data_01_cloud]
from [customerretention].[dbo].[base_cloud] a
left join [customerretention].[dbo].[zz_staging_sfopp_dw] b 
on a.account_number = b.ddi 
	and b.closedate between dateadd(mm, -11, a.time_month_key_dt) and a.time_month_key_dt
group by a.account_name, 
	a.account_number, 
	a.account_type, 
	a.account_segment, 
	a.time_month_key, 
	a.time_month_key_dt, 
	a.revenue_segment, 
	a.churn_flag
;


drop table [customerretention].[dbo].[zz_staging_opportunity_data_02_cloud]
select a.account_name, 
	a.account_number, 
	a.account_type, 
	a.account_segment, 
	a.time_month_key, 
	a.time_month_key_dt, 
	a.revenue_segment, 
	a.churn_flag,
	[num_opportunities_last_12_months] = sum(case when b.account_number <> 'null' then 1 else 0 end),
	[num_opps_allow_quote_not_allowed] = sum(case when lower(allow_quote) = 'not allowed' then 1 else 0 end),
	[num_opps_allow_quote_allowed] = sum(case when lower(allow_quote) = 'allowed' then 1 else 0 end),
	[num_opps_bucket_influence_null] = sum(case when lower(bucket_influence) = 'null' then 1 else 0 end),
	[num_opps_bucket_influence_marketing] = sum(case when lower(bucket_influence) = 'marketing' then 1 else 0 end),
	[num_opps_bucket_source_sales] = sum(case when lower(bucket_source) = 'sales' then 1 else 0 end),
	[num_opps_bucket_source_null] = sum(case when lower(bucket_source) = 'null' then 1 else 0 end),
	[num_opps_bucket_source_marketing] = sum(case when lower(bucket_source) = 'marketing' then 1 else 0 end),
	[num_opps_category_upgrade] = sum(case when lower(category) = 'upgrade' then 1 else 0 end),
	[num_opps_category_new] = sum(case when lower(category) = 'new' then 1 else 0 end),
	[num_opps_category_new_footprint] = sum(case when lower(category) = 'new footprint' then 1 else 0 end),
	[num_opps_category_cloud_net_revenue] = sum(case when lower(category) = 'cloud net revenue' then 1 else 0 end),
	[num_opps_category_new_logo] = sum(case when lower(category) = 'new logo' then 1 else 0 end),
	[num_opps_category_migration] = sum(case when lower(category) = 'migration' then 1 else 0 end),
	[num_opps_category_null] = sum(case when lower(category) = 'null' then 1 else 0 end),
	[num_opps_commission_role_null] = sum(case when lower(commission_role) = 'null' then 1 else 0 end),
	[num_opps_commission_role_pay_commissions] = sum(case when lower(commission_role) = 'pay commissions' then 1 else 0 end),
	[num_opps_competitors_null] = sum(case when cast(lower(competitors) as varchar) = 'null' then 1 else 0 end),
	[num_opps_competitors_in-house] = sum(case when cast(lower(competitors) as varchar) = 'in-house' then 1 else 0 end),
	[num_opps_competitors_other] = sum(case when cast(lower(competitors) as varchar) = 'other' then 1 else 0 end),
	[num_opps_contract_length_null] = sum(case when contract_length is null then 1 else 0 end),
	[num_opps_contract_length_12] = sum(case when contract_length = 12 then 1 else 0 end),
	[num_opps_contract_length_1] = sum(case when contract_length = 1 then 1 else 0 end),
	[num_opps_contract_length_0] = sum(case when contract_length = 0 then 1 else 0 end),
	[num_opps_contract_length_24] = sum(case when contract_length = 24 then 1 else 0 end),
	[num_opps_cvp_verified_false] = sum(case when cvp_verified = 'false' then 1 else 0 end),
	[num_opps_cvp_verified_true] = sum(case when cvp_verified = 'true' then 1 else 0 end),
	[num_opps_data_quality_description_missing_lead_source_next_ste] = sum(case when lower(data_quality_description) = 'missing: lead source, next ste' then 1 else 0 end),
	[num_opps_data_quality_description_missing_amount_lead_source] = sum(case when lower(data_quality_description) = 'missing: amount, lead source, ' then 1 else 0 end),
	[num_opps_data_quality_description_missing_amount_next_steps] = sum(case when lower(data_quality_description) = 'missing: amount, next steps' then 1 else 0 end),
	[num_opps_data_quality_description_missing_next_steps] = sum(case when lower(data_quality_description) = 'missing: next steps' then 1 else 0 end),
	[num_opps_data_quality_description_missing_lead_source] = sum(case when lower(data_quality_description) = 'missing: lead source,' then 1 else 0 end),
	[num_opps_data_quality_description_all_opportunity_details_captur] = sum(case when lower(data_quality_description) = 'all opportunity details captur' then 1 else 0 end),
	[num_opps_data_quality_description_missing_amount] = sum(case when lower(data_quality_description) = 'missing: amount,' then 1 else 0 end),
	[num_opps_data_quality_score_60] = sum(case when data_quality_score = 60 then 1 else 0 end),
	[num_opps_data_quality_score_40] = sum(case when data_quality_score = 40 then 1 else 0 end),
	[num_opps_data_quality_score_80] = sum(case when data_quality_score = 80 then 1 else 0 end),
	[num_opps_data_quality_score_100] = sum(case when data_quality_score = 100 then 1 else 0 end),
	[num_opps_econnect_received_false] = sum(case when lower(econnect_received) = 'false' then 1 else 0 end),
	[num_opps_econnect_received_true] = sum(case when lower(econnect_received) = 'true' then 1 else 0 end),
	[num_opps_focus_area_null] = sum(case when cast(lower(focus_area) as varchar) = 'null' then 1 else 0 end),
	[num_opps_focus_area_dedicated] = sum(case when cast(lower(focus_area) as varchar) = 'dedicated' then 1 else 0 end),
	[num_opps_focus_area_cloud_office] = sum(case when cast(lower(focus_area) as varchar) = 'cloud office' then 1 else 0 end),
	[num_opps_focus_area_openstack_public] = sum(case when cast(lower(focus_area) as varchar) = 'openstack public' then 1 else 0 end),
	[num_opps_focus_area_tricore] = sum(case when cast(lower(focus_area) as varchar) = 'tricore' then 1 else 0 end),
	[num_opps_focus_area_amazon] = sum(case when cast(lower(focus_area) as varchar) = 'amazon' then 1 else 0 end),
	[num_opps_forecastcategory_closed] = sum(case when lower(forecastcategory) = 'closed' then 1 else 0 end),
	[num_opps_forecastcategory_omitted] = sum(case when lower(forecastcategory) = 'omitted' then 1 else 0 end),
	[num_opps_forecastcategory_pipeline] = sum(case when lower(forecastcategory) = 'pipeline' then 1 else 0 end),
	[num_opps_forecastcategoryname_closed] = sum(case when lower(forecastcategoryname) = 'closed' then 1 else 0 end),
	[num_opps_forecastcategoryname_omitted] = sum(case when lower(forecastcategoryname) = 'omitted' then 1 else 0 end),
	[num_opps_forecastcategoryname_pipeline] = sum(case when lower(forecastcategoryname) = 'pipeline' then 1 else 0 end),
	[num_opps_iswon_true] = sum(case when iswon = 'true' then 1 else 0 end),
	[num_opps_iswon_false] = sum(case when iswon = 'false' then 1 else 0 end),
	[num_opps_leadsource_null] = sum(case when leadsource = 'null' then 1 else 0 end),
	[num_opps_leadsource_chat] = sum(case when leadsource = 'chat' then 1 else 0 end),
	[num_opps_leadsource_call_in] = sum(case when leadsource = 'call in' then 1 else 0 end),
	[num_opps_leadsource_partner_network] = sum(case when lower(leadsource) = 'partner network' then 1 else 0 end),
	[num_opps_leadsource_outbound] = sum(case when lower(leadsource) = 'outbound' then 1 else 0 end),
	[num_opps_leadsource_site_submission] = sum(case when lower(leadsource) = 'site submission' then 1 else 0 end),
	[num_opps_leadsource_unknown] = sum(case when lower(leadsource) = 'unknown' then 1 else 0 end),
	[num_opps_live_call_false] = sum(case when lower(live_call) = 'false' then 1 else 0 end),
	[num_opps_market_source_null] = sum(case when lower(market_source) = 'null' then 1 else 0 end),
	[num_opps_market_source_no] = sum(case when lower(market_source) = 'no' then 1 else 0 end),
	[num_opps_market_source_yes] = sum(case when lower(market_source) = 'yes' then 1 else 0 end),
	[num_opps_nutcase_deal_probability_0] = sum(case when nutcase_deal_probability = '0' then 1 else 0 end),
	[num_opps_on_demand_reconciled_false] = sum(case when lower(on_demand_reconciled) = 'false' then 1 else 0 end),
	[num_opps_on_demand_reconciled_true] = sum(case when lower(on_demand_reconciled) = 'true' then 1 else 0 end),
	[num_opps_pain_point_null] = sum(case when cast(lower(pain_point) as varchar) = 'null' then 1 else 0 end),
	[num_opps_pain_point_other] = sum(case when cast(lower(pain_point) as varchar) = 'other' then 1 else 0 end),
	[num_opps_pain_point_servicenow] = sum(case when cast(lower(pain_point) as varchar) = 'servicenow' then 1 else 0 end),
	[num_opps_probability_100] = sum(case when probability = '100' then 1 else 0 end),
	[num_opps_probability_0] = sum(case when probability = '0' then 1 else 0 end),
	[num_opps_probability_15] = sum(case when probability = '15' then 1 else 0 end),
	[num_opps_requested_products_null] = sum(case when cast(lower(requested_products) as varchar)= 'null' then 1 else 0 end),
	[num_opps_requested_products_hosting_only] = sum(case when cast(lower(requested_products) as varchar) = 'hosting only' then 1 else 0 end),
	[num_opps_support_unit_null] = sum(case when lower(support_unit) = 'null' then 1 else 0 end),
	[num_opps_support_unit_tricore] = sum(case when lower(support_unit) = 'tricore' then 1 else 0 end),
	[num_opps_support_unit_enterprise] = sum(case when lower(support_unit) = 'enterprise' then 1 else 0 end),
	[num_opps_support_unit_smb] = sum(case when lower(support_unit) = 'smb' then 1 else 0 end),
	[num_opps_support_unit_email_apps] = sum(case when lower(support_unit) = 'email & apps' then 1 else 0 end),
	[num_opps_ticket_type_null] = sum(case when cast(lower(ticket_type) as varchar) = 'null' then 1 else 0 end),
	[num_opps_ticket_type_upgrade] = sum(case when cast(lower(ticket_type) as varchar) = 'upgrade' then 1 else 0 end),
	[num_opps_typex_dedicated_private_cloud] = sum(case when cast(lower(typex) as varchar) = 'dedicated/private cloud' then 1 else 0 end),
	[num_opps_typex_revenue_ticket] = sum(case when cast(lower(typex) as varchar) = 'revenue ticket' then 1 else 0 end),
	[num_opps_typex_mail_contract_signup] = sum(case when lower(typex) = 'mail contract signup' then 1 else 0 end),
	[num_opps_typex_rackspace_cloud] = sum(case when lower(typex) = 'rackspace cloud' then 1 else 0 end) ,
	[num_opps_typex_tricore] = sum(case when lower(typex) = 'tricore' then 1 else 0 end),
	[num_opps_typex_aws] = sum(case when lower(typex) = 'aws' then 1 else 0 end),
	[num_opps_what_did_we_do_well_null] = sum(case when cast(lower(what_did_we_do_well) as varchar) = 'null' then 1 else 0 end),
	[num_opps_what_did_we_do_well_solution_fit] = sum(case when cast(lower(what_did_we_do_well) as varchar) = 'solution fit' then 1 else 0 end),
	[num_opps_why_did_we_lose_null] = sum(case when cast(lower(why_did_we_lose) as varchar) = 'null' then 1 else 0 end),
	[num_opps_why_did_we_lose_no_response] = sum(case when cast(lower(why_did_we_lose) as varchar) = 'no response' then 1 else 0 end),
	[num_opps_why_did_we_lose_unresponsive] = sum(case when cast(lower(why_did_we_lose) as varchar) = 'unresponsive' then 1 else 0 end),
	[num_opps_why_did_we_lose_project_abandoned] = sum(case when cast(lower(why_did_we_lose) as varchar) = 'project abandoned' then 1 else 0 end),
	[num_opps_why_did_we_lose_existing_opp_closed_via_ticket] = sum(case when cast(lower(why_did_we_lose) as varchar) = 'existing opp/closed via ticket' then 1 else 0 end),
	[num_opps_why_did_we_lose_price] = sum(case when cast(lower(why_did_we_lose) as varchar) = 'price' then 1 else 0 end)
into [customerretention].[dbo].[zz_staging_opportunity_data_02_cloud]
from [customerretention].[dbo].[base_cloud] a
left join [customerretention].[dbo].[zz_staging_sfopp_dw] b 
on a.account_number = b.ddi 
	and b.closedate between dateadd(mm, -11, a.time_month_key_dt) and a.time_month_key_dt
group by a.account_name, 
	a.account_number, 
	a.account_type, 
	a.account_segment, 
	a.time_month_key, 
	a.time_month_key_dt, 
	a.revenue_segment, 
	a.churn_flag
;
