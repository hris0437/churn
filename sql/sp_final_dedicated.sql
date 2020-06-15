USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_final_dedicated]    Script Date: 3/18/2020 3:49:57 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_final_dedicated]
as



--if not exists(select * from sys.indexes where name = 'ix_ticket_events_02_dedicated' and object_id = object_id('ticket_events_02_dedicated') )
--begin
--	create nonclustered index ix_ticket_events_02_dedicated on ticket_events_02_dedicated	([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_device_table_02_dedicated' and object_id = object_id('zz_staging_device_table_02_dedicated') )
--begin
--	create nonclustered index ix_device_table_02_dedicated on [customerretention].[dbo].[zz_staging_device_table_02_dedicated] ([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_amt_device_sku_01_dedicated' and object_id = object_id('zz_staging_device_sku_01_dedicated') )
--begin
--	create nonclustered index ix_amt_device_sku_01_dedicated on [customerretention].[dbo].[zz_staging_device_sku_01_dedicated] ([account_number],[time_month_key])
--end


--if not exists(select * from sys.indexes where name = 'ix_amt_revenue_01_dedicated' and object_id = object_id('zz_staging_revenue_01_dedicated') )
--begin
--	create nonclustered index ix_amt_revenue_01_dedicated on [customerretention].[dbo].[zz_staging_revenue_01_dedicated] ([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_amt_revenue_02_dedicated' and object_id = object_id('zz_staging_revenue_02_dedicated') )
--begin
--	create nonclustered index ix_amt_revenue_02_dedicated on [customerretention].[dbo].[zz_staging_revenue_02_dedicated] ([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_amt_opportunity_data_01_dedicated' and object_id = object_id('zz_staging_opportunity_data_01_dedicated') )
--begin
--	create nonclustered index ix_amt_opportunity_data_01_dedicated on [customerretention].[dbo].[zz_staging_opportunity_data_01_dedicated] ([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_amt_opportunity_data_02_dedicated' and object_id = object_id('zz_staging_opportunity_data_02_dedicated') )
--begin
--	create nonclustered index ix_amt_opportunity_data_02_dedicated on [customerretention].[dbo].[zz_staging_opportunity_data_02_dedicated] ([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_amt_account_data_01_dedicated' and object_id = object_id('zz_staging_account_data_01_dedicated') )
--begin
--	create nonclustered index ix_amt_account_data_01_dedicated on [customerretention].[dbo].[zz_staging_account_data_01_dedicated] ([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_amt_account_data_02_dedicated' and object_id = object_id('zz_staging_account_data_02_dedicated') )
--begin
--	create nonclustered index ix_amt_account_data_02_dedicated on [customerretention].[dbo].[zz_staging_account_data_02_dedicated] ([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_amt_nps_04_dedicated' and object_id = object_id('zz_staging_nps_04_dedicated') )
--begin
--	create nonclustered index ix_amt_nps_04_dedicated on [customerretention].[dbo].[zz_staging_nps_04_dedicated] ([account_number],[time_month_key])
--end

--if not exists(select * from sys.indexes where name = 'ix_amt_salesforce_account_dedicated' and object_id = object_id('zz_staging_salesforce_account_dedicated') )
--begin
--	create nonclustered index ix_amt_salesforce_account_dedicated on [customerretention].[dbo].[zz_staging_salesforce_account_dedicated] ([account_number],[time_month_key])
--end



/* adding code snippet to create a "first_churn_tmk" flag */ -- nr 04/04/2019
/*starts here */

select account_number, min(time_month_key) first_churn_tmk
into #first_churn_tmk  -- drop table #first_churn_tmk
from [customerretention].[dbo].[base_dedicated]
where churn_target = 1
group by account_number

select distinct abd.account_number, abd.time_month_key, --first_churn_tmk,
case when first_churn_tmk is not null then 1 else 0 end first_churn_tmk
into #first_churn_flag
from [customerretention].[dbo].[base_dedicated] abd
left join #first_churn_tmk fct on fct.account_number = abd.account_number
and abd.time_month_key = fct.first_churn_tmk


/*ends here */
	

drop table [customerretention].[dbo].[00final_dedicated]
select distinct a.time_month_key,  
	a.account_number,	
	a.account_type,	
	a.account_name,	
	account_sub_type,
	a.account_segment,
	total_invoiced_in_month,
	a.time_month_key_dt,
	time_month_key_eo_last_month_dt,
	average_invoiced_last_12_months, 
	average_invoiced_next_6_months, 
	num_months_last_12_months, 
	num_months_last_6_months, 
	num_months_next_6_months, 
	num_months_next_9_months, 
	month_order,
	month_order_desc,
	churn_pct_change,
	a.churn_flag, --categorical / text version
	churn_target, -- 0/1 numerical version
	past_6mo_baseline, --growth model
	avg_9mo_eval, -- growth model
	growth_pct_change,
	growth_target, -- 0/1 numerical version
	a.revenue_segment, --6 segment categorical variable
	revenue_segment_6mo = case when [total_invoiced_in_last_6_months] / 6 < 5000 then '1. 5k and below'
		when [total_invoiced_in_last_6_months] / 6 between 5000 and 25000 then '2. 5k to 25k'
		when [total_invoiced_in_last_6_months] / 6 > 25000 then '3. 25k and above' end, 
	isnull(first_churn_tmk,0) first_churn_tmk,	-- nr 04/04/2019 feature addiiton first churn tmk flag 1/0


--tickets 9/20/2018
	--[total_number_of_tickets],
	--[revenue_tickets_closed_last6mos],  -- modified by nr on 06/04/2019_feature addition : revenue tickets
	--[avg_days_to_close_ticket],	
	--[avg_days_to_close_ticket_category_application],
	--[avg_days_to_close_ticket_category_customer_initiated],
	--[avg_days_to_close_ticket_category_hardware],	
	--[avg_minutes_to_first_customer_comment],
	--[avg_minutes_to_first_racker_comment],
	--[avg_minutes_to_first_racker_comment_category_monitoring_alerts],
	--[avg_minutes_to_started_in_progress],
	--[avg_minutes_to_started_in_progress_severity_urgent],
	--[avg_minutes_to_started_in_progress_category_monitoring_alerts],	
	--[num_tickets_severity_emergency],
	--[num_tickets_ticket_category_monitoring_alerts],
	--[num_tickets_ticket_category_monitoring],
	--[num_tickets_ticket_category_null],
	--[num_tickets_ticket_category_application],
	--[num_tickets_ticket_category_account_management],
	--[num_tickets_ticket_category_customer_initiated],
	--[num_tickets_ticket_category_operating_services],
	--[number_of_tickets_confirmed_solved],
	--[number_of_tickets_solved],
	--[rate_of_tickets_confirmed_solved],
	--[rate_of_tickets_solved],
	
--devices 9/20/2018	
	[number_of_devices_last_month],	
	[number_of_device_status_eq_online_complete],	
	[number_of_device_status_eq_computer_no_longer_active],
	[number_of_device_status_eq_support_maintenance],
	[number_of_other_device_status],	
	[pct_of_device_status_eq_online_complete],	
	[pct_of_device_status_eq_computer_no_longer_active],
	[pct_of_device_status_eq_support_maintenance],
	[pct_of_other_device_status],	
	[average_active_device_tenure_months],	
	[longest_active_device_tenure_months],
	[shortest_active_device_tenure_months],	
	[pct_of_devices_with_contract_status_eq_out_of_contract_mtm],	
	[pct_of_devices_with_contract_status_eq_no_contract_status],	
	[pct_of_devices_with_contract_status_eq_in_contract],	
	[pct_of_devices_with_contract_status_eq_in_contract_risk_of_lapse_90_days],	
	[pct_of_revenue_with_contract_status_eq_out_of_contract_mtm],
	[pct_of_revenue_with_contract_status_eq_no_contract_status],	
	[pct_of_revenue_with_contract_status_eq_in_contract_risk_of_lapse_90_days],	
	[number_of_devices_os_firewall],	
	[number_of_devices_os_load_balancer],
	[number_of_devices_os_switch],
	[number_of_devices_os_name_linux],
	[3_mth_pct_change_number_of_device_status_eq_computer_no_longer_active],
	[3_mth_pct_change_longest_active_device_tenure_months],
	[3_mth_pct_change_number_of_devices_active_status_active],	
	[3_mth_pct_change_number_of_devices_online_status_online],
	
--skus 9/20/2018
	[avg_monthly_number_of_sku_description_eq_privatenet_last_6mo],	
	[avg_monthly_number_of_sku_description_eq_weekly_full_daily_incremental_last_6mo],	
	[avg_monthly_number_of_sku_description_eq_raid_1_last_6mo],	
	[avg_monthly_number_of_sku_name_eq_included_bandwidth_last_6mo],	
	[avg_monthly_number_of_sku_name_eq_hard_drive_size_last_6mo],	
	[avg_monthly_number_of_sku_name_eq_advanced_networking_last_6mo],	
	[avg_monthly_number_of_sku_description_eq_included_bandwidth_last_6mo],
	[avg_monthly_number_of_sku_name_eq_high_availability_last_6mo],	
	[avg_monthly_number_of_sku_name_eq_ip_allocation_last_6mo],	
	[avg_monthly_number_of_sku_name_eq_load_balancer_required_last_6mo],	
	[account_has_sku_name_eq_monitoring_last_6mo],	
	[account_has_sku_name_eq_included_bandwidth_last_6mo],	
	[account_has_sku_name_eq_dell_servers_last_6mo],	
	[account_has_sku_name_eq_hard_drive_last_6mo],
	[account_has_sku_name_eq_ip_allocation_last_6mo],	
	[account_has_sku_name_eq_support_last_6mo],

--revenue 01 9/20/2018
	[total_invoiced_in_last_6_months],	
	[avg_per_line_item_invoiced_in_last_6_months]-- as [avg_per_line_item_invoiced_in_last_6_months_original],	
	[avg_mthly_num_of_invoiced_items_in_last_6_months],
	[avg_credit_memo_in_last_6_months],
	[ratio_num_of_invoiced_items_in_last_6_months_vs_prior_6_months], --used to be total instead of ratio
	[avg_invoiced_in_last_6_months_vs_prior_6_months],	
--	[avg_mthly_num_of_invoiced_items_in_last_6_months_vs_prior_6_months],
	[ratio_num_of_invoiced_items_in_last_3_months_vs_prior_3_months], --used to be total instead of ratio
	[avg_invoiced_in_last_3_months_vs_prior_3_months],	
--	[avg_mthly_num_of_invoiced_items_in_last_3_months_vs_prior_3_months],

--revenue 02 9/20/2018
	[has_cloud],
	[does_account_have_next_gen_servers],	
	[does_account_have_aws],
	[does_account_have_cloud_block_storage],
	[does_account_have_cloud_files],	
	[does_account_have_azure],
	[does_account_have_managed_exchange],	
	[does_account_have_rackspace_email],
	[does_account_have_server],
	[does_account_have_virtual_hosting],
	[does_account_have_virtualization],	
	[does_account_have_dedicated_san],	
	[does_account_have_san],	
	[does_account_have_firewall],
	[does_account_have_load_balancer],	
	[does_account_have_rpc_core],	
	[does_account_have_switch],	
	[does_account_have_managed_storage],	
	[does_account_have_threat_manager],	
	[does_account_have_bandwidth_overages],
	[avg_unit_price_next_gen_servers],	
	[avg_unit_price_aws],	
	[avg_unit_price_cloud_block_storage],
	[avg_unit_price_cloud_files],	
	[avg_unit_price_azure],	
	[avg_unit_price_managed_exchange],	
	[avg_unit_price_rackspace_email],	
	[avg_unit_price_server],
	[avg_unit_price_virtual_hosting],
	[avg_unit_price_virtualization],	
	[avg_unit_price_dedicated_san],	
	[avg_unit_price_san],	
	[avg_unit_price_firewall],	
	[avg_unit_price_load_balancer],	
	[avg_unit_price_rpc_core],	
	[avg_unit_price_switch],	
	[avg_unit_price_managed_storage],	
	[avg_unit_price_threat_manager],	
	[avg_unit_price_bandwidth_overages],
	[how_many_units_next_gen_servers],	
	[how_many_units_aws],	
	[how_many_units_cloud_block_storage],	
	[how_many_units_cloud_files],	
	[how_many_units_azure],	
	[how_many_units_managed_exchange],	
	[how_many_units_rackspace_email],	
	[how_many_units_server],
	[how_many_units_virtual_hosting],	
	[how_many_units_virtualization],	
	[how_many_units_dedicated_san],
	[how_many_units_san],	
	[how_many_units_firewall],	
	[how_many_units_load_balancer],	
	[how_many_units_rpc_core],	
	[how_many_units_switch],	
	[how_many_units_managed_storage],	
	[how_many_units_threat_manager],	
	[how_many_units_bandwidth_overages],
	[pct_of_invoice_next_gen_servers],	
	[pct_of_invoice_aws],	
	[pct_of_invoice_cloud_block_storage],	
	[pct_of_invoice_cloud_files],	
	[pct_of_invoice_azure],	
	[pct_of_invoice_managed_exchange],	
	[pct_of_invoice_rackspace_email],	
	[pct_of_invoice_server],
	[pct_of_invoice_virtual_hosting],	
	[pct_of_invoice_virtualization],	
	[pct_of_invoice_dedicated_san],	
	[pct_of_invoice_san],	
	[pct_of_invoice_firewall],	
	[pct_of_invoice_load_balancer],	
	[pct_of_invoice_rpc_core],	
	[pct_of_invoice_switch],	
	[pct_of_invoice_managed_storage],	
	[pct_of_invoice_threat_manager],	
	[pct_of_invoice_bandwidth_overages],

--opportunity  9/20/2018
	[num_opportunities_last_6_months],	
	[num_opportunities_won_last_6_months],	
	[num_opportunities_lost_last_6_months],	
	[pct_opportunities_won_last_6_months],	
	[pct_opportunities_lost_last_6_months],	
	[num_opportunities_last_3_months],	
	[num_opportunities_won_last_3_months],	
	[num_opportunities_lost_last_3_months],	
	[pct_opportunities_won_last_3_months],	
	[pct_opportunities_lost_last_3_months],	
	[total_value_of_opportunities_last_6_months],	
	[total_value_of_opportunities_last_3_months],	
	[average_value_of_opportunities_last_6_months],	
	[average_value_of_opportunities_last_3_months],	

-- opp2 9/20/2018
	[num_opportunities_last_12_months],
	--[num_opps_allow_quote_not_allowed],	
	--[num_opps_allow_quote_allowed],	
	--[num_opps_bucket_influence_null],	
	--[num_opps_bucket_influence_marketing],	
	--[num_opps_bucket_source_sales],	
	--[num_opps_bucket_source_null],	
	--[num_opps_bucket_source_marketing],	
	--[num_opps_category_upgrade],	
	--[num_opps_category_new],	
	--[num_opps_category_new_footprint],	
	--[num_opps_category_cloud_net_revenue],	
	--[num_opps_category_new_logo],	
	[num_opps_category_migration],	
	--[num_opps_category_null],
	--[num_opps_commission_role_null],
	--[num_opps_commission_role_pay_commissions],
	--[num_opps_competitors_null],
	--[num_opps_competitors_in-house],
	--[num_opps_competitors_other],
	--[num_opps_contract_length_null],
	--[num_opps_contract_length_12],
	--[num_opps_contract_length_1],	
	--[num_opps_contract_length_0],	
	--[num_opps_contract_length_24],	
	--[num_opps_cvp_verified_false],	
	--[num_opps_cvp_verified_true],	
	--[num_opps_data_quality_description_missing:_lead_source,_next_ste],
	--[num_opps_data_quality_description_missing:_amount,_lead_source],
	--[num_opps_data_quality_description_missing:_amount,_next_steps],	
	--[num_opps_data_quality_description_missing:_next_steps],
	--[num_opps_data_quality_description_missing:_lead_source],
	--[num_opps_data_quality_description_all_opportunity_details_captur],
	--[num_opps_data_quality_description_missing:_amount],
	--[num_opps_data_quality_score_60],
	--[num_opps_data_quality_score_40],	
	--[num_opps_data_quality_score_80],	
	--[num_opps_data_quality_score_100],	
	--[num_opps_econnect_received_false],	
	--[num_opps_econnect_received_true],
	--[num_opps_focus_area_null],
	--[num_opps_focus_area_dedicated],
	--[num_opps_focus_area_cloud_office],
	--[num_opps_focus_area_openstack_public],
	--[num_opps_focus_area_tricore],
	--[num_opps_focus_area_amazon],	
	--[num_opps_forecastcategory_closed],
	--[num_opps_forecastcategory_omitted],	
	--[num_opps_forecastcategory_pipeline],	
	--[num_opps_forecastcategoryname_closed],
	--[num_opps_forecastcategoryname_omitted],	
	--[num_opps_forecastcategoryname_pipeline],	
	--[num_opps_iswon_true],
	--[num_opps_iswon_false],	
	--[num_opps_leadsource_null],
	--[num_opps_leadsource_chat],
	--[num_opps_leadsource_call_in],
	--[num_opps_leadsource_partner_network],
	--[num_opps_leadsource_outbound],
	--[num_opps_leadsource_site_submission],
	--[num_opps_leadsource_unknown],
	--[num_opps_live_call_false],
	--[num_opps_market_source_null],
	--[num_opps_market_source_no],	
	--[num_opps_market_source_yes],	
	--[num_opps_nutcase_deal_probability_0],
	--[num_opps_on_demand_reconciled_false],	
	--[num_opps_on_demand_reconciled_true],	
	--[num_opps_pain_point_null],
	--[num_opps_pain_point_other],	
	--[num_opps_pain_point_servicenow],
	--[num_opps_probability_100],
	--[num_opps_probability_0],
	--[num_opps_probability_15],	
	--[num_opps_requested_products_null],
	--[num_opps_requested_products_hosting_only],
	--[num_opps_support_unit_null],
	--[num_opps_support_unit_tricore],
	--[num_opps_support_unit_enterprise],
	[num_opps_support_unit_smb],
	--[num_opps_support_unit_email_&_apps],
	--[num_opps_ticket_type_null],
	--[num_opps_ticket_type_upgrade],
	[num_opps_typex_dedicated_private_cloud],
	[num_opps_typex_revenue_ticket],
	--[num_opps_typex_mail_contract_signup],
	--[num_opps_typex_rackspace_cloud],
	--[num_opps_typex_tricore],
	[num_opps_typex_aws],
	--[num_opps_what_did_we_do_well_null],
	--[num_opps_what_did_we_do_well_solution_fit],
	--[num_opps_why_did_we_lose_null],
	--[num_opps_why_did_we_lose_no_response],
	--[num_opps_why_did_we_lose_unresponsive],	
	--[num_opps_why_did_we_lose_project_abandoned],
	--[num_opps_why_did_we_lose_existing_opp/closed_via_ticket],
	--[num_opps_why_did_we_lose_price],
	
-- account
	[account_business_type] = coalesce([account_business_type], ''), 
	industry = coalesce(industry, ''), 


--accounts 9/20/2018
	[account_sla_type],
	[acc_tenure_in_months],	
	[lead_tech_flag], -- nr: 3/7/2019 adding lead_tech flag
--	[website_tld],



--accounts2 9/20/2018
	[num_distinct_account_sla_type],
	[num_distinct_account_business_type],
	[num_distinct_account_team_name],
	[num_distinct_account_manager],
	[num_distinct_account_bdc],
	[num_distinct_account_primary_contact],
	[num_distinct_account_region],
	[num_distinct_account_billing_city],
	[num_distinct_account_billing_state],
	[num_distinct_account_billing_postal_code],
	[num_distinct_account_billing_country],
	[num_distinct_account_geographic_location],


--nps 9/20/2018
	last_survey_responseflag = case when last_survey_score = '' then 0 else 1 end,
	last_survey_score = cast(last_survey_score as int),
	--last_survey_rating as last_survey_rating_original,
	second_last_survey_responseflag = case when second_last_survey_score = '' then 0 else 1 end,
	second_last_survey_score = cast(second_last_survey_score as int),
	--second_last_survey_rating as second_last_survey_rating_original,
	change_in_survey_score,
	rating_promoter_to_promoter,
	rating_promoter_to_passive,
	rating_promoter_to_detractor,
	rating_passive_to_promoter,
	rating_passive_to_passive,	
	rating_passive_to_detractor,	
	rating_detractor_to_promoter,	
	rating_detractor_to_passive,
	rating_detractor_to_detractor,	

	

--salesforce 9/20/2018
	shippingcountrycode,
	[ownership],
	annualrevenue as annualrevenue_original,
	numberofemployees as numberofemployees_original,
	[ownership] as ownership_original,
	[site],
	naicsdesc,
	company_age as company_age_original,
	number_of_accounts as number_of_accounts_original,
	company_review_priority,
	number_of_customer_accounts,
	number_of_cloud_accounts,
	company_priority as company_priority_original,


--price increase experiments
	priceincreaseexperimentflag = case when n.account_number is not null then 1 else 0 end

into [customerretention].[dbo].[00final_dedicated]
from [customerretention].[dbo].[base_dedicated] a with (nolock)

--left join [customerretention].[dbo].[zz_staging_ticket_events_02_dedicated] b with (nolock)			
--on a.account_number = b.account_number 
--	and a.time_month_key = b.time_month_key

left join [customerretention].[dbo].[zz_staging_device_table_02_dedicated]	c with (nolock)				
on a.account_number = c.account_number 
	and a.time_month_key = c.time_month_key

left join [customerretention].[dbo].[zz_staging_device_sku_01_dedicated] d with (nolock)			
on a.account_number = d.account_number 
	and a.time_month_key = d.time_month_key

left join [customerretention].[dbo].[zz_staging_revenue_01_dedicated] e with (nolock)				
on a.account_number = e.account_number 
	and a.time_month_key = e.time_month_key

left join [customerretention].[dbo].[zz_staging_revenue_02_dedicated] f with (nolock)				
on a.account_number = f.account_number 
	and a.time_month_key = f.time_month_key

left join [customerretention].[dbo].[zz_staging_opportunity_data_01_dedicated] g with (nolock)		
on a.account_number = g.account_number 
	and a.time_month_key = g.time_month_key

left join [customerretention].[dbo].[zz_staging_opportunity_data_02_dedicated] h with (nolock)	
on a.account_number = h.account_number 
	and a.time_month_key = h.time_month_key

left join [customerretention].[dbo].[zz_staging_account_01_dedicated] i with (nolock)
on a.account_number = i.account_number 
	and a.time_month_key = i.time_month_key

left join [customerretention].[dbo].[zz_staging_account_02_dedicated] j with (nolock)		
on a.account_number = j.account_number 
	and a.time_month_key = j.time_month_key

left join [customerretention].[dbo].[zz_staging_nps_04_dedicated] k with (nolock)					
on a.account_number = k.account_number 
	and a.time_month_key = k.time_month_key

left join [customerretention].[dbo].[zz_staging_salesforce_account_dedicated] m with (nolock)		
on a.account_number = m.account_number 
	and a.time_month_key = m.time_month_key

left join [priceincreases].[dbo].[dedicated_price_increase_pilot] n with (nolock)
on a.account_number = n.account_number	
	and a.time_month_key >= n.[notice_tmk]
	and n.pilot not like ('%control%')

/* nr 04/04/2019_feature addition : first churn tmk flag 1/0 */
left join #first_churn_flag cf on cf.account_number = a.account_number
	and a.time_month_key = cf.time_month_key
;

