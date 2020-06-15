USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_final_cloud]    Script Date: 3/18/2020 3:49:49 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_final_cloud]
as

/*
	this script joins all intermediate tables into a final amt used for modelling and/or scoring

	the first part of this script creates relevant indices on each of the table to optimise the join.
*/


--if not exists(select * from sys.indexes where name = 'ix_zz_staging_ticket_events_02_cloud' and object_id = object_id('zz_staging_ticket_events_02_cloud') )
--	begin
--		create nonclustered index ix_ticket_events_02_cloud on zz_staging_ticket_events_02_cloud	([account_number],[time_month_key])
--	end

--if not exists(select * from sys.indexes where name = 'ix_zz_staging_revenue_01_cloud' and object_id = object_id('zz_staging_revenue_01_cloud') )
--	begin
--		create nonclustered index ix_zz_staging_revenue_01_cloud	on zz_staging_revenue_01_cloud	([account_number],[time_month_key])
--	end

--if not exists(select * from sys.indexes where name = 'ix_zz_staging_revenue_02_cloud' and object_id = object_id('zz_staging_revenue_02_cloud') )
--	begin
--		create nonclustered index ix_zz_staging_revenue_02_cloud	on zz_staging_revenue_02_cloud	([account_number],[time_month_key])
--	end

--if not exists(select * from sys.indexes where name = 'ix_zz_staging_opportunity_data_01_cloud' and object_id = object_id('zz_staging_opportunity_data_01_cloud') )
--	begin
--		create nonclustered index ix_zz_staging_opportunity_data_01_cloud on zz_staging_opportunity_data_01_cloud ([account_number],[time_month_key])
--	end

--if not exists(select * from sys.indexes where name = 'ix_zz_staging_opportunity_data_02_cloud' and object_id = object_id('zz_staging_opportunity_data_02_cloud') )
--	begin
--		create nonclustered index ix_zz_staging_opportunity_data_02_cloud on zz_staging_opportunity_data_02_cloud ([account_number],[time_month_key])
--	end

--if not exists(select * from sys.indexes where name = 'ix_zz_staging_account_data_01_cloud' and object_id = object_id('zz_staging_account_data_01_cloud') )
--	begin
--		create nonclustered index ix_zz_staging_account_data_01_cloud on zz_staging_account_data_01_cloud ([account_number],[time_month_key])
--	end

--if not exists(select * from sys.indexes where name = 'ix_zz_staging_account_data_02_cloud' and object_id = object_id('zz_staging_account_data_02_cloud') )
--	begin
--		create nonclustered index ix_zz_staging_account_data_02_cloud on zz_staging_account_data_02_cloud ([account_number],[time_month_key])
--	end

--if not exists(select * from sys.indexes where name = 'ix_zz_staging_nps_04_cloud' and object_id = object_id('zz_staging_nps_04_cloud') )
--	begin
--		create nonclustered index ix_zz_staging_nps_04_cloud	on zz_staging_nps_04_cloud ([account_number],[time_month_key])
--	end

--if not exists(select * from sys.indexes where name = 'ix_zz_staging_salesforce_account_cloud' and object_id = object_id('zz_staging_salesforce_account_cloud') )
--	begin
--		create nonclustered index ix_zz_staging_salesforce_account_cloud	on zz_staging_salesforce_account_cloud ([account_number],[time_month_key])
--	end	

	
/* addign code snippet to create a "first_churn_tmk" flag */ -- nr 04/04/2019
/*starts here */

select account_number, min(time_month_key) first_churn_tmk
into #first_churn_tmk  -- drop table #first_churn_tmk
from [customerretention].[dbo].[base_cloud]
where pct_change <= -0.3
group by account_number

select distinct abd.account_number, abd.time_month_key, --first_churn_tmk,
case when first_churn_tmk is not null then 1 else 0 end first_churn_tmk
into #first_churn_flag -- drop table #first_churn_flag
from [customerretention].[dbo].[base_cloud] abd
left join #first_churn_tmk fct on fct.account_number = abd.account_number
and abd.time_month_key = fct.first_churn_tmk

/*ends here */	
	
drop table [customerretention].[dbo].[00final_cloud]
select a.account_number,
	a.time_month_key,
	a.time_month_key_dt,
	a.account_name,
	a.average_invoiced_last_12_months, 
	a.revenue_segment, 
	avg_monthly_invoice_band = case when [total_invoiced_in_last_6_months] / 6 < 1000 then '1k and below'
		when [total_invoiced_in_last_6_months] / 6 between 1000 and 10000 then '1k to 10k'
		when [total_invoiced_in_last_6_months] / 6 > 10000 then '10k and above' end,
	a.churn_flag, -- categorical text version
	a.target, -- 0/1 numerical version
	pct_change,
	isnull(first_churn_tmk,0) first_churn_tmk,	-- nr 04/04/2019 feature addiiton first churn tmk flag 1/0

	----Invoicing + account age----
	total_invoiced_in_month,
	average_invoiced_next_6_months,
	num_months_last_12_months,
	num_months_next_6_months,	
	month_order,
	month_order_desc,
	time_month_key_eo_last_month_dt,

	---- account data ----
	[account_type_cloud_uk] = case when lower(a.account_type)='cloud uk' then 1 else 0 end,
	mi_mo = case when x.mi_mo is null then 'unassigned' else x.mi_mo end,
	[mi_mo_core] = case when lower(x.mi_mo) = 'legacy core' then 1 else 0 end,
	[mi_mo_managed] = case when lower(x.mi_mo) = 'legacy managed' then 1 else 0 end,
	[mi_mo_mi] = case when x.mi_mo = 'mi' then 1 else 0 end,
	[mi_mo_mo] = case when x.mi_mo = 'mo' then 1 else 0 end,
	[mi_mo_unassigned] = case when x.mi_mo is null then 1 else 0 end,
	tam_changed = case when x.tam_changed is null then 0 
		when x.tam_changed='n' then 0 
		else 1 end,
	tam_ratio_changed = case when x.tam_ratio_changed is null then 0 
		when x.tam_ratio_changed = 'y' then 1
		when x.tam_ratio_changed = 'n' then 0 end,
	dedicated_risk = case when x.dedicated_risk is null then 'none' else x.dedicated_risk end,
	x.tam_acct_ratio,
	h.is_cloud_rackconnect_linked,
	--a.pct_change,
	[industry],
	[naicsdesc],
	[seasonal_control] = case 
		when right(a.[time_month_key], 2) in ('10', '11', '12', '01', '02', '03') then 1
		else 0 end,
	account_sub_type,
	a.account_segment,
	
	----Ticketing----
	--[total_number_of_tickets],
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
	--[number_of_tickets_feedback_received],
	--[number_of_tickets_solved],
	--[rate_of_tickets_feedback_received],
	--[rate_of_tickets_solved],
	--[number_of_commie_tickets],

	----Revenue----
	[total_invoiced_in_last_6_months],	
	[avg_per_line_item_invoiced_in_last_6_months],		
	[avg_mthly_num_of_invoiced_items_in_last_6_months],	
	[total_invoiced_in_last_6_months_vs_prior_6_months],	
	[avg_invoiced_in_last_6_months_vs_prior_6_months],	
	[ratio_mthly_num_of_invoiced_items_in_last_6_months_vs_prior_6_months],
	[total_invoiced_in_last_3_months_vs_prior_3_months],	
	[avg_invoiced_in_last_3_months_vs_prior_3_months],	
	[ratio_mthly_num_of_invoiced_items_in_last_3_months_vs_prior_3_months],

	----Product configuration----
	[does_account_have_next_gen_servers],	
	[does_account_have_cloud_block_storage],
	[does_account_have_cloud_files],	
	[avg_unit_price_next_gen_servers],	
	[avg_unit_price_cloud_block_storage],
	[avg_unit_price_cloud_files],	
	[avg_unit_price_first_gen_servers],
	[avg_unit_price_cloud_load_balancer],
	[avg_unit_price_cloud_sites],
	[avg_unit_price_cloud_backup],
	[avg_unit_price_total_outgoing_bw],
	[avg_unit_price_cloud_monitoring],
	[avg_unit_price_cloud_databases],
	[avg_unit_price_cloud_queues],
	[how_many_units_next_gen_servers],	
	[how_many_units_cloud_block_storage],	
	[how_many_units_cloud_files],	
	[how_many_units_first_gen_servers],
	[how_many_units_cloud_load_balancer],
	[how_many_units_cloud_sites],
	[how_many_units_cloud_backup],
	[how_many_units_total_outgoing_bw],
	[how_many_units_cloud_monitoring],
	[how_many_units_cloud_databases],
	[how_many_units_cloud_queues],
	[pct_of_invoice_next_gen_servers],	
	[pct_of_invoice_cloud_block_storage],	
	[pct_of_invoice_cloud_files],	
	[pct_of_invoice_first_gen_servers],
	[pct_of_invoice_cloud_load_balancer],
	[pct_of_invoice_cloud_sites],
	[pct_of_invoice_cloud_backup],
	[pct_of_invoice_total_outgoing_bw],
	[pct_of_invoice_cloud_monitoring],
	[pct_of_invoice_cloud_databases],
	[pct_of_invoice_cloud_queues],

	----Opportunity----
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
	[num_opportunities_last_12_months],	
	[num_opps_allow_quote_not_allowed],	
	[num_opps_allow_quote_allowed],	
	[num_opps_bucket_influence_null],	
	[num_opps_bucket_influence_marketing],	
	[num_opps_bucket_source_sales],	
	[num_opps_bucket_source_null],	
	[num_opps_bucket_source_marketing],	
	[num_opps_category_upgrade],	
	[num_opps_category_new],	
	[num_opps_category_new_footprint],	
	[num_opps_category_cloud_net_revenue],	
	[num_opps_category_new_logo],	
	[num_opps_category_migration],	
	[num_opps_category_null],
	[num_opps_commission_role_null],
	[num_opps_commission_role_pay_commissions],
	[num_opps_competitors_null],
	[num_opps_competitors_in-house],
	[num_opps_competitors_other],
	[num_opps_contract_length_null],
	[num_opps_contract_length_12],
	[num_opps_contract_length_1],	
	[num_opps_contract_length_0],	
	[num_opps_contract_length_24],	
	[num_opps_cvp_verified_false],	
	[num_opps_cvp_verified_true],	
	[num_opps_data_quality_description_missing_lead_source_next_ste],
	[num_opps_data_quality_description_missing_amount_lead_source],
	[num_opps_data_quality_description_missing_amount_next_steps],	
	[num_opps_data_quality_description_missing_next_steps],
	[num_opps_data_quality_description_missing_lead_source],
	[num_opps_data_quality_description_all_opportunity_details_captur],
	[num_opps_data_quality_description_missing_amount],
	[num_opps_data_quality_score_60],
	[num_opps_data_quality_score_40],	
	[num_opps_data_quality_score_80],	
	[num_opps_data_quality_score_100],	
	[num_opps_econnect_received_false],	
	[num_opps_econnect_received_true],
	[num_opps_focus_area_null],
	[num_opps_focus_area_dedicated],
	[num_opps_focus_area_cloud_office],
	[num_opps_focus_area_openstack_public],
	[num_opps_focus_area_tricore],
	[num_opps_focus_area_amazon],	
	[num_opps_forecastcategory_closed],
	[num_opps_forecastcategory_omitted],	
	[num_opps_forecastcategory_pipeline],	
	[num_opps_forecastcategoryname_closed],
	[num_opps_forecastcategoryname_omitted],	
	[num_opps_forecastcategoryname_pipeline],	
	[num_opps_iswon_true],
	[num_opps_iswon_false],	
	[num_opps_leadsource_null],
	[num_opps_leadsource_chat],
	[num_opps_leadsource_call_in],
	[num_opps_leadsource_partner_network],
	[num_opps_leadsource_outbound],
	[num_opps_leadsource_site_submission],
	[num_opps_leadsource_unknown],
	[num_opps_live_call_false],
	[num_opps_market_source_null],
	[num_opps_market_source_no],	
	[num_opps_market_source_yes],	
	[num_opps_nutcase_deal_probability_0],
	[num_opps_on_demand_reconciled_false],	
	[num_opps_on_demand_reconciled_true],	
	[num_opps_pain_point_null],
	[num_opps_pain_point_other],	
	[num_opps_pain_point_servicenow],
	[num_opps_probability_100],
	[num_opps_probability_0],
	[num_opps_probability_15],	
	[num_opps_requested_products_null],
	[num_opps_requested_products_hosting_only],
	[num_opps_support_unit_null],
	[num_opps_support_unit_tricore],
	[num_opps_support_unit_enterprise],
	[num_opps_support_unit_smb],
	[num_opps_support_unit_email_apps],
	[num_opps_ticket_type_null],
	[num_opps_ticket_type_upgrade],
	[num_opps_typex_dedicated_private_cloud],
	[num_opps_typex_revenue_ticket],
	[num_opps_typex_mail_contract_signup],
	[num_opps_typex_rackspace_cloud],
	[num_opps_typex_tricore],
	[num_opps_typex_aws],
	[num_opps_what_did_we_do_well_null],
	[num_opps_what_did_we_do_well_solution_fit],
	[num_opps_why_did_we_lose_null],
	[num_opps_why_did_we_lose_no_response],
	[num_opps_why_did_we_lose_unresponsive],	
	[num_opps_why_did_we_lose_project_abandoned],
	[num_opps_why_did_we_lose_existing_opp_closed_via_ticket],
	[num_opps_why_did_we_lose_price],

	----Account configuration----
	account_sla_type,
	[website_tld],
	[num_distinct_account_sla_type],
	[num_distinct_account_business_type],
	[num_distinct_account_team_name],
	[num_distinct_account_manager],
	[num_distinct_account_bdc],
	[num_distinct_account_primary_contact],
	[num_distinct_account_region],
	[num_distinct_account_billing_street],
	[num_distinct_account_billing_city],
	[num_distinct_account_billing_state],
	[num_distinct_account_billing_postal_code],
	[num_distinct_account_billing_country],
	[num_distinct_account_geographic_location],

	----NPS----
	last_survey_responseflag = case when last_survey_score = '' then 0 else 1 end,
	last_survey_score = cast(last_survey_score as int),
	last_survey_rating as last_survey_rating_original,
	second_last_survey_responseflag = case when second_last_survey_score = '' then 0 else 1 end,
	second_last_survey_score = cast(second_last_survey_score as int),
	second_last_survey_rating as second_last_survey_rating_original,
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

	----Salesforce account----
	shippingcountrycode,
	[ownership_private] = case 
		when lower([ownership]) = 'private' then 1
		else 0 end,
	[ownership_public] = case 
		when lower([ownership]) = 'public' then 1
		else 0 end,
	[ownership_unknown] = case 
		when [ownership] is null then 1
		else 0 end,
	[ownership_subsidiary] = case 
		when lower([ownership]) = 'subsidiary' then 1
		else 0 end,
	[ownership_other] = case 
		when lower([ownership]) = 'other' then 1
		when (not lower([ownership]) in ('private','subsidiary','other','public')) and ([ownership] is not null) then 1
		else 0 end,
	[site_branch] = case
		when lower([site]) = 'branch' then 1
		else 0 end,
	[site_headquarters] = case
		when lower([site]) = 'headquarters' then 1
		else 0 end,
	[site_single location] = case
		when lower([site]) = 'single location' then 1
		else 0 end,
	company_review_priority,
	number_of_customer_accounts,
	number_of_cloud_accounts--,

into [customerretention].[dbo].[00final_cloud]
from [customerretention].[dbo].[base_cloud] a
	--left join [customerretention].[dbo].[ticket_events_02_cloud] b 
	--on a.account_number = b.account_number and a.time_month_key = b.time_month_key

	left join [customerretention].[dbo].[zz_staging_revenue_01_cloud] h 
	on a.account_number = h.account_number and a.time_month_key = h.time_month_key

	left join [customerretention].[dbo].[zz_staging_revenue_02_cloud] i 
	on a.account_number = i.account_number and a.time_month_key = i.time_month_key

	left join [customerretention].[dbo].[zz_staging_opportunity_data_01_cloud] v 
	on a.account_number = v.account_number and a.time_month_key = v.time_month_key

	left join [customerretention].[dbo].[zz_staging_opportunity_data_02_cloud] w 
	on a.account_number = w.account_number and a.time_month_key = w.time_month_key

	left join [customerretention].[dbo].[zz_staging_account_data_01_cloud] x 
	on a.account_number = x.account_number and a.time_month_key = x.time_month_key

	left join [customerretention].[dbo].[zz_staging_account_data_02_cloud] y 
	on a.account_number = y.account_number and a.time_month_key = y.time_month_key

	left join [customerretention].[dbo].[zz_staging_nps_04_cloud] z 
	on a.account_number = z.account_number and a.time_month_key = z.time_month_key

	left join [customerretention].[dbo].[zz_staging_salesforce_account_cloud] aa 
	on a.account_number = aa.account_number and a.time_month_key = aa.time_month_key

	/* nr 04/04/2019_feature addition _first churn tmk flag 1/0 */
	left join #first_churn_flag cf on cf.account_number = a.account_number and a.time_month_key = cf.time_month_key
;	

