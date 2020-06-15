select
account_number
,time_month_key
,total_invoiced_in_month
,average_invoiced_last_12_months
,churn_target as target
,[3_mth_pct_change_longest_active_device_tenure_months]
,[3_mth_pct_change_number_of_device_status_eq_computer_no_longer_active]
,[3_mth_pct_change_number_of_devices_active_status_active]
,[3_mth_pct_change_number_of_devices_online_status_online]
,acc_tenure_in_months
--,account_business_type
,case when lower(account_has_sku_name_eq_dell_servers_last_6mo)='n' then 1 else 0 end as account_has_sku_name_eq_dell_servers_last_6mo
,case when lower(account_has_sku_name_eq_hard_drive_last_6mo)='n' then 1 else 0 end as account_has_sku_name_eq_hard_drive_last_6mo
,case when lower(account_has_sku_name_eq_included_bandwidth_last_6mo)='n' then 1 else 0 end as account_has_sku_name_eq_included_bandwidth_last_6mo
,case when lower(account_has_sku_name_eq_ip_allocation_last_6mo)='n' then 1 else 0 end as account_has_sku_name_eq_ip_allocation_last_6mo
,case when lower(account_has_sku_name_eq_monitoring_last_6mo)='n' then 1 else 0 end as account_has_sku_name_eq_monitoring_last_6mo
,case when lower(account_has_sku_name_eq_support_last_6mo)='n' then 1 else 0 end as account_has_sku_name_eq_support_last_6mo
,case when account_sla_type is null  then 0
   when account_sla_type ='none'  then 0
   when account_sla_type ='standard' then 1
   when account_sla_type ='gold' then 2
   when account_sla_type ='platinum' then 3
   end as account_sla_type_num
,annualrevenue_original
,average_active_device_tenure_months
,average_value_of_opportunities_last_3_months
,average_value_of_opportunities_last_6_months
,avg_credit_memo_in_last_6_months
,avg_invoiced_in_last_3_months_vs_prior_3_months
,avg_invoiced_in_last_6_months_vs_prior_6_months
,avg_monthly_number_of_sku_description_eq_included_bandwidth_last_6mo
,avg_monthly_number_of_sku_description_eq_privatenet_last_6mo
,avg_monthly_number_of_sku_description_eq_raid_1_last_6mo
,avg_monthly_number_of_sku_description_eq_weekly_full_daily_incremental_last_6mo
,avg_monthly_number_of_sku_name_eq_advanced_networking_last_6mo
,avg_monthly_number_of_sku_name_eq_hard_drive_size_last_6mo
,avg_monthly_number_of_sku_name_eq_high_availability_last_6mo
,avg_monthly_number_of_sku_name_eq_included_bandwidth_last_6mo
,avg_monthly_number_of_sku_name_eq_ip_allocation_last_6mo
,avg_monthly_number_of_sku_name_eq_load_balancer_required_last_6mo
,avg_mthly_num_of_invoiced_items_in_last_6_months
,avg_per_line_item_invoiced_in_last_6_months
,avg_unit_price_aws
,avg_unit_price_azure
,avg_unit_price_bandwidth_overages
,avg_unit_price_cloud_block_storage
,avg_unit_price_cloud_files
,avg_unit_price_dedicated_san
,avg_unit_price_firewall
,avg_unit_price_load_balancer
,avg_unit_price_managed_exchange
,avg_unit_price_managed_storage
,avg_unit_price_next_gen_servers
,avg_unit_price_rackspace_email
,avg_unit_price_rpc_core
,avg_unit_price_san
,avg_unit_price_server
,avg_unit_price_switch
,avg_unit_price_threat_manager
,avg_unit_price_virtual_hosting
,avg_unit_price_virtualization
,change_in_survey_score
,company_age_original
,company_priority_original
,company_review_priority
,case when lower(does_account_have_aws)='n' then 1 else 0 end as does_account_have_aws
,case when lower(does_account_have_azure)='n' then 1 else 0 end as does_account_have_azure
,case when lower(does_account_have_bandwidth_overages)='n' then 1 else 0 end as does_account_have_bandwidth_overages
,case when lower(does_account_have_cloud_block_storage)='n' then 1 else 0 end as does_account_have_cloud_block_storage
,case when lower(does_account_have_cloud_files)='n' then 1 else 0 end as does_account_have_cloud_files
,case when lower(does_account_have_dedicated_san)='n' then 1 else 0 end as does_account_have_dedicated_san
,case when lower(does_account_have_firewall)='n' then 1 else 0 end as does_account_have_firewall
,case when lower(does_account_have_load_balancer)='n' then 1 else 0 end as does_account_have_load_balancer
,case when lower(does_account_have_managed_exchange)='n' then 1 else 0 end as does_account_have_managed_exchange
,case when lower(does_account_have_managed_storage)='n' then 1 else 0 end as does_account_have_managed_storage
,case when lower(does_account_have_next_gen_servers)='n' then 1 else 0 end as does_account_have_next_gen_servers
,case when lower(does_account_have_rackspace_email)='n' then 1 else 0 end as does_account_have_rackspace_email
,case when lower(does_account_have_rpc_core)='n' then 1 else 0 end as does_account_have_rpc_core
,case when lower(does_account_have_san)='n' then 1 else 0 end as does_account_have_san
,case when lower(does_account_have_server)='n' then 1 else 0 end as does_account_have_server
,case when lower(does_account_have_switch)='n' then 1 else 0 end as does_account_have_switch
,case when lower(does_account_have_threat_manager)='n' then 1 else 0 end as does_account_have_threat_manager
,case when lower(does_account_have_virtual_hosting)='n' then 1 else 0 end as does_account_have_virtual_hosting
,case when lower(does_account_have_virtualization)='n' then 1 else 0 end as does_account_have_virtualization
,has_cloud
,how_many_units_aws
,how_many_units_azure
,how_many_units_bandwidth_overages
,how_many_units_cloud_block_storage
,how_many_units_cloud_files
,how_many_units_dedicated_san
,how_many_units_firewall
,how_many_units_load_balancer
,how_many_units_managed_exchange
,how_many_units_managed_storage
,how_many_units_next_gen_servers
,how_many_units_rackspace_email
,how_many_units_rpc_core
,how_many_units_san
,how_many_units_server
,how_many_units_switch
,how_many_units_threat_manager
,how_many_units_virtual_hosting
,how_many_units_virtualization
--,industry
,last_survey_responseflag
,last_survey_score
,lead_tech_flag
,longest_active_device_tenure_months
--,naicsdesc
,num_distinct_account_bdc
,num_distinct_account_billing_city
,num_distinct_account_billing_country
,num_distinct_account_billing_postal_code
,num_distinct_account_billing_state
,num_distinct_account_business_type
,num_distinct_account_geographic_location
,num_distinct_account_manager
,num_distinct_account_primary_contact
,num_distinct_account_region
,num_distinct_account_sla_type
,num_distinct_account_team_name
,num_opportunities_last_12_months
,num_opportunities_last_3_months
,num_opportunities_last_6_months
,num_opportunities_lost_last_3_months
,num_opportunities_lost_last_6_months
,num_opportunities_won_last_3_months
,num_opportunities_won_last_6_months
,num_opps_category_migration
,num_opps_support_unit_smb
,num_opps_typex_aws
,num_opps_typex_dedicated_private_cloud
,num_opps_typex_revenue_ticket
,number_of_accounts_original
,number_of_cloud_accounts
,number_of_customer_accounts
,number_of_device_status_eq_computer_no_longer_active
,number_of_device_status_eq_online_complete
,number_of_device_status_eq_support_maintenance
,number_of_devices_last_month
,number_of_devices_os_firewall
,number_of_devices_os_load_balancer
,number_of_devices_os_name_linux
,number_of_devices_os_switch
,number_of_other_device_status
,numberofemployees_original
--,ownership
--,ownership_original
,pct_of_device_status_eq_computer_no_longer_active
,pct_of_device_status_eq_online_complete
,pct_of_device_status_eq_support_maintenance
,pct_of_devices_with_contract_status_eq_in_contract
,pct_of_devices_with_contract_status_eq_in_contract_risk_of_lapse_90_days
,pct_of_devices_with_contract_status_eq_no_contract_status
,pct_of_devices_with_contract_status_eq_out_of_contract_mtm
,pct_of_invoice_aws
,pct_of_invoice_azure
,pct_of_invoice_bandwidth_overages
,pct_of_invoice_cloud_block_storage
,pct_of_invoice_cloud_files
,pct_of_invoice_dedicated_san
,pct_of_invoice_firewall
,pct_of_invoice_load_balancer
,pct_of_invoice_managed_exchange
,pct_of_invoice_managed_storage
,pct_of_invoice_next_gen_servers
,pct_of_invoice_rackspace_email
,pct_of_invoice_rpc_core
,pct_of_invoice_san
,pct_of_invoice_server
,pct_of_invoice_switch
,pct_of_invoice_threat_manager
,pct_of_invoice_virtual_hosting
,pct_of_invoice_virtualization
,pct_of_other_device_status
,pct_of_revenue_with_contract_status_eq_in_contract_risk_of_lapse_90_days
,pct_of_revenue_with_contract_status_eq_no_contract_status
,pct_of_revenue_with_contract_status_eq_out_of_contract_mtm
,pct_opportunities_lost_last_3_months
,pct_opportunities_lost_last_6_months
,pct_opportunities_won_last_3_months
,pct_opportunities_won_last_6_months
,priceincreaseexperimentflag
,rating_detractor_to_detractor
,rating_detractor_to_passive
,rating_detractor_to_promoter
,rating_passive_to_detractor
,rating_passive_to_passive
,rating_passive_to_promoter
,rating_promoter_to_detractor
,rating_promoter_to_passive
,rating_promoter_to_promoter
,ratio_num_of_invoiced_items_in_last_3_months_vs_prior_3_months
,ratio_num_of_invoiced_items_in_last_6_months_vs_prior_6_months
,second_last_survey_responseflag
,second_last_survey_score
--,shippingcountrycode
,shortest_active_device_tenure_months
--,site
,total_invoiced_in_last_6_months
,total_value_of_opportunities_last_3_months
,total_value_of_opportunities_last_6_months
from customerretention.dbo.[00final_dedicated] with (nolock)
where num_months_next_6_months=6 and num_months_last_12_months=12
order by account_number, time_month_key;
