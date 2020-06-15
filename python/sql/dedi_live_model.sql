select
a.account_number
,a.time_month_key
,a.average_invoiced_last_12_months
,churn_target as target
,[3_mth_pct_change_number_of_devices_active_status_active]
,[3_mth_pct_change_number_of_devices_online_status_online]
,case when lower(account_has_sku_name_eq_monitoring_last_6mo)='n' then 1 else 0 end as account_has_sku_name_eq_monitoring_last_6mo
,avg_invoiced_in_last_3_months_vs_prior_3_months
,avg_invoiced_in_last_6_months_vs_prior_6_months
,avg_unit_price_managed_exchange
,avg_unit_price_rackspace_email
,avg_unit_price_rpc_core
,change_in_survey_score
,case when lower(does_account_have_cloud_files)='n' then 1 else 0 end as does_account_have_cloud_files
,case when lower(does_account_have_managed_storage)='n' then 1 else 0 end as does_account_have_managed_storage
,how_many_units_next_gen_servers
,how_many_units_rackspace_email
,how_many_units_rpc_core
,last_survey_score
,num_distinct_account_billing_postal_code
,num_opportunities_last_3_months
,number_of_customer_accounts
,number_of_devices_os_firewall
,pct_of_device_status_eq_online_complete
,pct_of_device_status_eq_support_maintenance
,pct_of_devices_with_contract_status_eq_in_contract
,pct_of_invoice_bandwidth_overages
,pct_of_invoice_managed_exchange
,pct_of_invoice_managed_storage
,pct_of_invoice_rpc_core
,pct_opportunities_won_last_3_months
,rating_detractor_to_detractor
,rating_detractor_to_passive
,rating_passive_to_promoter
,rating_promoter_to_passive
,ratio_num_of_invoiced_items_in_last_3_months_vs_prior_3_months
,ratio_num_of_invoiced_items_in_last_6_months_vs_prior_6_months
,shortest_active_device_tenure_months
,total_invoiced_in_last_6_months
from customerretention.dbo.[00final_dedicated] a with (nolock)
where time_month_key=(select max(time_month_key) from customerretention.dbo.[00final_dedicated])
order by cast(a.account_number as int), time_month_key;
