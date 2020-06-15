SELECT
[account_number]
,[time_month_key]
,[average_invoiced_last_12_months]
,[target] = case when [binary target]='flag_1' then 1 else 0 end
,[pct of revenue with contract_status = in contract]
,[defection_flag]
,[total number of tickets]
,[avg days to close ticket]
,[avg days to close ticket - category application]
,[avg days to close ticket - category customer initiated]
,[avg days to close ticket - category hardware]
,[avg minutes to first customer comment]
,[avg minutes to first racker comment]
,[avg minutes to first racker comment - category monitoring alerts]
,[avg minutes to started in progress]
,[avg minutes to started in progress - severity urgent]
,[avg minutes to started in progress - category monitoring alerts]
,[num tickets severity - emergency]
,[num tickets ticket category - monitoring alerts]
,[num tickets ticket category - monitoring]
,[num tickets ticket category - null]
,[num tickets ticket category - application]
,[num tickets ticket category - account management]
,[num tickets ticket category - customer initiated]
,[num tickets ticket category - operating services]
,[number of tickets confirmed solved]
,[number of tickets solved]
,[rate of tickets confirmed solved]
,[rate of tickets solved]
,[number of devices last month]
,[number of device_status = online/complete]
,[number of device_status = computer no longer active]
,[Number of Device_Status = Support Maintenance]
,[number of other device_status]
,[number_of_accounts_original]
,[number_of_customer_accounts]
,[number_of_cloud_accounts]
,[Pct of Device_Status = Support Maintenance]
,[pct of other device_status]
,[average active device tenure months]
,[longest active device tenure months]
,[shortest active device tenure months]
,[pct of devices with contract_status = out of contract - mtm]
,[pct of devices with contract_status = no contract status]
,[pct of devices with contract_status = in contract]
,[pct of devices with contract_status = in contract - risk of lapse 90 days]
,[pct of revenue with contract_status = out of contract - mtm]
,[pct of revenue with contract_status = no contract status]
,[pct of revenue with contract_status = in contract - risk of lapse 90 days]
,[number of devices os - firewall]
,[number of devices os - load-balancer]
,[number of devices os - switch]
,[number of devices os name - linux]
,[3 mth pct change number of device_status = computer no longer active]
,[3 mth pct change longest active device tenure months]
,[3 mth pct change number of devices active status - active]
,[3 mth pct change number of devices online status - online]
,[avg monthly number of - sku_description =  privatenet - 6 mth to 1 mth]
,[avg monthly number of - sku_description =  weekly full + daily incremental - 6 mth to 1 mth]
,[avg monthly number of - sku_description =  raid 1 - 6 mth to 1 mth]
,[avg monthly number of - sku_name =  included bandwidth - 6 mth to 1 mth]
,[avg monthly number of - sku_name =  hard drive size - 6 mth to 1 mth]
,[avg monthly number of - sku_name =  advanced_networking - 6 mth to 1 mth]
,[avg monthly number of - sku_description =  included bandwidth - 6 mth to 1 mth]
,[avg monthly number of - sku_name =  high availability - 6 mth to 1 mth]
,[avg monthly number of - sku_name =  ip allocation - 6 mth to 1 mth]
,[avg monthly number of - sku_name =  load-balancer required - 6 mth to 1 mth]
,[total invoiced in last 6 months]
,[avg per line item invoiced in last 6 months_original]
,[avg mthly num of invoiced items in last 6 months]
,[avg credit memo in last 6 months]
,[total invoiced in last 6 months vs prior 6 months]
,[avg mthly num of invoiced items in last 6 months vs prior 6 months]
,[total invoiced in last 3 months vs prior 3 months]
,[avg mthly num of invoiced items in last 3 months vs prior 3 months]
,[Has_Cloud]	
,[Per Unit Price - Next Gen Servers]
,[Per Unit Price - AWS]
,[Per Unit Price - Cloud Block Storage]
,[Per Unit Price - Cloud Files]
,[Per Unit Price - Azure]
,[per unit price - managed exchange]
,[per unit price - rackspace email]
,[per unit price - server]
,[per unit price - virtual hosting]
,[per unit price - virtualization]
,[per unit price - dedicated san]
,[per unit price - san]
,[per unit price - firewall]
,[per unit price - load balancer]
,[per unit price - rpc core]
,[per unit price - switch]
,[per unit price - managed storage]
,[per unit price - threat manager]
,[per unit price - bandwidth overages]
,[How Many Units - Next Gen Servers]
,[How Many Units - AWS]
,[How Many Units - Cloud Block Storage]
,[How Many Units - Cloud Files]
,[How Many Units - Azure]
,[how many units - managed exchange]
,[how many units - rackspace email]
,[how many units - server]
,[how many units - virtual hosting]
,[how many units - virtualization]
,[how many units - dedicated san]
,[how many units - san]
,[how many units - firewall]
,[how many units - load balancer]
,[how many units - rpc core]
,[how many units - switch]
,[how many units - managed storage]
,[how many units - threat manager]
,[how many units - bandwidth overages]
,[Pct of Invoice - Next Gen Servers]
,[Pct of Invoice - AWS]
,[Pct of Invoice - Cloud Block Storage]
,[Pct of Invoice - Cloud Files]
,[Pct of Invoice - Azure]
,[pct of invoice - managed exchange]
,[pct of invoice - rackspace email]
,[pct of invoice - server]
,[pct of invoice - virtual hosting]
,[pct of invoice - virtualization]
,[pct of invoice - dedicated san]
,[pct of invoice - san]
,[pct of invoice - firewall]
,[pct of invoice - load balancer]
,[pct of invoice - rpc core]
,[pct of invoice - switch]
,[pct of invoice - managed storage]
,[pct of invoice - threat manager]
,[pct of invoice - bandwidth overages]
,[Num Opportunities Last 6 Months]
,[Num Opportunities Won Last 6 Months]
,[Num Opportunities Lost Last 6 Months]
,[Pct Opportunities Won Last 6 Months]
,[Pct Opportunities Lost Last 6 Months]
,[Num Opportunities Last 3 Months]
,[Num Opportunities Won Last 3 Months]
,[Num Opportunities Lost Last 3 Months]
,[Pct Opportunities Won Last 3 Months]
,[Pct Opportunities Lost Last 3 Months]
,[Total Value of Opportunities Last 6 Months]
,[Total Value of Opportunities Last 3 Months]
,[Average Value of Opportunities Last 6 Months]
,[Average Value of Opportunities Last 3 Months]
,[Num Opportunities]
,[Num Opps ALLOW_QUOTE - Not Allowed]
,[Num Opps ALLOW_QUOTE - Allowed]
,[Num Opps BUCKET_INFLUENCE - NULL]
,[Num Opps BUCKET_INFLUENCE - Marketing]
,[Num Opps BUCKET_SOURCE - Sales]
,[Num Opps BUCKET_SOURCE - NULL]
,[Num Opps BUCKET_SOURCE - Marketing]
,[Num Opps CATEGORY - Upgrade]
,[Num Opps CATEGORY - New]
,[Num Opps CATEGORY - New Footprint]
,[Num Opps CATEGORY - Cloud Net Revenue]
,[Num Opps CATEGORY - New Logo]
,[Num Opps CATEGORY - Migration]
,[Num Opps CATEGORY - NULL]
,[Num Opps COMMISSION_ROLE - NULL]
,[Num Opps COMMISSION_ROLE - Pay Commissions]
,[Num Opps COMPETITORS - NULL]
,[Num Opps COMPETITORS - In-house]
,[Num Opps COMPETITORS - Other]
,[Num Opps CONTRACT_LENGTH - NULL]
,[Num Opps CONTRACT_LENGTH - 12]
,[Num Opps CONTRACT_LENGTH - 1]
 ,[Num Opps CONTRACT_LENGTH - 0]
 ,[Num Opps CONTRACT_LENGTH - 24]
 ,[Num Opps CVP_VERIFIED - FALSE]
 ,[Num Opps CVP_VERIFIED - TRUE]
 ,[Num Opps DATA_QUALITY_DESCRIPTION - Missing: Lead Source, Next Ste]
 ,[Num Opps DATA_QUALITY_DESCRIPTION - Missing: Amount, Lead Source]
 ,[Num Opps DATA_QUALITY_DESCRIPTION - Missing: Amount, Next Steps]
 ,[Num Opps DATA_QUALITY_DESCRIPTION - Missing: Next Steps]
 ,[Num Opps DATA_QUALITY_DESCRIPTION - Missing: Lead Source]
 ,[Num Opps DATA_QUALITY_DESCRIPTION - All Opportunity Details Captur]
 ,[Num Opps DATA_QUALITY_DESCRIPTION - Missing: Amount]
 ,[Num Opps DATA_QUALITY_SCORE - 60]
 ,[Num Opps DATA_QUALITY_SCORE - 40]
 ,[Num Opps DATA_QUALITY_SCORE - 80]
 ,[Num Opps DATA_QUALITY_SCORE - 100]
 ,[Num Opps ECONNECT_RECEIVED - FALSE]
 ,[Num Opps ECONNECT_RECEIVED - TRUE]
 ,[Num Opps FOCUS_AREA - NULL]
 ,[Num Opps FOCUS_AREA - Dedicated]
 ,[Num Opps FOCUS_AREA - Cloud Office]
 ,[Num Opps FOCUS_AREA - OpenStack Public]
 ,[Num Opps FOCUS_AREA - TriCore]
 ,[Num Opps FOCUS_AREA - Amazon]
 ,[Num Opps FORECASTCATEGORY - Closed]
 ,[Num Opps FORECASTCATEGORY - Omitted]
 ,[Num Opps FORECASTCATEGORY - Pipeline]
 ,[Num Opps FORECASTCATEGORYNAME - Closed]
 ,[Num Opps FORECASTCATEGORYNAME - Omitted]
 ,[Num Opps FORECASTCATEGORYNAME - Pipeline]
 ,[Num Opps ISWON - TRUE]
 ,[Num Opps ISWON - FALSE]
 ,[Num Opps LEADSOURCE - NULL]
 ,[Num Opps LEADSOURCE - Chat]
 ,[Num Opps LEADSOURCE - Call In]
 ,[Num Opps LEADSOURCE - Partner Network]
 ,[Num Opps LEADSOURCE - Outbound]
 ,[Num Opps LEADSOURCE - Site Submission]
 ,[Num Opps LEADSOURCE - Unknown]
 ,[Num Opps LIVE_CALL - FALSE]
 ,[Num Opps MARKET_SOURCE - NULL]
 ,[Num Opps MARKET_SOURCE - No]
 ,[Num Opps MARKET_SOURCE - Yes]
 ,[Num Opps NUTCASE_DEAL_PROBABILITY - 0]
 ,[Num Opps ON_DEMAND_RECONCILED - FALSE]
 ,[Num Opps ON_DEMAND_RECONCILED - TRUE]
 ,[Num Opps PAIN_POINT - NULL]
 ,[Num Opps PAIN_POINT - Other]
 ,[Num Opps PAIN_POINT - ServiceNow]
 ,[Num Opps PROBABILITY - 100]
 ,[Num Opps PROBABILITY - 0]
 ,[Num Opps PROBABILITY - 15]
 ,[Num Opps REQUESTED_PRODUCTS - NULL]
 ,[Num Opps REQUESTED_PRODUCTS - Hosting Only]
 ,[Num Opps SUPPORT_UNIT - NULL]
 ,[Num Opps SUPPORT_UNIT - TriCore]
 ,[Num Opps SUPPORT_UNIT - Enterprise]
 ,[Num Opps SUPPORT_UNIT - SMB]
 ,[Num Opps SUPPORT_UNIT - Email & Apps]
 ,[Num Opps TICKET_TYPE - NULL]
 ,[Num Opps TICKET_TYPE - Upgrade]
 ,[Num Opps TYPEX - Dedicated/Private Cloud]
 ,[Num Opps TYPEX - Revenue Ticket]
 ,[Num Opps TYPEX - Mail Contract Signup]
 ,[Num Opps TYPEX - Rackspace Cloud]
 ,[Num Opps TYPEX - TriCore]
 ,[Num Opps TYPEX - AWS]
 ,[Num Opps WHAT_DID_WE_DO_WELL - NULL]
 ,[Num Opps WHAT_DID_WE_DO_WELL - Solution Fit]
 ,[Num Opps WHY_DID_WE_LOSE - NULL]
 ,[Num Opps WHY_DID_WE_LOSE - No Response]
 ,[Num Opps WHY_DID_WE_LOSE - Unresponsive]
 ,[Num Opps WHY_DID_WE_LOSE - Project Abandoned]
 ,[Num Opps WHY_DID_WE_LOSE - Existing Opp/Closed via Ticket]
 ,[Num Opps WHY_DID_WE_LOSE - Price]
 ,CASE WHEN Account_SLA_Type IS NULL  then 0
	   WHEN Account_SLA_Type ='None'  then 0
	   WHEN Account_SLA_Type ='Standard' then 1
	   WHEN Account_SLA_Type ='Gold' then 2
	   WHEN Account_SLA_Type ='Platinum' then 3
	   end as Account_SLA_Type_num
,[acc_tenure_in_months]
,[num distinct account_sla_type]
,[num distinct account_business_type]
,[num distinct account_team_name]
,[num distinct account_manager]
,[num distinct account_bdc]
,[num distinct account_primary_contact]
,[num distinct account_region]
,[num distinct account_billing_street]
,[num distinct account_billing_city]
,[num distinct account_billing_state]
,[num distinct account_billing_postal_code]
,[num distinct account_billing_country]
,[num distinct account_geographic_location]
,[last_survey_score]
,[second_last_survey_score]
 ,[ANNUALREVENUE_original]
 ,[NUMBEROFEMPLOYEES_original]
 ,[Company_Age_original]
FROM [CustomerRetention].[dbo].[AMT_FINAL_AMT_dedicated_live_allvariables] with (nolock)
order by account_number, time_month_key;
