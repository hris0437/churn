# -*- coding: utf-8 -*-
"""
Created on Thu Nov  8 11:45:35 2018

Fits the saved, fitted dedicated model to the live/ current month's data in
AMT_FINAL_AMT_dedicated_live_allvariables table

Outputs scores and account information to a SQL table, as well as two
spreadsheets (one for internal data science use with probabilities, other
spreadsheet for business units to use)

@author: andr3227
"""
import joblib
import pandas as pd

import churn_common as c #P2G_common.py file is in same directory as this one

print("Running query to gather live account data")
churn_dedi_live = c.data_etl(c.dedi_live_model_sql)
nulls = churn_dedi_live.isnull().sum()

#Export data to CSV for SHAP value analysis
#FUTURE STATE: WHEN WORKER CONTAINERS CAN PERFORM SQL DIRECTLY, THIS STEP WILL BE DEPRICATED
#churn_dedi_live.to_csv('C:/awelsh/python scripts/churn prediction/dedicated/churn_dedi_model_live.csv', encoding='ISO-8859-1')

#split the cleaned data from SQL into low, mid, high revenue band dataframes on values in past_6mo_baseline column
churn_dedi_live_low, churn_dedi_live_mid, churn_dedi_live_hi = c.split_df_revenue_segments(churn_dedi_live, 'average_invoiced_last_12_months')

#predictors = list(churn_dedi_live.columns[~churn_dedi_live.columns.isin(c.dedi_model_fit_exclude_col)]) #leaves only a list of the features used in the model
predictors_low = c.dedi_model_fit_include_col_low
predictors_mid = c.dedi_model_fit_include_col_mid
predictors_hi = c.dedi_model_fit_include_col_hi

#create copy of the X and Y columns
x_live_low = churn_dedi_live_low[predictors_low] #uses list of columns excluding target (predictors1) to select columns for x_live_low
y1_low = churn_dedi_live_low['target']

x_live_mid = churn_dedi_live_mid[predictors_mid]
y1_mid = churn_dedi_live_mid['target']

x_live_hi = churn_dedi_live_hi[predictors_hi]
y1_hi = churn_dedi_live_hi['target']

print("Loading fitted model")
xgb_final_low = joblib.load('C:/awelsh/python scripts/churn prediction - consolidated/dedicated/churn_dedi_model_fit_low.sav')
xgb_final_mid = joblib.load('C:/awelsh/python scripts/churn prediction - consolidated/dedicated/churn_dedi_model_fit_mid.sav')
xgb_final_hi  = joblib.load('C:/awelsh/python scripts/churn prediction - consolidated/dedicated/churn_dedi_model_fit_hi.sav')
print("Fitted model loaded from saved files on disk")

#%%
#==============================================================================================
#==============================================================================================
#fit final model to live data
#==============================================================================================
#==============================================================================================

print("Passing live data through fitted model")
y1_live_pred_low = xgb_final_low.predict(x_live_low)
y1_live_pred_prob_low = pd.DataFrame(xgb_final_low.predict_proba(x_live_low))
y1_live_pred_mid = xgb_final_mid.predict(x_live_mid)
y1_live_pred_prob_mid = pd.DataFrame(xgb_final_mid.predict_proba(x_live_mid))
y1_live_pred_hi = xgb_final_hi.predict(x_live_hi)
y1_live_pred_prob_hi = pd.DataFrame(xgb_final_hi.predict_proba(x_live_hi))
print("Live data predictions complete")

#to output probabilities with time_month_key and account, copy only key and revenue columns:
x_live_low_list = churn_dedi_live_low[['account_number','time_month_key','average_invoiced_last_12_months'
        ,'number_of_customer_accounts'
        ,'pct_of_devices_with_contract_status_eq_in_contract'
        ,'pct_opportunities_won_last_3_months'
        ,'avg_invoiced_in_last_3_months_vs_prior_3_months'
        ,'avg_invoiced_in_last_6_months_vs_prior_6_months'
        ]]
x_live_low_list.reset_index(inplace=True)

x_live_mid_list = churn_dedi_live_mid[['account_number','time_month_key','average_invoiced_last_12_months'
        ,'number_of_customer_accounts'
        ,'pct_of_devices_with_contract_status_eq_in_contract'
        ,'pct_opportunities_won_last_3_months'
        ,'avg_invoiced_in_last_3_months_vs_prior_3_months'
        ,'avg_invoiced_in_last_6_months_vs_prior_6_months'
       ]]
x_live_mid_list.reset_index(inplace=True)

x_live_hi_list = churn_dedi_live_hi[['account_number','time_month_key','average_invoiced_last_12_months'
        ,'number_of_customer_accounts'
        ,'pct_of_devices_with_contract_status_eq_in_contract'
        ,'pct_opportunities_won_last_3_months'
        ,'avg_invoiced_in_last_3_months_vs_prior_3_months'
        ,'avg_invoiced_in_last_6_months_vs_prior_6_months'
        ]]
x_live_hi_list.reset_index(inplace=True)

#concatenate probabilities onto dataframe copies created above
hi_live_final = pd.concat([x_live_hi_list,y1_live_pred_prob_hi], axis=1)
mid_live_final = pd.concat([x_live_mid_list,y1_live_pred_prob_mid], axis=1)
low_live_final = pd.concat([x_live_low_list,y1_live_pred_prob_low], axis=1)

#concatenate all revenue bands into one final list
#live_final_list = pd.concat([low_live_final,mid_live_final,hi_live_final], axis=0) #original code
live_final_list = pd.concat([low_live_final,mid_live_final,hi_live_final], axis=0, sort=True) #pandas change

#recode revenue column into low-med-high groups
live_final_list.loc[live_final_list['average_invoiced_last_12_months']>=25000, 'account value'] = '1. High'
live_final_list.loc[(live_final_list['average_invoiced_last_12_months']<25000) & (live_final_list['average_invoiced_last_12_months']>=5000), 'account value'] = '2. Med'
live_final_list.loc[live_final_list['average_invoiced_last_12_months']<5000, 'account value'] = '3. Low'

#convert probabilities to low-med-high probability groups
live_final_list.loc[live_final_list[1]>=.9, 'churn risk group'] = '1. High'
live_final_list.loc[(live_final_list[1]<.9) & (live_final_list[1]>=.5), 'churn risk group'] = '2. Med'
live_final_list.loc[live_final_list[1]<.5, 'churn risk group'] = '3. Low'

#convert account number to float for export to excel
live_final_list.account_number = live_final_list.account_number.astype(float)

#extract time_month_key for live run for inclusion in the file export string. Assumes one TMK in data
model_run_month = churn_dedi_live.time_month_key.loc[0].astype(str)

#%%
#======================================================================================================
#======================================================================================================
#======================================================================================================
#PUBLISH DATA IN DESIRED OUTPUT FORMAT
#======================================================================================================
#======================================================================================================
#======================================================================================================

print("Collecting account and AM names")
#grab account names and AM names
live_final_supp_data  = c.data_sql(c.dedi_live_model_supp_data_sql)

print("Collecting churn reason codes")
#grab churn reason codes for future 6 months
live_final_supp_data2 = c.data_sql(c.read_file('sql/dedi_live_model_supp_data_churn_codes.sql'))

#convert account number from supplemental data form object to float
live_final_supp_data.account_number = live_final_supp_data.account_number.astype(float)
live_final_supp_data2.account_number = live_final_supp_data2.account_number.astype(float)

print("Names Collected, exporting to excel")

#merge supplemental data to model output
live_final0 = pd.merge(live_final_list, live_final_supp_data, how='left', on=['account_number','time_month_key'])
live_final0.drop(['index'], axis=1, inplace=True)
live_final1 = pd.merge(live_final0, live_final_supp_data2, how='left', on='account_number')
 
#=============================
#NEW DISPLAY LOGIC 26 MAR 2020
#=============================
#filter out cases where total invoice last 3/6 vs prior 3/6 is -1, which is indication of net_revenue_detail
#errors, as the invoice amounts are null for one or more months
# recalculate 'total last x vs prior x' to percentage
# create row filter known_churn for dashboard display default
import pandasql as ps

rowfiltersql = """select *
    ,avg_invoiced_in_last_3_months_vs_prior_3_months - 1.0 
        as 'pct change in invoice in last 3 months vs prior 3 months'
    ,avg_invoiced_in_last_6_months_vs_prior_6_months - 1.0
        as 'pct change in invoice in last 6 months vs prior 6 months'
    ,case when (
    (avg_invoiced_in_last_3_months_vs_prior_3_months - 1) <= -0.3 
    or (avg_invoiced_in_last_6_months_vs_prior_6_months - 1) <= -0.3
    or future_6m_churn>0
    or average_invoiced_last_12_months < 1000
    or 'churn risk group' = '3. Low'
    or stage3_plus_cnt > 0
    or account_status = 'closed'
    ) 
    then 1 else 0 end as known_churn
    from live_final1
    where avg_invoiced_in_last_3_months_vs_prior_3_months>-1
    and avg_invoiced_in_last_6_months_vs_prior_6_months>-1
    and 'account team name' <> 'RAS-TCS'
     """

#"locals" definition here points to locally stored dataframes in memory, acting as SQL tables
live_final = ps.sqldf(rowfiltersql, locals())

#export to excel
live_final.to_excel('C:/awelsh/python scripts/churn prediction - consolidated/dedicated/final_list_probabilities_dedicated_live'+ model_run_month +'- with probabilities.xlsx', index=False)
live_final[['account_number'
            ,'average_invoiced_last_12_months'
            ,'account value'
            ,'churn risk group'
            , 'account_name'
            ,'account team name'
            ,'customer success manager'
            ,'region'
            ,'business_unit'
            ,'segment'
            ,'company annual revenue'
            ,'number of csms'
            ,'number of bdcs'
            ,'latest nps response in last 12 mos'
            ,'number_of_customer_accounts'
            ,'pct_of_devices_with_contract_status_eq_in_contract'
            ,'pct devices 90d'
            ,'num opportunities last 3 months'
            ,'pct_opportunities_won_last_3_months'
            ,'num opps in pipeline for next 12 mos'
    		,'renewal_opportunities_closed_historical_3months'
    		,'renewal_opportunities_won_historical_3months'
    		,'pct_renewal_opportunities_won_historical_3months'
    		,'renewal_opportunities_in_pipeline'
            ,'average active device tenure months'
            ,'pct change in invoice in last 3 months vs prior 3 months'
            ,'pct change in invoice in last 6 months vs prior 6 months'
            ,'account tenure in months'
    		,'does_account_have_mpc'
    		,'does_account_have_ospc'
    		,'future_6m_churn'
    		,'historic_6m_churn'
            ,'reason_code'
            ,'recent_contract_renewed_tmk'
            ,'recent_contract_renewal_discount_amt'
            ,'recent_renewal_discount_perc'
            ,'renewal_discount_amt_6_mos'
            ,'renewal_discount_perc_6_mos'
            ,'renewal_discount_amt_3_mos'
            ,'renewal_discount_perc_3_mos'
            ,'known_churn'
            ]].to_excel(
            'C:/awelsh/python scripts/churn prediction - consolidated/dedicated/final_list_probabilities_dedicated_live'+ model_run_month +'.xlsx', index=False)
print("Final file output complete")

#
##%%
#live_final.to_csv(path_or_buf="Y:/churn_dedi_probabilities.txt", sep="\t", encoding='utf-8', index=False, header=False)
#print('Text file updated')
#
##erase data in the target table to make sure it's empty
#c.publish_sql(c.dedi_live_model_sql_push1)
#print('Table erased')
#
##This text file is read using the local path on statcruncher / server upon which the SQL server is running
##the local text file was pushed to CSV in the above step.
##if you are specifying a file not on the SQL server itself, you will get "file not found" errors returned from 
##the SQL server
#c.publish_sql(c.dedi_live_model_sql_push2)
#print('Table updated')
#
##read in the test table to verify the code worked (uncomment next line for debugging/test purposes)
#sql_test1 = c.data_sql('SELECT * FROM [CustomerRetention].[dbo].[Dedicated_Scores] with (nolock);')
#
