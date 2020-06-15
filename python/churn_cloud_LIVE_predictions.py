# -*- coding: utf-8 -*-
"""
Created on Thu Nov  8 11:45:35 2018

Fits the saved, fitted cloud model to the live/ current month's data in
AMT_FINAL_AMT_cloud_live_allvariables table

Outputs scores and account information to a SQL table, as well as two
spreadsheets (one for internal data science use with probabilities, other
spreadsheet for business units to use)

@author: andr3227
"""
import joblib
import pandas as pd

import churn_common as c #P2G_common.py file is in same directory as this one

print("Running query to gather live account data")
churn_cloud_live = c.data_etl(c.cloud_live_model_sql)
nulls = churn_cloud_live.isnull().sum()

#Export data to CSV for SHAP value analysis
#FUTURE STATE: WHEN WORKER CONTAINERS CAN PERFORM SQL DIRECTLY, THIS STEP WILL BE DEPRICATED
#churn_cloud_live.to_csv('C:/awelsh/python scripts/churn prediction/cloud/churn_cloud_model_live.csv', encoding='ISO-8859-1')

#split the cleaned data from SQL into low, mid, high revenue band dataframes on values in past_6mo_baseline column
churn_cloud_live_low, churn_cloud_live_mid, churn_cloud_live_hi = c.split_df_revenue_segments(churn_cloud_live, 'average_invoiced_last_12_months', low=1000, high=10000)

predictors = list(churn_cloud_live.columns[~churn_cloud_live.columns.isin(c.cloud_model_fit_exclude_col)]) #leaves only a list of the features used in the model

#create copy of the X and Y columns
x_live_low = churn_cloud_live_low[predictors] #uses list of columns excluding target (predictors1) to select columns for x_live_low
y1_low = churn_cloud_live_low['target']

x_live_mid = churn_cloud_live_mid[predictors]
y1_mid = churn_cloud_live_mid['target']

x_live_hi = churn_cloud_live_hi[predictors]
y1_hi = churn_cloud_live_hi['target']

print("Loading fitted model")
xgb_final_low = joblib.load('C:/awelsh/python scripts/churn prediction - consolidated/cloud/churn_cloud_model_fit_low.sav')
xgb_final_mid = joblib.load('C:/awelsh/python scripts/churn prediction - consolidated/cloud/churn_cloud_model_fit_mid.sav')
xgb_final_hi  = joblib.load('C:/awelsh/python scripts/churn prediction - consolidated/cloud/churn_cloud_model_fit_hi.sav')
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
x_live_low_list = churn_cloud_live_low[['account_number','time_month_key','average_invoiced_last_12_months']]
x_live_low_list.reset_index(inplace=True)

x_live_mid_list = churn_cloud_live_mid[['account_number','time_month_key','average_invoiced_last_12_months']]
x_live_mid_list.reset_index(inplace=True)

x_live_hi_list = churn_cloud_live_hi[['account_number','time_month_key','average_invoiced_last_12_months']]
x_live_hi_list.reset_index(inplace=True)

#concatenate probabilities onto dataframe copies created above
hi_live_final = pd.concat([x_live_hi_list,y1_live_pred_prob_hi], axis=1)
mid_live_final = pd.concat([x_live_mid_list,y1_live_pred_prob_mid], axis=1)
low_live_final = pd.concat([x_live_low_list,y1_live_pred_prob_low], axis=1)

#concatenate all revenue bands into one final list
live_final_list = pd.concat([low_live_final,mid_live_final,hi_live_final], axis=0)

#recode revenue column into low-med-high groups
live_final_list.loc[live_final_list['average_invoiced_last_12_months']>=10000, 'account_value'] = '1. High'
live_final_list.loc[(live_final_list['average_invoiced_last_12_months']<10000) & (live_final_list['average_invoiced_last_12_months']>=1000), 'account_value'] = '2. Med'
live_final_list.loc[live_final_list['average_invoiced_last_12_months']<1000, 'account_value'] = '3. Low'

#convert probabilities to low-med-high probability groups
live_final_list.loc[live_final_list[1]>=.9, 'churn risk group'] = '1. High'
live_final_list.loc[(live_final_list[1]<.9) & (live_final_list[1]>=.5), 'churn risk group'] = '2. Med'
live_final_list.loc[live_final_list[1]<.5, 'churn risk group'] = '3. Low'

#convert account number to float for export to excel
live_final_list.account_number = live_final_list.account_number.astype(float)

#extract time_month_key for live run for inclusion in the file export string. Assumes one TMK in data
model_run_month = churn_cloud_live.time_month_key.loc[0].astype(str)

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
live_final_supp_data = c.data_sql(c.cloud_live_model_supp_data_sql)

#convert account number from supplemental data form object to float
live_final_supp_data.account_number = live_final_supp_data.account_number.astype(float)

print("Names Collected, exporting to excel")

#merge supplemental data to model output
live_final = pd.merge(live_final_list, live_final_supp_data, how='left', on='account_number')
live_final.drop(['index'], axis=1, inplace=True)
#export to excel
live_final.to_excel('C:/awelsh/Python Scripts/churn prediction - consolidated/cloud/final_list_probabilities_cloud_live'+ model_run_month +'- with probabilities.xlsx', index=False)
live_final[['account_number','average_invoiced_last_12_months','account_value','churn risk group', 'account_name',
            'account_team_name', 'tam', 'total_invoiced_in_month']].to_excel(
            'C:/awelsh/Python Scripts/churn prediction - consolidated/cloud/final_list_probabilities_cloud_live'+ model_run_month +'.xlsx', index=False)
print("Final file output complete")


#%%
#live_final.to_csv(path_or_buf="Y:/churn_cloud_probabilities.txt", sep="\t", encoding='utf-8', index=False, header=False)
#print('Text file updated')

#erase data in the target table to make sure it's empty
#c.publish_sql(c.cloud_live_model_sql_push1)
#print('Table erased')

#This text file is read using the local path on statcruncher / server upon which the SQL server is running
#the local text file was pushed to CSV in the above step.
#if you are specifying a file not on the SQL server itself, you will get "file not found" errors returned from 
#the SQL server
#c.publish_sql(c.cloud_live_model_sql_push2)
#print('Table updated')

#read in the test table to verify the code worked (uncomment next line for debugging/test purposes)
#sql_test1 = c.data_sql('SELECT * FROM [CustomerRetention].[dbo].[cloud_Scores] with (nolock);')

