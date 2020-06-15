# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 14:52:27 2019

@author: andr3227
"""

#common functions for propensity to grow model tuning and model fit

import pypyodbc
import pandas as pd
import numpy as np
import random
from random import seed
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support, accuracy_score, roc_auc_score
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
rcParams['figure.figsize'] = 12, 4

import time
import datetime
import xgboost as xgb
#from xgboost.sklearn import XGBClassifier

#def collect_data(conf):
#    sql_file = "./support_files/ded_cust_live/ded_live_data.sql"
#    df = db_cache.sql_extract(conf, sql_file)
#    df.set_index(['account_number','time_month_key'])
#    LOG.debug('Query complete')
#    LOG.debug(len(df))
#    return df
def read_file(filename):
    with open(filename, 'r') as f:
        out = f.read()
    return out

dedi_live_model_sql = read_file('sql/dedi_live_model.sql')

dedi_live_model_supp_data_sql = read_file('sql/dedi_live_model_supp_data.sql')

dedi_live_model_sql_push1 = read_file('sql/dedi_live_model_sql_push1.sql')

dedi_live_model_sql_push2 = read_file('sql/dedi_live_model_sql_push2.sql')

dedi_train_fit_sql = read_file('sql/dedi_train_fit.sql')

dedi_model_fit_include_col_low = [
'3_mth_pct_change_number_of_devices_active_status_active'
,'3_mth_pct_change_number_of_devices_online_status_online'
,'account_has_sku_name_eq_monitoring_last_6mo'
,'avg_invoiced_in_last_3_months_vs_prior_3_months'
,'avg_invoiced_in_last_6_months_vs_prior_6_months'
,'avg_unit_price_managed_exchange'
,'avg_unit_price_rackspace_email'
,'avg_unit_price_rpc_core'
,'change_in_survey_score'
,'does_account_have_cloud_files'
,'does_account_have_managed_storage'
,'how_many_units_next_gen_servers'
,'how_many_units_rackspace_email'
,'how_many_units_rpc_core'
,'last_survey_score'
,'num_distinct_account_billing_postal_code'
,'num_opportunities_last_3_months'
,'number_of_customer_accounts'
,'number_of_devices_os_firewall'
,'pct_of_device_status_eq_online_complete'
,'pct_of_device_status_eq_support_maintenance'
,'pct_of_devices_with_contract_status_eq_in_contract'
,'pct_of_invoice_bandwidth_overages'
,'pct_of_invoice_managed_exchange'
,'pct_of_invoice_managed_storage'
,'pct_of_invoice_rpc_core'
,'rating_detractor_to_detractor'
,'rating_detractor_to_passive'
,'rating_passive_to_promoter'
,'rating_promoter_to_passive'
,'ratio_num_of_invoiced_items_in_last_3_months_vs_prior_3_months'
,'ratio_num_of_invoiced_items_in_last_6_months_vs_prior_6_months'
,'shortest_active_device_tenure_months'
,'total_invoiced_in_last_6_months'
]

dedi_model_fit_include_col_mid = [
'3_mth_pct_change_number_of_devices_active_status_active'
,'3_mth_pct_change_number_of_devices_online_status_online'
,'account_has_sku_name_eq_monitoring_last_6mo'
,'avg_invoiced_in_last_3_months_vs_prior_3_months'
,'avg_invoiced_in_last_6_months_vs_prior_6_months'
,'avg_unit_price_managed_exchange'
,'avg_unit_price_rackspace_email'
,'avg_unit_price_rpc_core'
,'change_in_survey_score'
,'does_account_have_cloud_files'
,'does_account_have_managed_storage'
,'how_many_units_next_gen_servers'
,'how_many_units_rackspace_email'
,'how_many_units_rpc_core'
,'last_survey_score'
,'num_distinct_account_billing_postal_code'
,'num_opportunities_last_3_months'
,'number_of_customer_accounts'
,'number_of_devices_os_firewall'
,'pct_of_device_status_eq_online_complete'
,'pct_of_device_status_eq_support_maintenance'
,'pct_of_devices_with_contract_status_eq_in_contract'
,'pct_of_invoice_bandwidth_overages'
,'pct_of_invoice_managed_exchange'
,'pct_of_invoice_managed_storage'
,'pct_of_invoice_rpc_core'
,'rating_detractor_to_detractor'
,'rating_detractor_to_passive'
,'rating_passive_to_promoter'
,'rating_promoter_to_passive'
,'ratio_num_of_invoiced_items_in_last_3_months_vs_prior_3_months'
,'ratio_num_of_invoiced_items_in_last_6_months_vs_prior_6_months'
,'shortest_active_device_tenure_months'
,'total_invoiced_in_last_6_months'
]

dedi_model_fit_include_col_hi = [
'3_mth_pct_change_number_of_devices_active_status_active'
,'3_mth_pct_change_number_of_devices_online_status_online'
,'account_has_sku_name_eq_monitoring_last_6mo'
,'avg_invoiced_in_last_3_months_vs_prior_3_months'
,'avg_invoiced_in_last_6_months_vs_prior_6_months'
,'avg_unit_price_managed_exchange'
,'avg_unit_price_rackspace_email'
,'avg_unit_price_rpc_core'
,'change_in_survey_score'
,'does_account_have_cloud_files'
,'does_account_have_managed_storage'
,'how_many_units_next_gen_servers'
,'how_many_units_rackspace_email'
,'how_many_units_rpc_core'
,'last_survey_score'
,'num_distinct_account_billing_postal_code'
,'num_opportunities_last_3_months'
,'number_of_customer_accounts'
,'number_of_devices_os_firewall'
,'pct_of_device_status_eq_online_complete'
,'pct_of_device_status_eq_support_maintenance'
,'pct_of_devices_with_contract_status_eq_in_contract'
,'pct_of_invoice_bandwidth_overages'
,'pct_of_invoice_managed_exchange'
,'pct_of_invoice_managed_storage'
,'pct_of_invoice_rpc_core'
,'rating_detractor_to_detractor'
,'rating_detractor_to_passive'
,'rating_passive_to_promoter'
,'rating_promoter_to_passive'
,'ratio_num_of_invoiced_items_in_last_3_months_vs_prior_3_months'
,'ratio_num_of_invoiced_items_in_last_6_months_vs_prior_6_months'
,'shortest_active_device_tenure_months'
,'total_invoiced_in_last_6_months'
]


#
#
# CLOUD MODEL
#
#

cloud_live_model_sql = read_file('sql/cloud_live_model.sql')

cloud_live_model_supp_data_sql = read_file('sql/cloud_live_model_supp_data.sql')

cloud_live_model_sql_push1 = read_file('sql/cloud_live_model_sql_push1.sql')

cloud_live_model_sql_push2 = read_file('sql/cloud_live_model_sql_push2.sql')
            
cloud_model_fit_exclude_col = [
        'account_number'
        ,'time_month_key'
        ,'time_month_key_dt'
        ,'account_name'
        ,'average_invoiced_last_12_months'
        ,'revenue_segment'
        ,'avg_monthly_invoice_band'
        ,'churn_flag'
        ,'target'
        ,'pct_change'
        ,'first_churn_tmk'
        ,'total_invoiced_in_month'
        ,'average_invoiced_next_6_months'
        ,'num_months_last_12_months'
        ,'num_months_next_6_months'
        ,'month_order'
        ,'month_order_desc'
        ,'time_month_key_eo_last_month_dt'
        ,'account_type_cloud_uk'
        ,'account_segment'
        ,'account_sub_type'
 ]

cloud_train_fit_sql = read_file('sql/cloud_train_fit.sql')

#===============================================================================================================
#===============================================================================================================
#===============================================================================================================
#CLASSES
#===============================================================================================================
#===============================================================================================================
#===============================================================================================================


class TimeSeriesSplitTMK(TimeSeriesSplit):
    #def __init__ here is copy-paste from already working scikit-learn code
    def __init__(self, n_splits=5, max_train_size=None):
        super().__init__(n_splits, max_train_size)
        self.sorted_groups = []
        #self.max_train_size = max_train_size

        
        
    def group_split(self, X, y=None, groups=None):
        n_samples = len(X)
        n_splits = self.n_splits #number of splits, default 5
        n_folds = n_splits + 1 #default 6
        if n_folds > n_samples:
            raise ValueError(
                ("Cannot have number of folds ={0} greater"
                 " than the number of samples: {1}.").format(n_folds,
                                                             n_samples))
        #hard-coded to have time_month_key_dt.. fix this later to pass a date column as a parameter
        #Group the dataframe by month-year (i.e. Jan 17, Jan 18, versus lumping Jan 17, Jan 18 into "January")
        groups = X.groupby(X['time_month_key_dt'].dt.to_period("M")).groups
        
        self.sorted_groups = [value for (key, value) in sorted(groups.items())] 
        n_groups = len(self.sorted_groups)

        indices = np.arange(n_groups) #same as range(n_groups)
        
        test_size = (n_groups // n_folds) #floor division
        test_starts = range(test_size + n_groups % n_folds,
                            n_groups, test_size) 
        #arrange each month slice per the algorithm in sklearn's TimeSeriesSplit
        for test_start in test_starts:
            if self.max_train_size and self.max_train_size < test_start:
                yield (self.sorted_groups[indices][test_start - self.max_train_size:test_start],
                       indices[test_start:test_start + test_size])
            else:
                yield (indices[:test_start],
                       indices[test_start:test_start + test_size])
                
                
    def split(self, X, y=None, groups=None):
        #function to convert the month-group indicies into the original dataframe indicies
        #e.g. time month key 201701 has month-group index 15, so instead of determining test-train inclusion 
        #with the month-group index of 15, determine test-train inclusion using their original index 
        my_stuff = self.group_split(X, y, groups)
        for train, test in my_stuff:
            training_list = train.tolist()
            test_list = test.tolist()
            training_chunks = [x.values.tolist() for x in self.sorted_groups[training_list[0]:training_list[-1]]]
            test_chunks = [x.values.tolist() for x in self.sorted_groups[test_list[0]:test_list[-1]]]
            # what = [x.values.tolist() for y in each[0] for x in self.sorted_groups]
            train_idx = [y for x in training_chunks for y in x]
            test_idx = [y for x in test_chunks for y in x]
            train_idx.sort()
            test_idx.sort()
            test_train_indices =(np.asarray(train_idx), np.asarray(test_idx))
            yield (test_train_indices)
            #[y for x in self.sorted_groups for y in x] #will print flattened list


#===============================================================================================================
#===============================================================================================================
#===============================================================================================================
#FUNCTIONS
#===============================================================================================================
#===============================================================================================================
#===============================================================================================================

#function to perform SQL query and return dataframe
def data_sql(sqltext):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};Server=ordstatcrunch01.rackspace.corp;"
                            "Database=master;Uid=DataTeamMember;Trusted_Connection=yes;")
    df = pd.read_sql(sqltext, connection)
    return df

#function to perform SQL to modify tables and return dataframe
def publish_sql(sqltext):
    connection = pypyodbc.connect("Driver={SQL Server Native Client 11.0};Server=ordstatcrunch01.rackspace.corp;"
                            "Database=master;Uid=DataTeamMember;Trusted_Connection=yes;")
    sql_cur = connection.cursor()
    sql_cur.execute(sqltext).commit()
    #df = pd.read_sql(sqltext, connection)
    #return df, sql_cur

#returns result of SQL query passed to function. Currently, connection is hardcoded to statcruncher
def data_etl(sqltext):
    df = data_sql(sqltext)
    df.set_index(['account_number','time_month_key'])
    #df.select_dtypes(include=['object']).apply(pd.to_numeric, downcast='float')
    print('Query complete, index set')
    df = df.fillna({
    '3_mth_pct_change_longest_active_device_tenure_months':0,
    '3_mth_pct_change_number_of_device_status_eq_computer_no_longer_active':0,
    '3_mth_pct_change_number_of_devices_active_status_active':0,
    '3_mth_pct_change_number_of_devices_online_status_online':0,
    'annualrevenue_original':0,
    'average_active_device_tenure_months':0,
    'average_value_of_opportunities_last_3_months':0,
    'average_value_of_opportunities_last_6_months':0,
    'avg_credit_memo_in_last_6_months':0,
    'avg_mthly_num_of_invoiced_items_in_last_3_months_vs_prior_3_months':0,
    'avg_mthly_num_of_invoiced_items_in_last_6_months_vs_prior_6_months':0,
    'avg_mthly_num_of_invoiced_items_in_last_6_months':0,
    'avg_per_line_item_invoiced_in_last_6_months_original':0,
    'how_many_units_bandwidth_overages':0,
    'how_many_units_cloud_block_storage':0,
    'how_many_units_dedicated_san':0,
    'how_many_units_firewall':0,
    'how_many_units_load_balancer':0,
    'how_many_units_managed_exchange':0,
    'how_many_units_managed_storage':0,
    'how_many_units_rackspace_email':0,
    'how_many_units_rpc_core':0,
    'how_many_units_san':0,
    'how_many_units_server':0,
    'how_many_units_switch':0,
    'how_many_units_threat_manager':0,
    'how_many_units_virtual_hosting':0,
    'how_many_units_virtualization':0,
    'longest_active_device_tenure_months':0,
    'num_distinct_account_bdc':0,
    'num_distinct_account_billing_city':0,
    'num_distinct_account_billing_country':0,
    'num_distinct_account_billing_postal_code':0,
    'num_distinct_account_billing_state':0,
    'num_distinct_account_billing_street':0,
    'num_distinct_account_business_type':0,
    'num_distinct_account_manager':0,
    'num_distinct_account_primary_contact':0,
    'num_opportunities_last_6_months':0,
    'num_opportunities_won_last_3_months':0,
    'num_opportunities_won_last_6_months':0,
    'num_opportunities':0,
    'num_opps_category_migration':0,
    'num_opps_support_unit_smb':0,
    'num_opps_typex_aws':0,
    'num_opps_typex_dedicated_private_cloud':0,
    'num_opps_typex_revenue_ticket':0,
    'number_of_device_status_eq_computer_no_longer_active':0,
    'number_of_device_status_eq_online_complete':0,
    'number_of_device_status_eq_support_maintenance':0,
    'number_of_devices_last_month':0,
    'number_of_devices_os_firewall':0,
    'number_of_devices_os_load_balancer':0,
    'number_of_devices_os_switch':0,
    'number_of_devices_os_name_linux':0,
    'number_of_other_device_status':0,
    'number_of_accounts_original':0,
    'number_of_cloud_accounts':0,
    'number_of_customer_accounts':0,
    'numberofemployees_original':0,
    'pct_of_device_status_eq_support_maintenance':0,
    'pct_of_devices_with_contract_status_eq_in_contract':0,
    'pct_of_devices_with_contract_status_eq_no_contract_status':0,
    'pct_of_devices_with_contract_status_eq_out_of_contract_mtm':0,
    'pct_of_other_device_status':0,
    'pct_of_revenue_with_contract_status_in_contract_risk_of_lapse_90_days':0,
    'pct_of_revenue_with_contract_status_in_contract':0,
    'pct_of_revenue_with_contract_status_no_contract_status':0,
    'pct_of_revenue_with_contract_status_out_of_contract_mtm':0,
    'pct_opportunities_lost_last_3_months':0,
    'pct_opportunities_won_last_3_months':0,
    'pct_opportunities_won_last_6_months':0,
    'total_invoiced_in_last_6_months':0,
    'total_value_of_opportunities_last_6_months':0,
                    })
    print('Nulls imputed to 0')
    return df

def split_df_revenue_segments(df, invoice_column, low=5000, high=25000):
    #split dataset into low-med-hi revenue segments
    #invoice_column = 'average_invoiced_last_12_months'
    low_df = df.loc[
        df[invoice_column] < low].reset_index(drop=True)
    mid_df = df.loc[
        (df[invoice_column] >= low) &
        (df[invoice_column] < high)].reset_index(drop=True)
    high_df = df.loc[
        df[invoice_column] >= high].reset_index(drop=True)
    return low_df, mid_df, high_df

def AndrewsRandomUnderSampler(df, target_column, seed_num, majority_class_target_value=0, minority_class_target_value=1):
    #create random under sampler of dataset
    #based on minority class size, select random sample of majority class rows to equal the size of minority class
    minority_class = df.loc[df[target_column]==minority_class_target_value].reset_index(drop=True) #reset the index and drop the col 'index'
    majority_class = df.loc[df[target_column]==majority_class_target_value].reset_index(drop=True)
    
    # find size of minority class
    sample_size = len(minority_class)
    
    #get list of indicies of df where target=0 (majority class). From that list, randomly select
    rus_ix = list(majority_class.index.values)
    
    #seed is used to make results consistent for testing; pass None as seed_num setting for random assignment
    seed(seed_num)
    sample = [rus_ix[i] for i in random.sample(range(len(rus_ix)), sample_size)] #create list of indicies
    rus_majority = majority_class.iloc[sample] #select only rows in majority class based on random sample
    
    #recombine minority class rows with sampled majority class rows
    final = pd.concat([minority_class, rus_majority], ignore_index=True)
    return final






#define function to extract the specificity, sensitivty and F-score from precision_recall_fscore_support function
#for easy display and model evaluation
#For this analysis, using F2 score to weigh sensitivity more than specificity (we want fewer false negatives)
#F1 score for balanced evaluation, F0.5 for favoring specificity over sensitivity
#just set the beta_val value you pass to the function to adjust
    
def fscore (y1, y2, beta_val):
    temp = pd.DataFrame(list(precision_recall_fscore_support(y1, y2, beta=beta_val)))
    print("Precision-Specificity \n", temp.iloc[0][1])
    print("Recall-Sensitivity \n", temp.iloc[1][1])
    print("F",beta_val,"Score \n", temp.iloc[2][1])
    print("\n")
    print("Accuracy:")
    print(accuracy_score(y1, y2))
    print("\n")



#function to fit XGBoost classifier model to live data, and print feature importance graph
#this function presumes random over/under sampling is already done, and is used when
#the parameter tuning, feature engineering, and feature selection process steps are done
def modelfit(alg, training_dataset_X, training_dataset_Y, 
             predictors, useTrainCV=True, cv_folds=2, early_stopping_rounds=20):
    
    start_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("\nModel Fit Start time")
    print(start_time)
    print("\n")

    if useTrainCV:
        xgb_param = alg.get_xgb_params()
        xgtrain = xgb.DMatrix(training_dataset_X, label=training_dataset_Y)
        cvresult = xgb.cv(xgb_param, xgtrain, num_boost_round=alg.get_params()['n_estimators'], nfold=cv_folds,
            metrics='auc', early_stopping_rounds=early_stopping_rounds)
        alg.set_params(n_estimators=cvresult.shape[0])
    
    #Fit the algorithm on the data
    alg.fit(training_dataset_X, training_dataset_Y,eval_metric='auc')
        
    #plot feature importances
    feat_imp = pd.Series(alg.get_booster().get_fscore()).sort_values(ascending=False)
    feat_imp.plot(kind='bar', title='Feature Importances')
    plt.ylabel('Feature Importance Score')
    
    #print head of feature importance list
    print(feat_imp.head())
	  
    #display timestamp when model fit is done
    end_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("\nModel Fit End time")
    print(end_time)


#function to fit XGBoost classifier model for parameter tuning grid searches, with cross validation 
#and prints a feature importance graph. This function presumes the train-test split and random over/under 
#sampling is already done
def modeleval(alg, training_dataset_X, training_dataset_Y, evalX, evalY, 
             predictors, useTrainCV=True, cv_folds=2, early_stopping_rounds=20):
    
    start_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("\nModel Fit Start time")
    print(start_time)
    print("\n")

    if useTrainCV:
        xgb_param = alg.get_xgb_params()
        xgtrain = xgb.DMatrix(training_dataset_X, label=training_dataset_Y)
        cvresult = xgb.cv(xgb_param, xgtrain, num_boost_round=alg.get_params()['n_estimators'], nfold=cv_folds,
            metrics='auc', early_stopping_rounds=early_stopping_rounds)
        alg.set_params(n_estimators=cvresult.shape[0])
    
    #Fit the algorithm on the data
    alg.fit(training_dataset_X, training_dataset_Y,eval_metric='auc')
        
#    #Predict training set:
#    predictions = alg.predict(training_dataset_X)
#    predprob = alg.predict_proba(training_dataset_X)[:,1]
    
#    #Print model report:
#    print("\nModel Report")
#    print("Accuracy : %.4g" % accuracy_score(training_dataset_Y, predictions))
#    print("AUC Score (Train): %f" % roc_auc_score(training_dataset_Y, predprob))
    
    #plot feature importances
    feat_imp = pd.Series(alg.get_booster().get_fscore()).sort_values(ascending=False)
    feat_imp.plot(kind='bar', title='Feature Importances')
    plt.ylabel('Feature Importance Score')
    plt.show()
    
    #print head of feature importance list
    print(feat_imp.head())
	
    #provide confusion matrix (likely overfitted) for test data not withheld by specific TMK
    #used for parameter tuning grid search, can be commented out for final model fits
    #testpred = alg.predict(testX)
    #cmt = confusion_matrix(testY, testpred)
    #print("Confusion matrix for internal test data:\n", cmt)   
    #fscore(testY, testpred, 2)
    
    #provide confusion matrix for evaluation data (specific TMK's)
    evalpred = alg.predict(evalX)
    cme = confusion_matrix(evalY, evalpred)
    print("Confusion matrix for evaluation data:\n", cme)
    fscore(evalY, evalpred, 2)
    
    #display timestamp when model fit is done
    end_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("\nModel Fit End time")
    print(end_time)



#function to fit XGBoost classifier model for parameter tuning grid searches, with cross validation 
#and prints a feature importance graph. This function presumes the train-test split and random over/under 
#sampling is already done
def modeltune(alg, training_dataset_X, training_dataset_Y, testX, testY, evalX, evalY, 
             predictors, useTrainCV=True, cv_folds=2, early_stopping_rounds=20):
    
    start_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("\nModel Fit Start time")
    print(start_time)
    print("\n")

    if useTrainCV:
        xgb_param = alg.get_xgb_params()
        xgtrain = xgb.DMatrix(training_dataset_X, label=training_dataset_Y)
        cvresult = xgb.cv(xgb_param, xgtrain, num_boost_round=alg.get_params()['n_estimators'], nfold=cv_folds,
            metrics='auc', early_stopping_rounds=early_stopping_rounds)
        alg.set_params(n_estimators=cvresult.shape[0])
    
    #Fit the algorithm on the data
    alg.fit(training_dataset_X, training_dataset_Y,eval_metric='auc')
        
    #Predict training set:
    predictions = alg.predict(training_dataset_X)
    predprob = alg.predict_proba(training_dataset_X)[:,1]
    
    #Print model report:
    print("\nModel Report")
    print("Accuracy : %.4g" % accuracy_score(training_dataset_Y, predictions))
    print("AUC Score (Train): %f" % roc_auc_score(training_dataset_Y, predprob))
    
    #plot feature importances
    feat_imp = pd.Series(alg.get_booster().get_fscore()).sort_values(ascending=False)
    feat_imp.plot(kind='bar', title='Feature Importances')
    plt.ylabel('Feature Importance Score')
     
    #print head of feature importance list
    print(feat_imp.head())
	
#    provide confusion matrix (likely overfitted) for test data not withheld by specific TMK
#    used for parameter tuning grid search, can be commented out for final model fits
    testpred = alg.predict(testX)
    cmt = confusion_matrix(testY, testpred)
    print("Confusion matrix for internal test data:\n", cmt)   
    fscore(testY, testpred, 2)
    
    #provide confusion matrix for evaluation data (specific TMK's)
    evalpred = alg.predict(evalX)
    cme = confusion_matrix(evalY, evalpred)
    print("Confusion matrix for evaluation data:\n", cme)
    fscore(evalY, evalpred, 2)
    
    #display timestamp when model fit is done
    end_time = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("\nModel Fit End time")
    print(end_time)