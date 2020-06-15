# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 11:15:30 2019

@author: andr3227

This script extracts the data for model training, parameter tuning, and fit (for live run, every 6 months)
"""

import churn_common as p #P2G_common.py file is in same directory as this one

churn_data = p.data_etl(p.cloud_train_fit_sql)

nulls = churn_data.isnull().sum()

churn_data.to_csv('c:/awelsh/python scripts/churn prediction - consolidated/churn_cloud_model_training_fit.csv', encoding='ISO-8859-1')
