#!/usr/bin/env python
# coding: utf-8

# In[1]:


import configuration
import pandas as pd
import warnings
import re
import os
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None,'display.max_rows',100)


# In[2]:


class main:
    def __init__(self):
        self.input_path_units = configuration.Configuration(os.getcwd(),'Data','Case_Study').get_units_use_path()
        self.input_path_charges = configuration.Configuration(os.getcwd(),'Data','Case_Study').get_charges_use_path()
        self.output_path = configuration.Configuration(os.getcwd()).output_path_analysis_2()
        
    def analysis_2(self): 
        df_units = pd.read_csv(self.input_path_units)[['CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID']]
        df_charges = pd.read_csv(self.input_path_charges)
        df_join = df_units.merge(df_charges,on=['CRASH_ID','UNIT_NBR'],how='inner')
        count_of_two_wheelers_charged = df_join[df_join['VEH_BODY_STYL_ID'].isin(['MOTORCYCLE','POLICE MOTORCYCLE'])][['CRASH_ID']]
        count_of_two_wheelers_charged.rename(columns = {'CRASH_ID':'COUNT_OF_TWO_WHEELERS_CHARGED'}, inplace=True)
        count_of_two_wheelers_charged = count_of_two_wheelers_charged[['COUNT_OF_TWO_WHEELERS_CHARGED']].count().reset_index(drop=True, inplace=False)
        count_of_two_wheelers_charged = count_of_two_wheelers_charged.to_frame(name='COUNT_OF_TWO_WHEELERS_CHARGED')
        count_of_two_wheelers_charged.to_csv(self.output_path, index=False, header=True)


# In[3]:


obj = main().analysis_2()
obj


# In[ ]:




