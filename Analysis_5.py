#!/usr/bin/env python
# coding: utf-8

# In[2]:


import configuration
import pandas as pd
import warnings
import re
import os
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None,'display.max_rows',100)


# In[3]:


class main:
    def __init__(self):
        self.input_path_person = configuration.Configuration(os.getcwd(),'input_data','output_data').get_person_use_path()
        self.input_path_unit = configuration.Configuration(os.getcwd(),'input_data','output_data').get_units_use_path()
        self.output_path = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_5()
        
    def analysis_5(self): 
        df_person_use = pd.read_csv(self.input_path_person)
        df_unit_use = pd.read_csv(self.input_path_unit)
        df_person_use_2 = df_person_use[['CRASH_ID','UNIT_NBR','PRSN_ETHNICITY_ID']]
        df_unit_use_2 = df_unit_use[['CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID']]
        df_join_person_units = pd.merge(df_unit_use_2,df_person_use_2,on=['CRASH_ID','UNIT_NBR'],how='left')
        df_ethnic_group_vs_body_style = df_join_person_units.groupby(['VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID'])['CRASH_ID']                                                            .count().reset_index(name='COUNT')
        df_ethnic_group_vs_body_style = df_ethnic_group_vs_body_style.groupby('VEH_BODY_STYL_ID')                                                                     .apply(lambda x: x[x['COUNT']==x['COUNT'].max()])
        df_ethnic_group_vs_body_style.reset_index(drop=True, inplace=True)
        df_ethnic_group_vs_body_style.to_csv(self.output_path, index=False, header=True)


# In[4]:


obj = main().analysis_5()
obj


# In[ ]:




