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
        self.input_path_damage = configuration.Configuration(os.getcwd(),'input_data','output_data').get_damages_use_path()
        self.input_path_unit = configuration.Configuration(os.getcwd(),'input_data','output_data').get_units_use_path()
        self.output_path = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_7()
        
    def analysis_7(self): 
        df_damage = pd.read_csv(self.input_path_damage)
        df_unit_use = pd.read_csv(self.input_path_unit)
        df_nodamage = df_unit_use[~df_unit_use['CRASH_ID'].isin(df_damage['CRASH_ID'])]
        df_nodamage = df_nodamage[['CRASH_ID','FIN_RESP_TYPE_ID','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID']]
        df_nodamage = df_nodamage[df_nodamage['FIN_RESP_TYPE_ID'].notna()]
        df_nodamage['VEH_DMAG_SCL_1_ID'] = df_nodamage['VEH_DMAG_SCL_1_ID'].astype('str').str                                                                           .extractall('(\d+)').unstack().fillna('')                                                                           .sum(axis=1).astype(int)
        df_nodamage['VEH_DMAG_SCL_2_ID'] = df_nodamage['VEH_DMAG_SCL_2_ID'].astype('str').str                                                                           .extractall('(\d+)').unstack().fillna('')                                                                           .sum(axis=1).astype(int)
        df_nodamage['VEH_DMAG_SCL_1_ID'] = df_nodamage['VEH_DMAG_SCL_1_ID'].fillna(0)
        df_nodamage['VEH_DMAG_SCL_2_ID'] = df_nodamage['VEH_DMAG_SCL_2_ID'].fillna(0)
        df_nodamage = df_nodamage[(df_nodamage['VEH_DMAG_SCL_1_ID'] > 4) & (df_nodamage['VEH_DMAG_SCL_2_ID'] > 4)]
        df_nodamage = df_nodamage[['CRASH_ID']].drop_duplicates().count().reset_index(drop=True, inplace=False)  
        df_analysis_7 = df_nodamage.to_frame(name='COUNT_OF_DISTINCT_CRASHES')
        df_analysis_7.to_csv(self.output_path, index=False, header=True)


# In[3]:


obj = main().analysis_7()
obj


# In[ ]:




