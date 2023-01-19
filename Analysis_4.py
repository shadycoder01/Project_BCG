#!/usr/bin/env python
# coding: utf-8

# In[3]:


import configuration
import pandas as pd
import warnings
import re
import os
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None,'display.max_rows',100)


# In[1]:


class main:
    def __init__(self):
        self.input_path = configuration.Configuration(os.getcwd(),'Data','Case_Study').get_units_use_path()
        self.output_path = configuration.Configuration(os.getcwd(),'Data','Case_Study').output_path_analysis_4()
        
    def analysis_4(self): 
        df_unit_use = pd.read_csv(self.input_path)
        df_unit_use['TOTAL_INJURIES'] = df_unit_use['TOT_INJRY_CNT'] + df_unit_use['DEATH_CNT']
        df_veh_make_id = df_unit_use.groupby('VEH_MAKE_ID').agg({'TOTAL_INJURIES':'sum'})                                                           .sort_values('TOTAL_INJURIES',ascending = False)
        df_5th_to_15th_veh_make_id = df_veh_make_id.iloc[4:15].reset_index()
        df_5th_to_15th_veh_make_id.to_csv(self.output_path, index=False, header=True)


# In[5]:


obj = main().analysis_4()
obj


# In[ ]:




