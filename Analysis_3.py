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
        self.input_path = configuration.Configuration(os.getcwd(),'Data','Case_Study').get_person_use_path()
        self.output_path = configuration.Configuration(os.getcwd(),'Data','Case_Study').output_path_analysis_3()
        
    def analysis_3(self):
        df_person_use = pd.read_csv(self.input_path)
        df_highest_accident_state = df_person_use[df_person_use['PRSN_GNDR_ID'] == 'FEMALE']                                                .groupby('DRVR_LIC_STATE_ID')['CRASH_ID'].count().reset_index(name='COUNT')                                                .sort_values('COUNT',ascending=False).head(1)                                                .rename(columns = {'DRVR_LIC_STATE_ID':'STATE_WITH_MAX_ACCIDENTS_INVOLVING_FEMALE_ONLY'})
        df_highest_accident_state.to_csv(self.output_path,index=False,header=True)


# In[3]:


obj = main().analysis_3()
obj


# In[ ]:




