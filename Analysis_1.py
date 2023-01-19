#!/usr/bin/env python
# coding: utf-8

# In[9]:


import configuration
import pandas as pd
import warnings
import re
import os
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None,'display.max_rows',100)


# In[10]:


class main:
    def __init__(self):
        """
        This function is used to initialise path to input and output data
        """
        self.input_path = configuration.Configuration(os.getcwd(),'Data','Case_Study').get_person_use_path()
        self.output_path = configuration.Configuration(os.getcwd(),'Data','Case_Study').output_path_analysis_1()
        
    def analysis_1(self):
        df_person_use = pd.read_csv(self.input_path)
        count_of_crashes = df_person_use[(df_person_use['PRSN_INJRY_SEV_ID']=='KILLED') & (df_person_use['PRSN_GNDR_ID']=='MALE')][['CRASH_ID']]
        count_of_crashes.rename(columns = {'CRASH_ID':'COUNT_OF_CRASHES'}, inplace=True)
        count_of_crashes = count_of_crashes[['COUNT_OF_CRASHES']].count().reset_index(drop=True, inplace=False)
        count_of_crashes = count_of_crashes.to_frame(name='COUNT_OF_CRASHES')
        count_of_crashes.to_csv(self.output_path, index=False, header=True)


# In[11]:


obj = main().analysis_1()
obj

