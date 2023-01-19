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
        self.input_path_person = configuration.Configuration(os.getcwd(),'Data','Case_Study').get_person_use_path()
        self.input_path_unit = configuration.Configuration(os.getcwd(),'Data','Case_Study').get_units_use_path()
        self.output_path = configuration.Configuration(os.getcwd(),'Data','Case_Study').output_path_analysis_6()
        
    def analysis_6(self):
        df_person_use = pd.read_csv(self.input_path_person)
        df_units_use = pd.read_csv(self.input_path_unit)
        df_units_cars = df_units_use[(df_units_use['VEH_BODY_STYL_ID']                                    .isin(['PASSENGER CAR, 4-DOOR','SPORT UTILITY VEHICLE','PASSENGER CAR','2-DOOR','VAN','POLICE CAR/TRUCK']))                                    & ((df_units_use['CONTRIB_FACTR_1_ID'] == 'UNDER INFLUENCE - ALCOHOL')                                    | (df_units_use['CONTRIB_FACTR_2_ID'] == 'UNDER INFLUENCE - ALCOHOL')                                    | (df_units_use['CONTRIB_FACTR_P1_ID'] == 'UNDER INFLUENCE - ALCOHOL'))]
        df_units_cars_2 = df_units_cars[['CRASH_ID','UNIT_NBR']]
        df_person_2 = df_person_use[['CRASH_ID','UNIT_NBR','DRVR_ZIP']]
        df_join = pd.merge(df_person_2,df_units_cars_2, on=['CRASH_ID','UNIT_NBR'],how='inner')
        df_top_5_drvr_zip = df_join.groupby('DRVR_ZIP')['CRASH_ID'].count().reset_index(name='COUNT')                                                                   .sort_values('COUNT',ascending=False).head(5)
        df_top_5_drvr_zip.to_csv(self.output_path, index=False, header=True)


# In[3]:


obj = main().analysis_6()
obj


# In[ ]:




