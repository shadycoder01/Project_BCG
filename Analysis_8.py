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


# In[4]:


class main:
    def __init__(self):
        self.input_path_charge = configuration.Configuration(os.getcwd(),'input_data','output_data').get_charges_use_path()
        self.input_path_unit = configuration.Configuration(os.getcwd(),'input_data','output_data').get_units_use_path()
        self.input_path_person = configuration.Configuration(os.getcwd(),'input_data','output_data').get_person_use_path()
        self.output_path = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_8()
        
    def analysis_8(self): 
        df_charges = pd.read_csv(self.input_path_charge)
        df_units = pd.read_csv(self.input_path_unit)
        df_person = pd.read_csv(self.input_path_person)
        
        df_units_merge_charges = df_units.merge(df_charges,on=['CRASH_ID','UNIT_NBR'],how='inner')
        
        state_top25 = df_units_merge_charges.groupby('VEH_LIC_STATE_ID')['CHARGE']                                            .count().reset_index(name='COUNT')                                            .sort_values('COUNT',ascending=False).iloc[:25]                                            .reset_index()['VEH_LIC_STATE_ID'].tolist()
        
        df_units_merge_charges = df_units_merge_charges[df_units_merge_charges['VEH_BODY_STYL_ID'].str.contains('car',case=False) == True]
        df_units_merge_charges = df_units_merge_charges[~df_units_merge_charges['VEH_LIC_STATE_ID'].isna()]
        color_top10 = df_units_merge_charges.groupby('VEH_COLOR_ID')['VEH_LIC_STATE_ID']                                            .count().reset_index(name='COUNT')                                            .sort_values('COUNT',ascending=False)                                            .iloc[:10].reset_index()['VEH_COLOR_ID'].tolist()
        
        df_units_merge_charges = df_units_merge_charges[df_units_merge_charges['CHARGE'].str.contains('speed',case=False) == True]
        df_charges_2 = df_charges[df_charges['CHARGE'].str.contains('speed',case=False) == True]
        df_units_merge_charges = df_units_merge_charges[df_units_merge_charges['CRASH_ID'].isin(df_charges_2['CRASH_ID'])]
        
        df_person = df_person[df_person['DRVR_LIC_TYPE_ID'].isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'])]['CRASH_ID'].unique()
        df_units_merge_charges = df_units_merge_charges[df_units_merge_charges['CRASH_ID'].isin(df_person)]
        
        df_veh_top10_colour = df_units_merge_charges[df_units_merge_charges['VEH_COLOR_ID'].isin(color_top10)]
        df_top25_state_id = df_units_merge_charges[df_units_merge_charges['VEH_LIC_STATE_ID'].isin(state_top25)]
        df_concat =  pd.concat([df_veh_top10_colour,df_top25_state_id]).drop_duplicates()
        
        df_analysis_8 = df_concat.groupby('VEH_MAKE_ID')['CRASH_ID'].count().reset_index(name='COUNT')                                                                    .sort_values('COUNT',ascending=False)                                                                    .iloc[:5].reset_index(drop=True)
        
        df_analysis_8.to_csv(self.output_path, index=False,header=True)


# In[5]:


obj = main().analysis_8()
obj


# In[ ]:




