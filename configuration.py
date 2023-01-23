#!/usr/bin/env python
# coding: utf-8

# In[1]:


class Configuration:
    def __init__(self,path=None,input_data=None,output_data=None):
        self.input_path = path + '\\' + input_data + '\\'
        self.output_path = path + '\\' + output_data + '\\'
        self.person = 'Primary_Person_use.csv'
        self.charges = "Charges_use.csv"
        self.damages = "Damages_use.csv"
        self.endorse = "Endorse_use.csv"
        self.restrict = "Restrict_use.csv"
        self.units = 'Units_use.csv'
        self.analysis_1 = 'Analysis_1.csv'
        self.analysis_2 = 'Analysis_2.csv'
        self.analysis_3 = 'Analysis_3.csv'
        self.analysis_4 = 'Analysis_4.csv'
        self.analysis_5 = 'Analysis_5.csv'
        self.analysis_6 = 'Analysis_6.csv'
        self.analysis_7 = 'Analysis_7.csv'
        self.analysis_8 = 'Analysis_8.csv'
        
    def get_person_use_path(self):
        try:
            return self.input_path + self.person
        except:
            raise
    def get_units_use_path(self):
        try:
            return self.input_path + self.units
        except:
            raise
    def get_charges_use_path(self):
        try:
            return self.input_path + self.charges
        except:
            raise
    def get_damages_use_path(self):
        try:
            return self.input_path + self.damages
        except:
            raise
    def get_endorse_use_path(self):
        try:
            return self.input_path + self.endorse
        except:
            raise
    def get_restrict_use_path(self):
        try:
            return self.input_path + self.restrict
        except:
            raise
    def output_path_analysis_1(self):
        try:
            return self.output_path + self.analysis_1
        except:
            raise
    def output_path_analysis_2(self):
        try:
            return self.output_path + self.analysis_2
        except:
            raise
    def output_path_analysis_3(self):
        try:
            return self.output_path + self.analysis_3
        except:
            raise
    def output_path_analysis_4(self):
        try:
            return self.output_path + self.analysis_4
        except:
            raise
    def output_path_analysis_5(self):
        try:
            return self.output_path + self.analysis_5
        except:
            raise
    def output_path_analysis_6(self):
        try:
            return self.output_path + self.analysis_6
        except:
            raise
    def output_path_analysis_7(self):
        try:
            return self.output_path + self.analysis_7
        except:
            raise
    def output_path_analysis_8(self):
        try:
            return self.output_path + self.analysis_8
        except:
            raise


# In[ ]:




