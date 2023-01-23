#!/usr/bin/env python
# coding: utf-8

# In[205]:


from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import configuration
import warnings
import os
import sys
from pyspark.sql.window import Window
warnings.filterwarnings('ignore')


# In[206]:


spark = SparkSession.builder.master('local[1]').appName('Case_Study_Sabhye').getOrCreate()


# In[390]:


class main:
    def __init__(self):
        """
        This function is used to initialise path to input and output data
        """
        self.input_path_person = configuration.Configuration(os.getcwd(),'input_data','output_data').get_person_use_path()
        self.input_path_units = configuration.Configuration(os.getcwd(),'input_data','output_data').get_units_use_path()
        self.input_path_charges = configuration.Configuration(os.getcwd(),'input_data','output_data').get_charges_use_path()
        self.input_path_damage = configuration.Configuration(os.getcwd(),'input_data','output_data').get_damages_use_path()
        
        self.output_path_1 = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_1()
        self.output_path_2 = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_2()
        self.output_path_3 = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_3()
        self.output_path_4 = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_4()
        self.output_path_5 = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_5()
        self.output_path_6 = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_6()
        self.output_path_7 = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_7()
        self.output_path_8 = configuration.Configuration(os.getcwd(),'input_data','output_data').output_path_analysis_8()
        
        self.df_person_use = spark.read.format('csv').options(header=True,inferSchema=True).load(self.input_path_person)
        self.df_units = spark.read.format('csv').options(header=True,inferSchema=True).load(self.input_path_units)
        self.df_charges = spark.read.format('csv').options(header=True,inferSchema=True).load(self.input_path_charges)
        self.df_damages = spark.read.format('csv').options(header=True,inferSchema=True).load(self.input_path_damage)
        
    def analysis_1(self):
        count_of_crashes = self.df_person_use.where((col('PRSN_INJRY_SEV_ID')=='KILLED') & (col('PRSN_GNDR_ID')=='MALE')).distinct().count()
        
        new_schema=StructType([StructField('COUNT_OF_CRASHES',IntegerType(),True)])
        count_of_crashes = spark.createDataFrame([[count_of_crashes]],schema=new_schema)
        
        count_of_crashes.write.mode('overwrite').option('header',True).format('csv').save(self.output_path_1)
    
    def analysis_2(self): 
        df_join = self.df_units.join(self.df_charges,on=['CRASH_ID','UNIT_NBR'],how='inner')
        count_of_two_wheelers_charged = df_join.filter(col('VEH_BODY_STYL_ID').isin(['MOTORCYCLE','POLICE MOTORCYCLE'])).select('CRASH_ID','UNIT_NBR').distinct().count()
        
        new_schema = StructType([StructField('COUNT_OF_TWO_WHEELERS_CHARGED',IntegerType(),True)])
        count_of_two_wheelers_charged = spark.createDataFrame([[count_of_two_wheelers_charged]],new_schema)
        
        count_of_two_wheelers_charged.write.mode('overwrite').format('csv').option('header',True).save(self.output_path_2)

    def analysis_3(self):
        df_highest_accident_state = self.df_person_use.where(col('PRSN_GNDR_ID') == 'FEMALE')                                                 .groupby('DRVR_LIC_STATE_ID').count()                                                 .orderBy(col('count').desc())
        df_highest_accident_state = df_highest_accident_state.withColumnRenamed('count','STATE_WITH_MAX_ACCIDENTS_INVOLVING_FEMALE_ONLY')
        df_highest_accident_state = df_highest_accident_state.show(1)
        
        df_highest_accident_state.write.mode('overwrite').format('csv').option('header',True).save(self.output_path_3)
        
    def analysis_4(self):
        df_units_2 = self.df_units.withColumn('TOTAL_INJURIES', df_units.TOT_INJRY_CNT + df_units.DEATH_CNT)
        df_veh_make_id = df_units_2.groupby('VEH_MAKE_ID').agg(sum('TOTAL_INJURIES').alias('TOTAL_INJURIES'))
        
        window_spec = Window.orderBy(col('TOTAL_INJURIES').desc())
        df_veh_make_id = df_veh_make_id.withColumn('id_number',row_number().over(window_spec))
        df_veh_make_id = df_veh_make_id.where((col('id_number')>=5) & (col('id_number') <=15)).select('VEH_MAKE_ID')
        
        df_veh_make_id.write.mode('overwrite').format('csv').option('header',True).save(self.output_path_4)
    
    def analysis_5(self):
        df_person_use_2 = self.df_person_use.select('CRASH_ID','UNIT_NBR','PRSN_ETHNICITY_ID')
        df_units_2 = self.df_units.select('CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID')
        df_join_person_units = df_units_2.join(df_person_use_2,on=['CRASH_ID','UNIT_NBR'],how='left')
        df_ethnic_group_vs_body_style = df_join_person_units.groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').count()
        
        window_spec = Window.partitionBy(col('VEH_BODY_STYL_ID')).orderBy(col('count').desc())
        df_ethnic_group_vs_body_style = df_ethnic_group_vs_body_style.withColumn('dense_rank',dense_rank().over(window_spec))
        df_ethnic_group_vs_body_style = df_ethnic_group_vs_body_style.where(col('dense_rank')==1).select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')
        
        df_ethnic_group_vs_body_style.write.mode('overwrite').format('csv').option('header',True).save(self.output_path_5)

    def analysis_6(self):
        df_units_cars = self.df_units.where((col('VEH_BODY_STYL_ID')                                     .isin(['PASSENGER CAR, 4-DOOR','SPORT UTILITY VEHICLE','PASSENGER CAR','2-DOOR','VAN','POLICE CAR/TRUCK']))                                            & ((col('CONTRIB_FACTR_1_ID') == 'UNDER INFLUENCE - ALCOHOL')                                            | (col('CONTRIB_FACTR_2_ID') == 'UNDER INFLUENCE - ALCOHOL')                                            | (col('CONTRIB_FACTR_P1_ID') == 'UNDER INFLUENCE - ALCOHOL')))
        df_units_cars_2 = df_units_cars.select('CRASH_ID','UNIT_NBR')
        df_person_2 = self.df_person_use.select('CRASH_ID','UNIT_NBR','DRVR_ZIP')
        
        df_join = df_person_2.join(df_units_cars_2, on=['CRASH_ID','UNIT_NBR'],how='inner').distinct()
        df_top_5_drvr_zip = df_join.groupBy('DRVR_ZIP').count()                                                       .orderBy(col('count').desc()).where(col('DRVR_ZIP').isNotNull())

        df_top_5_drvr_zip.write.mode('overwrite').format('csv').option('header',True).save(self.output_path_6)
        
    def analysis_7(self):
        df_nodamage = self.df_units.join(self.df_damages,on=['CRASH_ID'],how='left_anti')
        df_nodamage = df_nodamage.select('CRASH_ID','FIN_RESP_TYPE_ID','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID')
        df_nodamage = df_nodamage.where(df_nodamage.FIN_RESP_TYPE_ID.isNotNull())
        df_nodamage = df_nodamage.withColumn('VEH_DMAG_SCL_1_ID',regexp_extract(col('VEH_DMAG_SCL_1_ID'),"[0-9]",0))                                 .withColumn('VEH_DMAG_SCL_2_ID',regexp_extract(col('VEH_DMAG_SCL_2_ID'),"[0-9]",0))
        
        df_nodamage = df_nodamage.na.fill(value=0,subset=['VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID'])
        df_nodamage = df_nodamage.where((df_nodamage.VEH_DMAG_SCL_1_ID > 4) | (df_nodamage.VEH_DMAG_SCL_2_ID > 4))
        df_nodamage = df_nodamage.select('CRASH_ID').distinct().count()
        
        new_schema=StructType([StructField('COUNT_OF_DISTINCT_CRASHES',IntegerType(),True)])
        df_nodamage = spark.createDataFrame([[df_nodamage]],schema=new_schema)
        
        df_nodamage.write.mode('overwrite').format('csv').option('header',True).save(self.output_path_7)

    def analysis_8(self):
        df_units_merge_charges = self.df_units.join(self.df_charges,on=['CRASH_ID','UNIT_NBR'],how='inner')
        
        state_top25 = df_units_merge_charges.groupBy('VEH_LIC_STATE_ID').count().orderBy(col('count').desc()).collect()[:25]
        top25_state_list=[]
        i=0
        while 0<=i<25:
            top25_state_list.append(state_top25[i][0])
            i += 1
        
        color_top10 = df_units_merge_charges.groupBy('VEH_COLOR_ID').count().orderBy(col('count').desc()).collect()[:10]
        top10_color_veh_list = []
        j=0
        while 0<=j<10:
            top10_color_veh_list.append(color_top10[j][0])
            j += 1
        
        df_units_merge_charges = df_units_merge_charges.filter(((col('VEH_BODY_STYL_ID').contains('car')) | (col('VEH_BODY_STYL_ID').contains('CAR')))                                                            & ((col('CHARGE').contains('SPEED')) | (col('CHARGE').contains('speed')))                                                            & (df_units_merge_charges.VEH_LIC_STATE_ID.isNotNull())).distinct()
        
        df_charges_2 = self.df_charges.filter((self.df_charges.CHARGE.contains('SPEED')) | (self.df_charges.CHARGE.contains('speed'))).distinct()
        df_units_merge_charges = df_units_merge_charges.join(df_charges_2,on=['CRASH_ID','UNIT_NBR'],how='inner')
        
        df_person = self.df_person_use.filter(col('DRVR_LIC_TYPE_ID').isin(['DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'])).select('CRASH_ID','UNIT_NBR').distinct()
        df_units_merge_charges = df_units_merge_charges.join(df_person,on=['CRASH_ID','UNIT_NBR'],how='inner')
        
        df_veh_top25_state_id = df_units_merge_charges.filter(col('VEH_LIC_STATE_ID').isin(top25_state_list))
        df_veh_top10_color_and_top25_state_id = df_veh_top25_state_id.filter(col('VEH_COLOR_ID').isin(top10_color_veh_list))

        df_veh_top10_color_and_top25_state_id = df_veh_top10_color_and_top25_state_id.groupby('VEH_MAKE_ID').count().orderBy(col('count').desc()).show(5)
        
        df_veh_top10_color_and_top25_state_id.write.mode('overwrite').format('csv').option('header',True).save(self.output_path_8)


# In[392]:


obj_analysis_1 = main().analysis_1()


# In[345]:


obj_analysis_2 = main().analysis_2()


# In[ ]:


obj_analysis_3 = main().analysis_3()


# In[ ]:


obj_analysis_4 = main().analysis_4()


# In[ ]:


obj_analysis_5 = main().analysis_5()


# In[ ]:


obj_analysis_6 = main().analysis_6()


# In[ ]:


obj_analysis_7 = main().analysis_7()


# In[ ]:


obj_analysis_8 = main().analysis_8()

