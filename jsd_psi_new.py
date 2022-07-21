#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import pyodbc
import openpyxl

import numpy as np
from cmlib.model_monitoring_metrics import JensenShannonDivergence as js
from cmlib.model_monitoring_metrics import PsiDivergence as psi
from cmlib.model_monitoring_metrics import Threshold


# In[2]:


#Connected to database server
conn = pyodbc.connect('''DRIVER={ODBC Driver 17 for SQL Server};Server=lbbbubble.eu.rabonet.com\ABB_PRD_01;
Database=RA_ABB_Custom;Trusted_connection=yes;''')


# In[3]:


prod_table = "[RA_ABB_Custom].[dbo].[RSME_FinalModelPrediction_Monitoring2021_200912_202106_Product_RemediatedModel_202111]"
facility_table = "[RA_ABB_Custom].[dbo].[RSME_FinalModelPrediction_Monitoring2021_200912_202106_Facility_RemediatedModel_202111]"


# In[5]:


#query from sql for AWL
data_awl = pd.read_sql(f""" select P.measurementPeriodId,
floor(P.measurementperiodID/100) as year,
P.measurementperiodID - floor(P.measurementperiodID/100)*100 as month,
F.facilityOutstanding,
P.outstandingAmount,
P.eadSegment

from {prod_table} as P

left join {facility_table} as F

on P.FacilityId=F.FacilityId and P.measurementperiodID=F.measurementperiodID


where P.defaultFlag = 0
                        and P.Isactive = 1
                        and P.CapitalBearing = 1
                        and F.Indicator_In_Scope = 1
                        and P.eadSegment = 'Account without Limit'
                        and P.measurementperiodID >= 201001 
                        and P.measurementperiodID <= 202012
order by P.measurementperiodID
 """, conn)


# In[ ]:


#query from sql for Loan
data_loan = pd.read_sql(f""" select P.measurementPeriodId,
floor(P.measurementperiodID/100) as year,
P.measurementperiodID - floor(P.measurementperiodID/100)*100 as month,
P.exposure,
F.Exposure,
P.eadSegment
from {prod_table} as P

left join {facility_table} as F

on P.FacilityId=F.FacilityId and P.measurementperiodID=F.measurementperiodID


where P.defaultFlag = 0
                        and P.Isactive = 1
                        and P.CapitalBearing = 1
                        and F.Indicator_In_Scope = 1
                        and P.eadSegment = 'Loan'
                        and P.measurementperiodID >= 201001 
                        and P.measurementperiodID <= 202012
order by P.measurementperiodID
 """, conn)


# In[11]:


#query from sql for ROPflexcredits
data_rop = pd.read_sql(f""" select P.measurementPeriodId,
floor(P.measurementperiodID/100) as year,
P.measurementperiodID - floor(P.measurementperiodID/100)*100 as month,
P.limitAmount,
P.eadSegment
from {prod_table} as P

left join {facility_table} as F

on P.FacilityId=F.FacilityId and P.measurementperiodID=F.measurementperiodID


where P.defaultFlag = 0
                        and P.Isactive = 1
                        and P.CapitalBearing = 1
                        and F.Indicator_In_Scope = 1
                        and P.eadSegment = 'ROPflexcredits'
                        and P.measurementperiodID >= 201001 
                        and P.measurementperiodID <= 202012
order by P.measurementperiodID
 """, conn)


# In[7]:


def fn_perc(df,var):
    #     perc 10 and 90
    
    perc_10_var = df[var].quantile(.1)
    perc_90_var = df[var].quantile(.9)

    div = perc_90_var - perc_10_var
    div = div / 9
    
    b_01 = perc_10_var
    b_02 = perc_10_var + div
    b_03 = b_02 + div
    b_04 = b_03 + div
    b_05 = b_04 + div
    b_06 = b_05 + div
    b_07 = b_06 + div
    b_08 = b_07 + div
    b_09 = b_08 + div
    b_10 = b_09 + div
    
    return b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10


# In[8]:


def floor_bucket(df,var1,b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10):
    
    df[f'{var1}_bucket'] = np.where(
   
        df[var1]<b_01,'B01',
        np.where((df[var1]>b_01) & (df[var1]<b_02),'B02',
                 np.where((df[var1]>b_02) & (df[var1]<b_03),'B03',
                          np.where((df[var1]>b_03) & (df[var1]<b_04),'B04',
                                   np.where((df[var1]>b_04) & (df[var1]<b_05),'B05',
                                            np.where((df[var1]>b_05) & (df[var1]<b_06),'B06',
                                                     np.where((df[var1]>b_06) & (df[var1]<b_07),'B07',
                                                              np.where((df[var1]>b_07) & (df[var1]<b_08),'B08',
                                                                       np.where((df[var1]>b_08) & (df[var1]<b_09),'B09',
                                                                                np.where((df[var1]>b_09) & (df[var1]<b_10),'B10', 'B11' ))))))))))
                                                                       
    return df
                 


# In[ ]:





# In[9]:


def fn_test(df,var1=None,var2=None):
    
    df_curr = df[(df['measurementPeriodId']>=202001) & (df['measurementPeriodId']<=202012)]
    df_prev = df[(df['measurementPeriodId']>=201901) & (df['measurementPeriodId']<=201912)]
    df_dev = df[(df['measurementPeriodId']>=201001) & (df['measurementPeriodId']<=201612)]
    
    b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10 = fn_perc(df_dev,var1)
    
        
    df_curr_buck_var1 = floor_bucket(df_curr,var1,b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10)
    df_curr_buck_var1.drop([var1], axis=1,inplace=True)
    
    df_prev_buck_var1 = floor_bucket(df_prev,var1,b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10)
    df_prev_buck_var1.drop([var1], axis=1,inplace=True)
    
    df_dev_buck_var1 = floor_bucket(df_dev,var1,b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10)
    df_dev_buck_var1.drop([var1], axis=1,inplace=True)
    
    var1_bucket = f'{var1}_bucket'
    var2_bucket = f'{var2}_bucket'
    
    # cohort bucket methodology for variable-1 for each data set
    df_dev_var1 = pd.crosstab(df_dev_buck_var1[var1_bucket].fillna('missing'), df_dev_buck_var1['month'].fillna('missing'),
                          margins=False, dropna=False, normalize='columns')    
    df_prev_var1 = pd.crosstab(df_prev_buck_var1[var1_bucket].fillna('missing'),
                           df_prev_buck_var1['month'].fillna('missing'), margins=False, dropna=False, normalize='columns')
    df_curr_var1 = pd.crosstab(df_curr_buck_var1[var1_bucket].fillna('missing'),
                           df_curr_buck_var1['month'].fillna('missing'), margins=False, dropna=False, normalize='columns')
    
    
   
    
    # implementing of JSD
    
    ## Long term calculation of JSD
    for i in range(1, 13):
        globals()['outcome_' + str(i)] = js(
        actual_model=np.array(df_curr_var1.iloc[:, i - 1].to_numpy()),
            reference_model=np.array(df_dev_var1.iloc[:, i - 1].to_numpy()),
            bucket_names=["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"],
            thresholds={Threshold.GREEN: 0.001, Threshold.RED: 0.025},
            ).compare()

        globals()['outcome_teststat_' + str(i)] = globals()['outcome_' + str(i)].overall_divergence
        
    jsd_var1_long = np.mean((
        outcome_teststat_1, outcome_teststat_2, outcome_teststat_3, outcome_teststat_4, outcome_teststat_5, outcome_teststat_6,
        outcome_teststat_7, outcome_teststat_8, outcome_teststat_9, outcome_teststat_10, outcome_teststat_11,
        outcome_teststat_12))
    
    
    ## Short term  calculation of JSD
    for i in range(1, 13):
        globals()['outcome_' + str(i)] = js(
        actual_model=np.array(df_curr_var1.iloc[:, i - 1].to_numpy()),
            reference_model=np.array(df_prev_var1.iloc[:, i - 1].to_numpy()),
            bucket_names=["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"],
            thresholds={Threshold.GREEN: 0.001, Threshold.RED: 0.025},
            ).compare()

        globals()['outcome_teststat_' + str(i)] = globals()['outcome_' + str(i)].overall_divergence
        
    jsd_var1_short = np.mean((
        outcome_teststat_1, outcome_teststat_2, outcome_teststat_3, outcome_teststat_4, outcome_teststat_5, outcome_teststat_6,
        outcome_teststat_7, outcome_teststat_8, outcome_teststat_9, outcome_teststat_10, outcome_teststat_11,
        outcome_teststat_12))
       
    
    ## long term calc PSI for var-1
    for i in range(1, 13):
        globals()['outcome_' + str(i)] = psi(
        actual_model=np.array(df_curr_var1.iloc[:, i - 1].to_numpy()),
            reference_model=np.array(df_dev_var1.iloc[:, i - 1].to_numpy()),
            bucket_names=["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"],
            thresholds={Threshold.GREEN: 0.001, Threshold.RED: 0.025},
            epsilon=0.0001,
            ).compare()

        globals()['outcome_teststat_' + str(i)] = globals()['outcome_' + str(i)].overall_divergence
        
    psi_var1_long = np.mean((
        outcome_teststat_1, outcome_teststat_2, outcome_teststat_3, outcome_teststat_4, outcome_teststat_5, outcome_teststat_6,
        outcome_teststat_7, outcome_teststat_8, outcome_teststat_9, outcome_teststat_10, outcome_teststat_11,
        outcome_teststat_12))
    
    ## Short term  calculation of JSD
    for i in range(1, 13):
        globals()['outcome_' + str(i)] = psi(
        actual_model=np.array(df_curr_var1.iloc[:, i - 1].to_numpy()),
            reference_model=np.array(df_prev_var1.iloc[:, i - 1].to_numpy()),
            bucket_names=["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"],
            thresholds={Threshold.GREEN: 0.001, Threshold.RED: 0.025},
            ).compare()

        globals()['outcome_teststat_' + str(i)] = globals()['outcome_' + str(i)].overall_divergence
        
    psi_var1_short = np.mean((
        outcome_teststat_1, outcome_teststat_2, outcome_teststat_3, outcome_teststat_4, outcome_teststat_5, outcome_teststat_6,
        outcome_teststat_7, outcome_teststat_8, outcome_teststat_9, outcome_teststat_10, outcome_teststat_11,
        outcome_teststat_12))
    
    
    
     # collecing the output in a df
    dic = {
        'Segment':df.eadSegment.unique(),
        "variable" :var1,
        "JSD_longTerm":jsd_var1_long,
        "JSD_shortTerm":jsd_var1_short,
        "PSI_longTerm":psi_var1_long,
        "PSI_shortTerm":psi_var1_short }
    df_all_results = pd.DataFrame(dic, index=[0])
    
    
    
    
    ######################## calculation for variable -2  ##################################
    if var2 is not None:
        print('if var 2 worked... !!')
        b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10 = fn_perc(df_dev,var2)
        
        
        df_curr_buck_var2 = floor_bucket(df_curr,var2,b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10)
        df_curr_buck_var1.drop([var2], axis=1,inplace=True)
        
        df_prev_buck_var2 = floor_bucket(df_prev,var2,b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10)
        df_prev_buck_var2.drop([var2], axis=1,inplace=True)
        
        df_dev_buck_var2 = floor_bucket(df_dev,var2,b_01,b_02,b_03,b_04,b_05,b_06,b_07,b_08,b_09,b_10)
        df_dev_buck_var2.drop([var2], axis=1,inplace=True)
        
        # cohort bucket methodology for variable-1 for each data set
        
        df_dev_var2 = pd.crosstab(df_dev_buck_var2[var2_bucket].fillna('missing'), df_dev_buck_var2['month'].fillna('missing'),
                          margins=False, dropna=False, normalize='columns')
        df_prev_var2 = pd.crosstab(df_prev_buck_var2[var2_bucket].fillna('missing'),
                           df_prev_buck_var2['month'].fillna('missing'), margins=False, dropna=False, normalize='columns')
        df_curr_var2 = pd.crosstab(df_curr_buck_var2[var2_bucket].fillna('missing'),
                           df_curr_buck_var2['month'].fillna('missing'), margins=False, dropna=False, normalize='columns')
                
        
        ######## implementing of JSD and PSI for var-2   ################

        ## Long term calculation of JSD for variable -2
        for i in range(1, 13):
            globals()['outcome_' + str(i)] = js(
            actual_model=np.array(df_curr_var2.iloc[:, i - 1].to_numpy()),
                reference_model=np.array(df_dev_var2.iloc[:, i - 1].to_numpy()),
                bucket_names=["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"],
                thresholds={Threshold.GREEN: 0.001, Threshold.RED: 0.025},
                ).compare()

            globals()['outcome_teststat_' + str(i)] = globals()['outcome_' + str(i)].overall_divergence

        jsd_var2_long = np.mean((
            outcome_teststat_1, outcome_teststat_2, outcome_teststat_3, outcome_teststat_4, outcome_teststat_5, outcome_teststat_6,
            outcome_teststat_7, outcome_teststat_8, outcome_teststat_9, outcome_teststat_10, outcome_teststat_11,
            outcome_teststat_12))


        ## Short term  calculation of JSD for var-2
        for i in range(1, 13):
            globals()['outcome_' + str(i)] = js(
            actual_model=np.array(df_curr_var2.iloc[:, i - 1].to_numpy()),
                reference_model=np.array(df_prev_var2.iloc[:, i - 1].to_numpy()),
                bucket_names=["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"],
                thresholds={Threshold.GREEN: 0.001, Threshold.RED: 0.025},
                ).compare()

            globals()['outcome_teststat_' + str(i)] = globals()['outcome_' + str(i)].overall_divergence

        jsd_var2_short = np.mean((
            outcome_teststat_1, outcome_teststat_2, outcome_teststat_3, outcome_teststat_4, outcome_teststat_5, outcome_teststat_6,
            outcome_teststat_7, outcome_teststat_8, outcome_teststat_9, outcome_teststat_10, outcome_teststat_11,
            outcome_teststat_12))
        
        
         ## long term calc PSI for var-1
        for i in range(1, 13):
            globals()['outcome_' + str(i)] = psi(
            actual_model=np.array(df_curr_var2.iloc[:, i - 1].to_numpy()),
                reference_model=np.array(df_dev_var2.iloc[:, i - 1].to_numpy()),
                bucket_names=["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"],
                thresholds={Threshold.GREEN: 0.001, Threshold.RED: 0.025},
                epsilon=0.0001,
                ).compare()

            globals()['outcome_teststat_' + str(i)] = globals()['outcome_' + str(i)].overall_divergence

        psi_var2_long = np.mean((
            outcome_teststat_1, outcome_teststat_2, outcome_teststat_3, outcome_teststat_4, outcome_teststat_5, outcome_teststat_6,
            outcome_teststat_7, outcome_teststat_8, outcome_teststat_9, outcome_teststat_10, outcome_teststat_11,
            outcome_teststat_12))

        ## Short term  calculation of JSD
        for i in range(1, 13):
            globals()['outcome_' + str(i)] = psi(
            actual_model=np.array(df_curr_var2.iloc[:, i - 1].to_numpy()),
                reference_model=np.array(df_prev_var2.iloc[:, i - 1].to_numpy()),
                bucket_names=["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", "B10", "B11"],
                thresholds={Threshold.GREEN: 0.001, Threshold.RED: 0.025},
                ).compare()

            globals()['outcome_teststat_' + str(i)] = globals()['outcome_' + str(i)].overall_divergence

        psi_var2_short = np.mean((
            outcome_teststat_1, outcome_teststat_2, outcome_teststat_3, outcome_teststat_4, outcome_teststat_5, outcome_teststat_6,
            outcome_teststat_7, outcome_teststat_8, outcome_teststat_9, outcome_teststat_10, outcome_teststat_11,
            outcome_teststat_12))
        
         # collecing the output in a df
        dic = {
            'Segment':df.eadSegment.unique(),
            "variable" :var2,
            "JSD_longTerm":jsd_var2_long,
            "JSD_shortTerm":jsd_var2_short,
            "PSI_longTerm":psi_var2_long,
            "PSI_shortTerm":psi_var2_short }
        df_result = pd.DataFrame(dic, index=[0])
        df_all_results = pd.concat([df_all_results,df_result])
           
    return df_all_results


# In[13]:


fn_test(data_awl,'facilityOutstanding','outstandingAmount')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# def fn_run_segments():
#     df_all_results = fn_test(data_awl,'facilityOutstanding','outstandingAmount')
    


# In[ ]:





# In[ ]:




