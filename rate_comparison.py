# -*- coding: utf-8 -*-
"""
Rate Comparison

Created on Tue Jul 25 09:41:07 2023

@author: rvackner
"""

import pyodbc
import pandas as pd
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta
from calendar import monthrange

# get todays date
today = date.today()
year_ago = today - relativedelta(months=13)

# get interval readings data
cnxncis = pyodbc.connect('DSN=XXXXX;PWD=XXXXX')
df_rdg = pd.read_sql_query(f"SELECT BI_ACCT, BI_RATE_SCHED, BI_USAGE, BI_DMD_RDG, BI_MTR_MULT, BI_PRES_READ_DT, BI_MTR_NBR, BI_SRV_LOC_NBR, BI_RDG_ACT_CD, BI_NET_METER_SW FROM XXXXX.XXXXX WHERE (BI_PRES_READ_DT >= TO_DATE('{year_ago}', 'yyyy-mm-dd')) AND (BI_PRES_READ_DT <= TO_DATE('{today}', 'yyyy-mm-dd'))", cnxncis)
df_mtr_inv = pd.read_sql_query(f"SELECT BI_MTR_NBR, BI_MTR_FORM_NBR FROM XXXXX.XXXXX", cnxncis)
df_srv_loc = pd.read_sql_query(f"SELECT BI_SRV_LOC_NBR, BI_ADDR1, BI_SRV_DESC FROM XXXXX.XXXXX", cnxncis)
df_consumer = pd.read_sql_query(f"SELECT BI_VWN_CO_ACCT AS BI_ACCT, BI_FNAME, BI_LNAME, BI_CYC_CD FROM XXXXX.XXXXX", cnxncis)

# merge mtr form nbr to the df_rdg
df_rdg = df_rdg.merge(df_mtr_inv, how='left', on='BI_MTR_NBR')
df_rdg = df_rdg.merge(df_srv_loc, how='left', on='BI_SRV_LOC_NBR')
df_rdg = df_rdg.merge(df_consumer, how='left', on='BI_ACCT')

# getting the correct demand (using the multiplier)
df_rdg["BI_DMD_RDG"] = df_rdg["BI_DMD_RDG"]*df_rdg["BI_MTR_MULT"]

# fixing duplicate dates and accounts
df_rdg = df_rdg.groupby(['BI_ACCT', 'BI_RATE_SCHED', 'BI_MTR_FORM_NBR', 'BI_NET_METER_SW', 'BI_PRES_READ_DT', 'BI_MTR_MULT', 'BI_ADDR1', 'BI_FNAME', 'BI_LNAME', 'BI_SRV_DESC', 'BI_CYC_CD', 'BI_RDG_ACT_CD'], dropna=False)[['BI_USAGE', 'BI_DMD_RDG']].max().reset_index()

# get the number of days in billing cycle
df_rdg['days'] = df_rdg['BI_PRES_READ_DT'].apply(lambda t: pd.Period(t, freq='S').days_in_month)

# get the rate sched
general_service = ['2', '5', '9', '26']
church = ['6']
residential = ['3', '4', '8', '33', '34']
general_service_pp = ['19']
residential_pp = ['13', '20']
three_phase_meters = ['9S', '16S', '45S']

# find the old rate cost
df_rdg['old_rate'] = np.where(df_rdg['BI_RATE_SCHED'].isin(general_service), round((df_rdg['BI_USAGE']*.128) + (df_rdg['days']*.8),2), \
                     np.where(df_rdg['BI_RATE_SCHED'].isin(church), round((df_rdg['BI_USAGE']*.128) + (df_rdg['days']*.8),2), \
                     np.where(df_rdg['BI_RATE_SCHED'].isin(residential), round((df_rdg['BI_USAGE']*.125) + (df_rdg['days']*.8),2), \
                     np.where(df_rdg['BI_RATE_SCHED'].isin(general_service_pp), round((df_rdg['BI_USAGE']*.128) + (df_rdg['days']*1.03),2), \
                     np.where(df_rdg['BI_RATE_SCHED'].isin(residential_pp), round((df_rdg['BI_USAGE']*.125) + (df_rdg['days']*1.03),2), None)))))

# find the new rate cost
df_rdg['new_rate'] = np.where(df_rdg['BI_RATE_SCHED'].isin(general_service), round((df_rdg['BI_USAGE']*.0725) + (df_rdg['days']*.8) + (df_rdg['BI_DMD_RDG']*12),2), \
                     np.where(df_rdg['BI_RATE_SCHED'].isin(church), round((df_rdg['BI_USAGE']*.091) + (df_rdg['days']*.8) + (df_rdg['BI_DMD_RDG']*4),2), \
                     np.where(df_rdg['BI_RATE_SCHED'].isin(residential), round((df_rdg['BI_USAGE']*.065) + (df_rdg['days']*.8) + (df_rdg['BI_DMD_RDG']*12),2), \
                     np.where(df_rdg['BI_RATE_SCHED'].isin(general_service_pp), round((df_rdg['BI_USAGE']*.0725) + (df_rdg['days']*1.03) + (df_rdg['BI_DMD_RDG']*12),2), \
                     np.where(df_rdg['BI_RATE_SCHED'].isin(residential_pp), round((df_rdg['BI_USAGE']*.065) + (df_rdg['days']*1.03),2) + (df_rdg['BI_DMD_RDG']*12), None)))))

# add three phase meter charge
df_rdg['old_rate'] = np.where(df_rdg['BI_MTR_FORM_NBR'].isin(three_phase_meters), df_rdg['old_rate']+12, df_rdg['old_rate'])
df_rdg['new_rate'] = np.where(df_rdg['BI_MTR_FORM_NBR'].isin(three_phase_meters), df_rdg['new_rate']+12, df_rdg['new_rate'])

# difference and percent difference
df_rdg['difference'] = df_rdg['new_rate'] - df_rdg['old_rate']
df_rdg['percent_difference'] = (df_rdg['difference']/df_rdg['old_rate']).apply(lambda x: round(x, 2))

# calculations for load factor
df_rdg['load_factor'] = np.where(df_rdg['BI_DMD_RDG'] != 0, round(((df_rdg['BI_USAGE']/(df_rdg['days']*24))/(df_rdg['BI_DMD_RDG'])),2), 0)

# remove the X
df_rdg = df_rdg[df_rdg['BI_RDG_ACT_CD'] != 'X']

# export to csv
df_rdg.to_csv(r"XXXXX\rate_comparison.csv", index=False)
