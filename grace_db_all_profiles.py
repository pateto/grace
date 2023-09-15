# -*- coding: utf-8 -*-
"""
Created on Tue Aug  1 13:32:49 2023

@author: luby_ka
"""


# import cdflib
import numpy as np
import pandas as pd
import glob
from datetime import datetime
import sqlite3
import xarray as xr


import sqlite3


# Create the database
connection = sqlite3.connect(r'S:\IGS TEC data\GRACE_availability\GRACE_all_prof_cut_new_3.db')
cursor = connection.cursor() 


 

cursor.execute('create table grace_prof_north_all(\
               date_grace_start timestamp, date_grace_end timestamp,\
             mlt_start real, mlt_end real, apex_lat_start real, apex_lat_end real, lat_igrf_start real, lat_igrf_end real )')
                   

connection.commit()
 #%%        
connection.close()   
    #%%                    


years = [ 2002, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015] # 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015


connection = sqlite3.connect(r'S:\IGS TEC data\GRACE_availability\GRACE_all_prof_cut_new_3.db')
cursor = connection.cursor() 
rows2insert = []
for year in years:

    path = r'S:\Grace_data\40_70_Northern\{}_igrf_48_3\*.nc'.format(year) #KBRNE_2014-01-01_8  #_24run_av
    for file in glob.glob(path):
        print('We are reading: ', file)
        ds = xr.open_dataset(file)
        df = ds.to_dataframe()
        
        df_originanal = df.iloc[1:-1]
    
        
        df = df_originanal.sort_values('lat_igrf', ascending=False, ignore_index=True) # ascending=True
        

        df = df[(df['Ne_runev_48'] >= 0)]
        df['ut'] = df['Datetime'] 
        
        
        
        df = df.reset_index(drop=True)
        

        
        # try:
            
        df = df.drop(df[df.Ne < 0].index).reset_index(drop=True)
        

        if not (df.Ne.values < 0).any() and not df.empty:
            dictionary_data = {
                        
                        'date_grace_start': str(df['Datetime'].iloc[0]), # %H:%M:%S
                        'date_grace_end': str(df['Datetime'].iloc[-1]), # %H:%M:%S
                        'mlt_start': df['mlt'].iloc[0], # %H:%M:%S, 
                        'mlt_end':df['mlt'].iloc[-1],
                        'apex_lat_start': df['apex_lat'].iloc[0], # %H:%M:%S, 
                        'apex_lat_end':df['apex_lat'].iloc[-1],
                        'lat_igrf_start': df['lat_igrf'].iloc[0], # %H:%M:%S, 
                        'lat_igrf_end':df['lat_igrf'].iloc[-1],
                        
                        }
            print(file)


            rows2insert.append(list(dictionary_data.values()))
 #%%            

    # qmarks = ', '.join('?' * len(rows2insert[0]))           
    # cursor.executemany("INSERT INTO new_trough4 VALUES ({})".format(qmarks), rows2insert)
                        
    # connection.commit()
    # connection.close()   
    # print('Finished reading: ', file)
    qmarks = ', '.join('?' * len(rows2insert[0]))
    cursor.executemany("INSERT INTO grace_prof_north_all VALUES ({})".format(qmarks), rows2insert)
    connection.commit()
 #%%
connection.close()  