import json
import pandas as pd
import numpy as np
import re
import pathlib
from pathlib import Path
import sys
from datetime import datetime
import argparse


#initial vars
counter=0
x=0  

# get arguments from user 
parser = argparse.ArgumentParser()
parser.add_argument("path", help = "Enter path of the source") 
parser.add_argument("-d", "--dest", help = "Enter path of the destination", required = False)
parser.add_argument("-u", action="store_true", dest="unix", default=False, help="unix time converter")

start_time = datetime.now()

# checking user arguments
args = parser.parse_args()
directory=args.path

if args.unix:
    x='-u'
    
if args.dest:
    destination=args.dest
else:
    destination=directory
    

files = Path(directory).glob('**/*.json')
for file in files:
        records = [json.loads(line) for line in open(file)]
        trans_data = pd.DataFrame(records)
        
        # help in transformation step
        trans_data['ll'] = trans_data['ll'].fillna(0)
        
        # url function for getting url
        def get_url(str1):
            if str1.isalpha():
                return str1
            else:
                return str1[str1.find("/") + 2:str1.find("/", str1.find("/") + 2)]
        
        # time converting functions
        def time_format_to_stamp():
            trans_data['time_in'] = pd.to_datetime(trans_data['t'], unit='s')
            trans_data['time_out'] = trans_data['hc'].apply(
                lambda x: pd.to_datetime(str(x)[:10], unit='s', errors='ignore'))

        def time_format_unix():
            trans_data['time_in'] = trans_data['t']
            trans_data['time_out'] = trans_data['hc']

        
        # transformation 
        trans_data['web_browser'] = trans_data['a'].apply(lambda x: x[0:x.find("/")])

        trans_data['operating_sys'] = trans_data['a'].apply(
            lambda x: re.findall("[\dA-Za-z/]*", x[x.find("(") + 1:x.find(";")])[0]  if any(y in x for y in ['(']) else np.nan  )

        trans_data['from_url'] = trans_data['r'].apply(lambda x: get_url(x))
        trans_data['to_url'] = trans_data['u'].apply(lambda x: get_url(x))

        trans_data['city'] = trans_data['cy']

        trans_data['longitude'] = trans_data['ll'].apply(lambda val: str(val)[1:str(val).find(',')])
        trans_data['latitude'] = trans_data['ll'].apply(lambda val: str(val)[str(val).find(',') + 1:-1])

        trans_data['time_zone'] = trans_data['tz']


        # checking needed time format based on user option 
        if x == '-u':
            time_format_unix()
        else:
            time_format_to_stamp()

        
        # final DataFrame
        final_df = trans_data[['web_browser', 'operating_sys', 'from_url',
                               'to_url', 'city', 'longitude', 'latitude', 'time_zone', 'time_in', 'time_out']]

        # Replacing all NANs
        final_df=final_df.replace('',np.nan)
        final_df['longitude'].fillna('0',inplace=True)
        final_df['latitude'].fillna('0',inplace=True)
        final_df['time_zone'].fillna('unknown', inplace=True)
        final_df['city'].fillna('unknown', inplace=True)
        final_df['operating_sys'].fillna('unknown', inplace=True)

        
        # exporting file as CSV
        final_df.to_csv(destination +r"\ ".strip()+ 'file' +str(counter)+ '.csv')
        
        # file characteristics
        print('total transformed rows is :' + str(len(final_df)))
        print('location of file : ' + destination +r"\ ".strip()+ 'file' +str(counter)+ '.csv' )
        counter +=1

# execution time 
end_time = datetime.now()
print('job has been done successfully, total execution time is: {}'.format(end_time - start_time))
