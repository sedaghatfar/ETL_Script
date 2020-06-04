import requests
import json
import pandas as pd
import numpy as np
import time
import config
import psycopg2


#found cadence_id from /v2/cadences.json

#interest cadence
mem_base = "https://api.salesloft.com/v2/cadence_memberships.json?cadence_id=80025&include_paging_counts=true&per_page=100&page="

payload = {}
headers = {
  'Authorization': config.saleskey
}

#interest cadence

response = requests.request("GET", mem_base+str(1), headers=headers, data = payload)

mem = json.loads(response.text.encode('utf8'))

mem_df = pd.json_normalize(mem['data'])
mem_df = mem_df[['id', 'added_at', 'created_at', 'updated_at', 'person.id','person_deleted','user.id','currently_on_cadence', 'current_state','counts.replies','counts.calls', 'counts.sent_emails', 'counts.bounces']]

total_pages = mem['metadata']['paging']['total_pages']

for pagenumber in range(2, total_pages+1):

    response = requests.request("GET", mem_base+str(pagenumber), headers=headers, data = payload)
    mem = json.loads(response.text.encode('utf8'))
    temp_df = pd.json_normalize(mem['data'])

    temp_df = temp_df[['id', 'added_at', 'created_at', 'updated_at', 'person.id','person_deleted','user.id','currently_on_cadence', 'current_state','counts.replies','counts.calls', 'counts.sent_emails', 'counts.bounces']]
    mem_df = mem_df.append(temp_df, ignore_index=True)
    time.sleep(1)

#Can do this all in SQL, tested in python first
mem_df['updated_at'] = pd.to_datetime(mem_df['updated_at'], infer_datetime_format=True).dt.strftime('%Y-%m-%d')
mem_df['recent'] = np.where(mem_df['updated_at'] > str((pd.to_datetime('today') - pd.Timedelta(days=14)).strftime('%Y-%m-%d')), 1, 0)
mem_df['points'] = np.where((mem_df['recent']==1) & (mem_df['current_state']=='active'), 1, 0)

# change columns

mem_df.rename({'person.id':'person_id','user.id':'user_id','counts.replies': 'replies','counts.calls': 'calls','counts.sent_emails': 'sent_emails','counts.bounces': 'bounces'}, axis=1, inplace=True)

#replace NaN

mem_df = mem_df.replace({np.nan: 'NULL'})

# To CSVs

mem_df.to_csv('/home/matts/Documents/interest.csv',sep='|', index = False)
