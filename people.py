#!/usr/bin/python3
import requests
import json
import pandas as pd
import numpy as np
import time
import config
import psycopg2

ppl_base = "https://api.salesloft.com/v2/people.json?per_page=100&include_paging_counts=true&page="

payload = {}
headers = {
  'Authorization': config.saleskey
}


# people total pages

peopleurlresponse = requests.request("GET", ppl_base+str(1), headers=headers, data = payload)

people = json.loads(peopleurlresponse.text.encode('utf8'))

ppl_pages = people['metadata']['paging']['total_pages']

people_df = pd.json_normalize(people['data'])
people_df = people_df[['id','created_at','updated_at','last_contacted_at','display_name','email_address','phone','do_not_contact','tags','counts.emails_sent','counts.emails_viewed','counts.emails_replied_to','counts.calls','owner.id']]

#people full df

for pagenumber in range(2, ppl_pages+1):
    response = requests.request("GET", ppl_base+str(pagenumber), headers=headers, data = payload)
    ppl = json.loads(response.text.encode('utf8'))
    temp_df = pd.json_normalize(ppl['data'])

    temp_df = temp_df[['id','created_at','updated_at','last_contacted_at','display_name','email_address','phone','do_not_contact','tags','counts.emails_sent','counts.emails_viewed','counts.emails_replied_to','counts.calls','owner.id']]
    people_df = people_df.append(temp_df, ignore_index=True)
    time.sleep(1)


people_df.rename({'counts.emails_sent': 'emails_sent','counts.emails_viewed': 'emails_viewed','counts.emails_replied_to': 'emails_replied_to','counts.calls': 'calls','owner.id':'owner_id'}, axis=1, inplace=True)

people_df = people_df.replace({np.nan: 'NULL'})
people_df = people_df.replace(r'\n',' ', regex=True)

# To CSVs

people_df.to_csv('/home/matts/Documents/people.csv',sep='|', index = False)
