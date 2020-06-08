import requests
import json
import pandas as pd
import numpy as np
import datetime
import time
import psycopg2
import config

#first get max time from datebase
conn = psycopg2.connect(config.placepass)
cur = conn.cursor()
Query = """SELECT MAX(updated_at) FROM salesloft.people; """

cur.execute(Query)
max_update = cur.fetchone()

cur.close()

# get new def
max_update = max_update[0].date()
#Set initial size
page_size = 100

#API Request

payload = {}
headers = {
'Authorization': config.saleskey
}

ppl_df = pd.DataFrame(columns=['id','created_at','updated_at','last_contacted_at','display_name','email_address','phone','do_not_contact','tags','counts.emails_sent','counts.emails_viewed','counts.emails_replied_to','counts.calls','owner.id'])

#email full df

while page_size > 99:
    url = "https://api.salesloft.com/v2/people.json?&sort=updated_at&sort_direction=ASC&updated_at[gt]={cursor}&per_page={page_size}".format(cursor=max_update, page_size=page_size)
    response = requests.request("GET", url, headers=headers, data = payload)

    emails = json.loads(response.text.encode('utf8'))
    temp_df = pd.json_normalize(emails['data'])
    temp_df = temp_df[['id','created_at','updated_at','last_contacted_at','display_name','email_address','phone','do_not_contact','tags','counts.emails_sent','counts.emails_viewed','counts.emails_replied_to','counts.calls','owner.id']]
    ppl_df = ppl_df.append(temp_df, ignore_index=True)

    max_update = max(ppl_df['updated_at'])
    page_size = len(temp_df.index)
    time.sleep(1)

#rename
ppl_df.rename({'counts.emails_sent': 'emails_sent','counts.emails_viewed': 'emails_viewed','counts.emails_replied_to': 'emails_replied_to','counts.calls': 'calls','owner.id':'owner_id'}, axis=1, inplace=True)

#replace NaN
ppl_df = ppl_df.replace({np.nan:'NULL'})
ppl_df = ppl_df.replace(r'\n',' ', regex=True)

# To CSVs

ppl_df.to_csv('/home/matts/Documents/new_ppl.csv',sep='|', index = False)
