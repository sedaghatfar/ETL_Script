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
Query = """SELECT MAX(updated_at) FROM salesloft.emails; """

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

email_df = pd.DataFrame(columns=['id','created_at','updated_at','recipient_email_address','status','bounced','sent_at','counts.clicks','counts.views','counts.replies','user.id','recipient.id','cadence.id'])

#email full df

while page_size > 99:
    url = "https://api.salesloft.com/v2/activities/emails.json?&include_paging_counts=true&sort=updated_at&sort_direction=ASC&updated_at[gt]={cursor}&per_page={page_size}".format(cursor=max_update, page_size=page_size)
    response = requests.request("GET", url, headers=headers, data = payload)

    emails = json.loads(response.text.encode('utf8'))
    temp_df = pd.json_normalize(emails['data'])
    temp_df = temp_df[['id','created_at','updated_at','recipient_email_address','status','bounced','sent_at','counts.clicks','counts.views','counts.replies','user.id','recipient.id','cadence.id']]
    email_df = email_df.append(temp_df, ignore_index=True)

    max_update = max(email_df['updated_at'])
    page_size = len(temp_df.index)
    time.sleep(1)

#rename
email_df.rename({'counts.clicks': 'clicks','counts.views': 'views','counts.replies': 'replies','user.id': 'user_id','recipient.id':'recipient_id','cadence.id':'cadence_id'}, axis=1, inplace=True)

#replace NaN
email_df = email_df.replace({np.nan:'NULL'})
email_df['sent'] =np.where((email_df['sent_at']!='NULL'), 1, 0)

# To CSVs

email_df.to_csv('/home/matts/Documents/new_emails.csv',sep='|', index = False)

