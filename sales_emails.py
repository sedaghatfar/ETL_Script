import requests
import json
import pandas as pd
import numpy as np
import time
import config
import psycopg2

email_base = "https://api.salesloft.com/v2/activities/emails.json?per_page=100&include_paging_counts=true&page="

payload = {}
headers = {
'Authorization': config.saleskey
}

#email total pages
response = requests.request("GET", email_base+str(1), headers=headers, data = payload)

emails = json.loads(response.text.encode('utf8'))
total_pages = emails['metadata']['paging']['total_pages']

email_df = pd.json_normalize(emails['data'])
email_df = email_df[['id','created_at','recipient_email_address','status','bounced','sent_at','counts.clicks','counts.views','counts.replies','user.id','recipient.id','cadence.id']]


#email full df

for pagenumber in range(2, total_pages+1):
    response = requests.request("GET", email_base+str(pagenumber), headers=headers, data = payload)
    emails = json.loads(response.text.encode('utf8'))
    temp_df = pd.json_normalize(emails['data'])
    temp_df = temp_df[['id','created_at','recipient_email_address','status','bounced','sent_at','counts.clicks','counts.views','counts.replies','user.id','recipient.id','cadence.id']]
    email_df = email_df.append(temp_df, ignore_index=True)
    time.sleep(1)


#rename
email_df.rename({'counts.clicks': 'clicks','counts.views': 'views','counts.replies': 'replies','user.id': 'user_id','recipient.id':'recipient_id','cadence.id':'cadence_id'}, axis=1, inplace=True)

#replace NaN
email_df = email_df.replace({np.nan:'NULL'})
email_df['sent'] =np.where((email_df['sent_at']!='NULL'), 1, 0)

# To CSVs

email_df.to_csv('/home/matts/Documents/emails.csv',sep='|', index = False)
