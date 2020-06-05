#!/usr/bin/python3
import config
import pandas as pd
import psycopg2
from io import StringIO

conn = psycopg2.connect(config.placepass)
cur = conn.cursor()

#emails

df = pd.read_csv("/home/matts/Documents/new_emails.csv", sep='|')

cur.execute("DELETE FROM salesloft.emails_staging WHERE 1 = 1;")
conn.commit()

sio = StringIO()
sio.write(df.to_csv(index=None, header=None, sep='|'))
sio.seek(0)

with conn.cursor() as c:
    c.copy_from(sio, "salesloft.emails_staging", columns=df.columns, sep='|')
    conn.commit()

cur.execute("DELETE FROM salesloft.emails WHERE emails.id in (SELECT id from salesloft.emails_staging);")
conn.commit()

cur.execute("""INSERT INTO salesloft.emails (id, created_at, updated_at, recipient_email_address, status, bounced, sent_at, clicks, views, replies, user_id, recipient_id, cadence_id, sent)
SELECT *
FROM salesloft.emails_staging;""")
conn.commit()

cur.close()
