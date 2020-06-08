#!/usr/bin/python3
import config
import pandas as pd
import psycopg2
from io import StringIO

conn = psycopg2.connect(config.placepass)
cur = conn.cursor()

#emails

df = pd.read_csv("/home/matts/Documents/new_ppl.csv", sep='|')

cur.execute("DELETE FROM salesloft.people_staging WHERE 1 = 1;")
conn.commit()

sio = StringIO()
sio.write(df.to_csv(index=None, header=None, sep='|'))
sio.seek(0)

with conn.cursor() as c:
    c.copy_from(sio, "salesloft.people_staging", columns=df.columns, sep='|')
    conn.commit()

cur.execute("DELETE FROM salesloft.people WHERE people.id in (SELECT id from salesloft.people_staging);")
conn.commit()

cur.execute("""INSERT INTO salesloft.people (id, created_at, updated_at, last_contacted_at, display_name, email_address, phone, do_not_contact, tags, emails_sent, emails_viewed, emails_replied_to, calls, owner_id)
SELECT *
FROM salesloft.people_staging;""")
conn.commit()

cur.close()
