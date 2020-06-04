#!/usr/bin/python3
import config
import pandas as pd
import psycopg2
from io import StringIO

conn = psycopg2.connect(config.placepass)
cur = conn.cursor()

#emails

df = pd.read_csv("/home/matts/Documents/emails.csv", sep='|')

cur.execute("DELETE FROM salesloft.emails WHERE 1 = 1;")
conn.commit()

sio = StringIO()
sio.write(df.to_csv(index=None, header=None, sep='|'))
sio.seek(0)

with conn.cursor() as c:
    c.copy_from(sio, "salesloft.emails", columns=df.columns, sep='|')
    conn.commit()

# people
df = pd.read_csv("/home/matts/Documents/people.csv", sep='|')

cur.execute("DELETE FROM salesloft.people WHERE 1 = 1;")
conn.commit()

sio = StringIO()
sio.write(df.to_csv(index=None, header=None, sep='|'))
sio.seek(0)

with conn.cursor() as c:
    c.copy_from(sio, "salesloft.people", columns=df.columns, sep='|')
    conn.commit()

# interest
df = pd.read_csv("/home/matts/Documents/interest.csv", sep='|')

cur.execute("DELETE FROM salesloft.interest WHERE 1 = 1;")
conn.commit()

sio = StringIO()
sio.write(df.to_csv(index=None, header=None, sep='|'))
sio.seek(0)

with conn.cursor() as c:
    c.copy_from(sio, "salesloft.interest", columns=df.columns, sep='|')
    conn.commit()


cur.close()
