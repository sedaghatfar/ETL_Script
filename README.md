# ETL Script

Script to take data from Salesloft CRM https://developers.salesloft.com/api.html and load into Postgres

Looking to Answer 

1. Count of leads added

2. Count of emails Sent

3. Count of email replies

4. Count of leads aded to interest cadence

5. % of leads in interest cadence with touches in the last 2 weeks

6. Count of leads that have X Tag


In sudo crontab -e to run Mon-Friday at 1 UTC (Make sure root has all the python libraries installed)

0 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/people.py
5 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/lev_email.py
10 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/interest.py
15 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/uploads.py


#Example Query for number of leads in the last 30 days

SELECT 
    created_at::date as date
    ,COUNT(*)
FROM salesloft.people
WHERE created_at >= CURRENT_DATE - 30
GROUP BY 1


