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

0 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/new_people.py

3 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/new_emails.py

6 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/interest.py

9 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/uploads.py

12 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/email_upload.py

15 1 * * 1-5 cd /home/matts/Documents && sudo /usr/bin/python3 /home/matts/Documents/people_upload.py

# Future Updates

1. Check how often data is refreshed, as dropping and pulling all data can be time intesive
    - This has been addressed using the updated_at column
2. Using a staging table
    - Done
3. Use https://cloud.google.com/functions for serverless script and have csvs stored in https://cloud.google.com/storage for backup


# Example Query for number of leads in the last 30 days

SELECT 
    created_at::date as date
    
   ,COUNT(*)
    
FROM salesloft.people

WHERE created_at >= CURRENT_DATE - 30

GROUP BY 1


