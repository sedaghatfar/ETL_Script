CREATE SCHEMA IF NOT EXISTS salesloft;

CREATE TABLE salesloft.emails (
    id int,
    created_at timestamp,
    recipient_email_address varchar,
    status varchar,
    bounced boolean,
    sent_at varchar,
    clicks int,
    views int,
    replies int,
    user_id int,
    recipient_id int,
    cadence_id varchar,
    sent int
);

CREATE TABLE salesloft.people (
  id int,
  created_at timestamp,
  updated_at timestamp,
  last_contacted_at varchar,
  display_name varchar,
  email_address varchar,
  phone varchar,
  do_not_contact varchar,
  tags varchar,
  emails_sent int,
  emails_viewed int,
  emails_replied_to int,
  calls int,
  owner_id int
);

CREATE TABLE salesloft.interest (
  id int,
  added_at timestamp,
  created_at timestamp,
  updated_at varchar,
  person_id int,
  person_deleted boolean,
  user_id int,
  currently_on_cadence boolean,
  current_state varchar,
  replies int,
  calls int,
  sent_emails int,
  bounces int,
  recent int,
  points int
  );
