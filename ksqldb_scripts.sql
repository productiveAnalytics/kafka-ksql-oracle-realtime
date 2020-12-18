-- create table
CREATE TABLE users (
     user_name VARCHAR PRIMARY KEY,
     FName VARCHAR,
     LName VARCHAR,
     gender VARCHAR,
     active BOOLEAN,
     last_login: VARCHAR
   ) WITH (
     KAFKA_TOPIC = 'test-topic-users', 
     VALUE_FORMAT = 'JSON'
   );
   
   
-- read KSQL table as Stream
SELECT * FROM USERS EMIT CHANGES;
