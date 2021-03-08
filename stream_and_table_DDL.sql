
CREATE OR REPLACE STREAM USER_AVRO_RAW_STREAM 
WITH (KAFKA_TOPIC='user_avro_topic', VALUE_FORMAT='AVRO');

-- partition by USER_ID
CREATE OR REPLACE STREAM USER_AVRO_PARTITIONED_STREAM
AS
    SELECT * 
    FROM USER_AVRO_RAW_STREAM
    PARTITION BY USER_ID;

-- materialized table with PRIMARY KEY same as PARTITION_BY
CREATE OR REPLACE TABLE USER_AVRO_TABLE 
AS 
    SELECT
        USER_ID USER_ID,
        LATEST_BY_OFFSET(USER_NAME) as USER_NAME,
        LATEST_BY_OFFSET(LAST_LOGIN) as LAST_LOGIN,
        LATEST_BY_OFFSET(ACTIVE) as ACTIVE_FLAG,
        LATEST_BY_OFFSET(CONTACT_REF_ID) as CONTACT_REF_ID
    FROM USER_AVRO_PARTITIONED_STREAM
    GROUP BY USER_ID;


CREATE OR REPLACE STREAM CONTACT_AVRO_RAW_STREAM 
WITH (KAFKA_TOPIC='contact_avro_topic', VALUE_FORMAT='AVRO');

CREATE OR REPLACE STREAM CONTACT_AVRO_PARTITIONED_STREAM 
AS 
    SELECT *
    FROM CONTACT_AVRO_RAW_STREAM
    PARTITION BY CONTACT_ID;

-- materialized table with PRIMARY KEY same as PARTITION_BY
CREATE OR REPLACE TABLE CONTACT_AVRO_TABLE
AS
    SELECT
        s.CONTACT_ID,
        LATEST_BY_OFFSET(s.PRIMARY) as PRIMARY_CONTACT_TYPE,
        LATEST_BY_OFFSET(s.EMAIL) as EMAIL_ID,
        LATEST_BY_OFFSET(s.COUNTRY_CODE) as COUNTRY_CODE,
        LATEST_BY_OFFSET(s.PHONE) as PHONE
    FROM CONTACT_AVRO_PARTITIONED_STREAM s
    GROUP BY CONTACT_ID;

-- Stream LEFT JOIN Table
CREATE OR REPLACE STREAM user_stream_join_contact_table__stream
AS
    SELECT
        u_s.USER_ID as USER_ID,
        u_s.USER_NAME as USER_NAME,
        c_t.CONTACT_ID as CONTACT_ID,
        c_t.EMAIL_ID as EMAIL_ID,
        cast(c_t.COUNTRY_CODE as string) + '-' + cast(c_t.PHONE as string) as PHONE_NUMBER,
        CASE
            WHEN (c_t.PRIMARY_CONTACT_TYPE = 'EMAIL' OR c_t.PRIMARY_CONTACT_TYPE = 'E') THEN c_t.EMAIL_ID
            WHEN (c_t.PRIMARY_CONTACT_TYPE = 'PHONE' OR c_t.PRIMARY_CONTACT_TYPE = 'P') THEN cast(c_t.COUNTRY_CODE as string) + '-' + cast(c_t.PHONE as string)
            ELSE 'INVALID_PRIMARY_CONTACT'
        END as PRIMARY_CONTACT,
        u_s.LAST_LOGIN as LAST_LOGIN_TIMESTAMP,
        u_s.ACTIVE as ACTIVE_FLAG
    FROM USER_AVRO_PARTITIONED_STREAM u_s
        LEFT JOIN CONTACT_AVRO_TABLE c_t 
            ON u_s.CONTACT_REF_ID = c_t.CONTACT_ID -- MUST USE PRIMARY_KEY of TABLE
    PARTITION BY u_s.USER_NAME;

-- materialized table on-top-of joined stream
CREATE OR REPLACE TABLE user_join_contact_table
AS
    SELECT
        USER_NAME,
        timestamptostring(MAX(STRINGTOTIMESTAMP(LAST_LOGIN_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS', 'America/New_York')), 'yyyy-MM-dd HH:mm:ss.SSS', 'America/New_York') as LATEST_LOGIN_TIMESTAMP,
        LATEST_BY_OFFSET(PRIMARY_CONTACT) as PRIMARY_CONTACT,
        LATEST_BY_OFFSET(ACTIVE_FLAG) as ACTIVE_FLAG
    FROM user_stream_join_contact_table__stream
    GROUP BY USER_NAME;

 CREATE OR REPLACE STREAM user_stream_equi_join_contact_table_repartitioned__stream
AS
    SELECT u_s.CONTACT_REF_ID as CONTACT_ID,
        u_s.USER_ID as USER_ID,
        u_s.ROWTIME as USER_TS
    FROM USER_AVRO_PARTITIONED_STREAM u_s
        JOIN CONTACT_AVRO_TABLE c_t
            ON u_s.CONTACT_REF_ID = c_t.CONTACT_ID
    PARTITION BY u_s.CONTACT_REF_ID;
---

CREATE OR REPLACE STREAM user_by_dependent_lookup_stream
    (_ID BIGINT KEY)
WITH (KAFKA_TOPIC='user_by_dependent_lookup_topic', VALUE_FORMAT='AVRO');

INSERT INTO user_by_dependent_lookup_stream 
AS 
    SELECT s.CONTACT_ID, s.USER_ID, s.USER_TS, 'CONTACT' as RELATIONSHIP_TYPE
    FROM user_stream_equi_join_contact_table_repartitioned__stream s;

--- Create lookup entity for each contact_id that gives map of user_id as key and user's timestamp as value
CREATE OR REPLACE TABLE user_by_contact_lookup_table 
AS 
    SELECT
    uc_s.CONTACT_ID as CONTACT_ID,
    'CONTACT' as RELATIONSHIP_TYPE,
    AS_MAP(COLLECT_LIST(CAST(uc_s.USER_ID AS STRING)), COLLECT_LIST(uc_s.USER_TS)) as PARENT_IDS_MAP
    FROM user_by_contact_lookup_stream uc_s
    GROUP BY uc_s.CONTACT_ID;
