-- VERY Important to ensure the table is recorded from beginning
SET 'auto.offset.reset' = 'earliest';

CREATE OR REPLACE TABLE lndcdcadsprpsl_prpslotlt_record_counter_tbl
AS
SELECT
    _ID as KAFKA_KEY_AS_TBL_PK,
    latest_by_offset(PRPSL_OTLT_ID) PRPSL_OTLT_ID,
    earliest_by_offset(CNCRNCY_VRSN, false) MIN_CNCRNCY_VRSN,
    latest_by_offset(CNCRNCY_VRSN, false) MAX_CNCRNCY_VRSN,
    earliest_by_offset(SRC_CDC_OPER_NM) MIN_OPER,
    latest_by_offset(SRC_CDC_OPER_NM) MAX_OPER,
    COUNT(SRC_CDC_OPER_NM) TOTAL_RECORDS
FROM LNDCDCADSPRPSL_PRPSLOTLT_STREAM
GROUP BY _ID;

DESCRIBE EXTENDED lndcdcadsprpsl_prpslotlt_record_counter_tbl;

```
Name                 : LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL
Type                 : TABLE
Timestamp field      : Not set - using <ROWTIME>
Key format           : KAFKA
Value format         : AVRO
Kafka topic          : LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL (partitions: 8, replication: 3)
Statement            : CREATE OR REPLACE TABLE LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL WITH (KAFKA_TOPIC='LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL', PARTITIONS=8, REPLICAS=3) AS SELECT
  LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID KAFKA_KEY_AS_TBL_PK,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRPSL_OTLT_ID) PRPSL_OTLT_ID,
  EARLIEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN, false) MIN_CNCRNCY_VRSN,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN, false) MAX_CNCRNCY_VRSN,
  EARLIEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) MIN_OPER,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) MAX_OPER,
  COUNT(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) TOTAL_RECORDS
FROM LNDCDCADSPRPSL_PRPSLOTLT_STREAM LNDCDCADSPRPSL_PRPSLOTLT_STREAM
GROUP BY LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID
EMIT CHANGES;

 Field               | Type                           
------------------------------------------------------
 KAFKA_KEY_AS_TBL_PK | VARCHAR(STRING)  (primary key) 
 PRPSL_OTLT_ID       | BIGINT                         
 MIN_CNCRNCY_VRSN    | BIGINT                         
 MAX_CNCRNCY_VRSN    | BIGINT                         
 MIN_OPER            | VARCHAR(STRING)                
 MAX_OPER            | VARCHAR(STRING)                
 TOTAL_RECORDS       | BIGINT                         
------------------------------------------------------

Queries that write from this TABLE
-----------------------------------
CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263 (RUNNING) : CREATE OR REPLACE TABLE LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL WITH (KAFKA_TOPIC='LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL', PARTITIONS=8, REPLICAS=3) AS SELECT   LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID KAFKA_KEY_AS_TBL_PK,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRPSL_OTLT_ID) PRPSL_OTLT_ID,   EARLIEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN, false) MIN_CNCRNCY_VRSN,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN, false) MAX_CNCRNCY_VRSN,   EARLIEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) MIN_OPER,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) MAX_OPER,   COUNT(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) TOTAL_RECORDS FROM LNDCDCADSPRPSL_PRPSLOTLT_STREAM LNDCDCADSPRPSL_PRPSLOTLT_STREAM GROUP BY LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID EMIT CHANGES;

For query topology and execution plan please run: EXPLAIN <QueryId>

Local runtime statistics
------------------------
messages-per-sec:         0   total-messages:   1210214     last-message: 2021-09-27T20:34:48.584Z

(Statistics of the local KSQL server interaction with the Kafka topic LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL)

Consumer Groups summary:

Consumer Group       : _confluent-ksql-default_query_CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263

Kafka topic          : lndcdcadsprpsl_prpslotlt
Max lag              : 0

 Partition | Start Offset | End Offset | Offset | Lag 
------------------------------------------------------
 0         | 0            | 155724     | 155724 | 0   
 1         | 0            | 150976     | 150976 | 0   
 2         | 0            | 151341     | 151341 | 0   
 3         | 0            | 152318     | 152318 | 0   
 4         | 0            | 152113     | 152113 | 0   
 5         | 0            | 151287     | 151287 | 0   
 6         | 0            | 151954     | 151954 | 0   
 7         | 0            | 152154     | 152154 | 0   
------------------------------------------------------
```

-- BAD BOYS
select * from lndcdcadsprpsl_prpslotlt_record_counter_tbl
where KAFKA_KEY_AS_TBL_PK in ('1295237', '1296923', '1296924', '1295936');
```
+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
|KAFKA_KEY_AS_TBL_PK    |PRPSL_OTLT_ID          |MIN_CNCRNCY_VRSN       |MAX_CNCRNCY_VRSN       |MIN_OPER               |MAX_OPER               |TOTAL_RECORDS          |
+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
|1295237                |1295237                |1                      |1                      |INSERT                 |INSERT                 |1                      |
|1296923                |1296923                |1                      |1                      |INSERT                 |INSERT                 |1                      |
|1296924                |1296924                |1                      |1                      |INSERT                 |INSERT                 |1                      |
|1295936                |1295936                |1                      |1                      |INSERT                 |INSERT                 |1                      |
```

-- GOOD GIRLS
select * from lndcdcadsprpsl_prpslotlt_record_counter_tbl 
where KAFKA_KEY_AS_TBL_PK in ('1296206', '1296590');
```
+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
|KAFKA_KEY_AS_TBL_PK    |PRPSL_OTLT_ID          |MIN_CNCRNCY_VRSN       |MAX_CNCRNCY_VRSN       |MIN_OPER               |MAX_OPER               |TOTAL_RECORDS          |
+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+-----------------------+
|1296206                |1296206                |2                      |null                   |SQL COMPUPDATE         |DELETE                 |3                      |
|1296590                |1296590                |2                      |null                   |INSERT                 |DELETE                 |4                      |
```