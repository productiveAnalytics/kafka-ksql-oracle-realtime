-- VERY IMPORTANT so that the stream is from beginning of Kafka topic
SET 'auto.offset.reset' = 'earliest';

CREATE OR REPLACE STREAM lndcdcadsprpsl_prpslotlt_stream (_ID STRING KEY) 
with (KAFKA_TOPIC='lndcdcadsprpsl_prpslotlt', VALUE_FORMAT='AVRO');

SHOW STREAMS;

DESCIBE EXTENDED LNDCDCADSPRPSL_PRPSLOTLT_STREAM;

```
Name                 : LNDCDCADSPRPSL_PRPSLOTLT_STREAM
Type                 : STREAM
Timestamp field      : Not set - using <ROWTIME>
Key format           : KAFKA
Value format         : AVRO
Kafka topic          : lndcdcadsprpsl_prpslotlt (partitions: 8, replication: 3)
Statement            : CREATE OR REPLACE STREAM LNDCDCADSPRPSL_PRPSLOTLT_STREAM (_ID STRING KEY, PRPSL_OTLT_ID BIGINT, LAST_MODIFIED_BY STRING, LAST_MODIFIED_DT STRING, PROPOSAL_ID BIGINT, OUTLET_EXTENSION_ID BIGINT, PRIM_INVNTRY_TYP_ID BIGINT, PRIM_UNIT_LGTH_ID BIGINT, PRIM_UNIT_QTY BIGINT, SEC_INVNTRY_TYP_ID BIGINT, SEC_UNIT_LGTH_ID BIGINT, SEC_UNIT_QTY BIGINT, SRC_SYS_APP_RGSTRY_ID BIGINT, CNCRNCY_VRSN BIGINT, SRC_KEY_VAL STRING, SRC_CDC_OPER_NM STRING, SRC_COMMIT_DT_UTC STRING, TRG_CRT_DT_PART_UTC STRING, SRC_SCHEMA_NM STRING) WITH (KAFKA_TOPIC='lndcdcadsprpsl_prpslotlt', KEY_FORMAT='KAFKA', VALUE_FORMAT='AVRO', VALUE_SCHEMA_ID=355);

 Field                 | Type                   
------------------------------------------------
 _ID                   | VARCHAR(STRING)  (key) 
 PRPSL_OTLT_ID         | BIGINT                 
 LAST_MODIFIED_BY      | VARCHAR(STRING)        
 LAST_MODIFIED_DT      | VARCHAR(STRING)        
 PROPOSAL_ID           | BIGINT                 
 OUTLET_EXTENSION_ID   | BIGINT                 
 PRIM_INVNTRY_TYP_ID   | BIGINT                 
 PRIM_UNIT_LGTH_ID     | BIGINT                 
 PRIM_UNIT_QTY         | BIGINT                 
 SEC_INVNTRY_TYP_ID    | BIGINT                 
 SEC_UNIT_LGTH_ID      | BIGINT                 
 SEC_UNIT_QTY          | BIGINT                 
 SRC_SYS_APP_RGSTRY_ID | BIGINT                 
 CNCRNCY_VRSN          | BIGINT                 
 SRC_KEY_VAL           | VARCHAR(STRING)        
 SRC_CDC_OPER_NM       | VARCHAR(STRING)        
 SRC_COMMIT_DT_UTC     | VARCHAR(STRING)        
 TRG_CRT_DT_PART_UTC   | VARCHAR(STRING)        
 SRC_SCHEMA_NM         | VARCHAR(STRING)        
------------------------------------------------

Queries that read from this STREAM
-----------------------------------
CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263 (RUNNING) : CREATE OR REPLACE TABLE LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL WITH (KAFKA_TOPIC='LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL', PARTITIONS=8, REPLICAS=3) AS SELECT   LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID KAFKA_KEY_AS_TBL_PK,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRPSL_OTLT_ID) PRPSL_OTLT_ID,   EARLIEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN, false) MIN_CNCRNCY_VRSN,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN, false) MAX_CNCRNCY_VRSN,   EARLIEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) MIN_OPER,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) MAX_OPER,   COUNT(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) TOTAL_RECORDS FROM LNDCDCADSPRPSL_PRPSLOTLT_STREAM LNDCDCADSPRPSL_PRPSLOTLT_STREAM GROUP BY LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID EMIT CHANGES;
CTAS_LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL_237 (RUNNING) : CREATE OR REPLACE TABLE LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL WITH (KAFKA_TOPIC='LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL', PARTITIONS=8, REPLICAS=3) AS SELECT   LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID KAFKA_KEY_AS_TBL_PK,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRPSL_OTLT_ID) PRPSL_OTLT_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.LAST_MODIFIED_BY) LAST_MODIFIED_BY,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.LAST_MODIFIED_DT) LAST_MODIFIED_DT,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PROPOSAL_ID) PROPOSAL_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.OUTLET_EXTENSION_ID) OUTLET_EXTENSION_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_INVNTRY_TYP_ID) PRIM_INVNTRY_TYP_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_UNIT_LGTH_ID) PRIM_UNIT_LGTH_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_UNIT_QTY) PRIM_UNIT_QTY,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_INVNTRY_TYP_ID) SEC_INVNTRY_TYP_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_UNIT_LGTH_ID) SEC_UNIT_LGTH_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_UNIT_QTY) SEC_UNIT_QTY,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_SYS_APP_RGSTRY_ID) SRC_SYS_APP_RGSTRY_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN) CNCRNCY_VRSN,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) SRC_CDC_OPER_NM,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_COMMIT_DT_UTC) SRC_COMMIT_DT_UTC,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.TRG_CRT_DT_PART_UTC) TRG_CRT_DT_PART_UTC FROM LNDCDCADSPRPSL_PRPSLOTLT_STREAM LNDCDCADSPRPSL_PRPSLOTLT_STREAM GROUP BY LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID EMIT CHANGES;
```

---

-- SHOW detailed ksqldb query (Internal Kafka Steams app)
-- View the Kafka Streams topology at https://zz85.github.io/kafka-streams-viz/
EXPLAIN CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263;

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
ksql> EXPLAIN CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263;

ID                   : CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263
Query Type           : PERSISTENT
SQL                  : CREATE OR REPLACE TABLE LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL WITH (KAFKA_TOPIC='LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL', PARTITIONS=8, REPLICAS=3) AS SELECT
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
Host Query Status    : {kafka1:8088=RUNNING}

 Field               | Type                   
----------------------------------------------
 KAFKA_KEY_AS_TBL_PK | VARCHAR(STRING)  (key) 
 PRPSL_OTLT_ID       | BIGINT                 
 MIN_CNCRNCY_VRSN    | BIGINT                 
 MAX_CNCRNCY_VRSN    | BIGINT                 
 MIN_OPER            | VARCHAR(STRING)        
 MAX_OPER            | VARCHAR(STRING)        
 TOTAL_RECORDS       | BIGINT                 
----------------------------------------------

Sources that this query reads from: 
-----------------------------------
LNDCDCADSPRPSL_PRPSLOTLT_STREAM

For source description please run: DESCRIBE [EXTENDED] <SourceId>

Sinks that this query writes to: 
-----------------------------------
LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL

For sink description please run: DESCRIBE [EXTENDED] <SinkId>

Execution plan      
--------------      
 > [ SINK ] | Schema: KAFKA_KEY_AS_TBL_PK STRING KEY, PRPSL_OTLT_ID BIGINT, MIN_CNCRNCY_VRSN BIGINT, MAX_CNCRNCY_VRSN BIGINT, MIN_OPER STRING, MAX_OPER STRING, TOTAL_RECORDS BIGINT | Logger: CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263.LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL
                 > [ PROJECT ] | Schema: KAFKA_KEY_AS_TBL_PK STRING KEY, PRPSL_OTLT_ID BIGINT, MIN_CNCRNCY_VRSN BIGINT, MAX_CNCRNCY_VRSN BIGINT, MIN_OPER STRING, MAX_OPER STRING, TOTAL_RECORDS BIGINT | Logger: CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263.Aggregate.Project
                                 > [ AGGREGATE ] | Schema: _ID STRING KEY, _ID STRING, PRPSL_OTLT_ID BIGINT, CNCRNCY_VRSN BIGINT, SRC_CDC_OPER_NM STRING, KSQL_AGG_VARIABLE_0 BIGINT, KSQL_AGG_VARIABLE_1 BIGINT, KSQL_AGG_VARIABLE_2 BIGINT, KSQL_AGG_VARIABLE_3 STRING, KSQL_AGG_VARIABLE_4 STRING, KSQL_AGG_VARIABLE_5 BIGINT | Logger: CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263.Aggregate.Aggregate
                                                 > [ GROUP_BY ] | Schema: _ID STRING KEY, _ID STRING, PRPSL_OTLT_ID BIGINT, CNCRNCY_VRSN BIGINT, SRC_CDC_OPER_NM STRING, KSQL_INTERNAL_COL_4 BOOLEAN | Logger: CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263.Aggregate.GroupBy
                                                                 > [ PROJECT ] | Schema: _ID STRING KEY, _ID STRING, PRPSL_OTLT_ID BIGINT, CNCRNCY_VRSN BIGINT, SRC_CDC_OPER_NM STRING, KSQL_INTERNAL_COL_4 BOOLEAN | Logger: CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263.Aggregate.Prepare
                                                                                 > [ SOURCE ] | Schema: _ID STRING KEY, PRPSL_OTLT_ID BIGINT, LAST_MODIFIED_BY STRING, LAST_MODIFIED_DT STRING, PROPOSAL_ID BIGINT, OUTLET_EXTENSION_ID BIGINT, PRIM_INVNTRY_TYP_ID BIGINT, PRIM_UNIT_LGTH_ID BIGINT, PRIM_UNIT_QTY BIGINT, SEC_INVNTRY_TYP_ID BIGINT, SEC_UNIT_LGTH_ID BIGINT, SEC_UNIT_QTY BIGINT, SRC_SYS_APP_RGSTRY_ID BIGINT, CNCRNCY_VRSN BIGINT, SRC_KEY_VAL STRING, SRC_CDC_OPER_NM STRING, SRC_COMMIT_DT_UTC STRING, TRG_CRT_DT_PART_UTC STRING, SRC_SCHEMA_NM STRING, ROWTIME BIGINT, _ID STRING | Logger: CTAS_LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL_263.KsqlTopic.Source


Processing topology 
------------------- 
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [lndcdcadsprpsl_prpslotlt])
      --> KSTREAM-TRANSFORMVALUES-0000000001
    Processor: KSTREAM-TRANSFORMVALUES-0000000001 (stores: [])
      --> Aggregate-Prepare
      <-- KSTREAM-SOURCE-0000000000
    Processor: Aggregate-Prepare (stores: [])
      --> KSTREAM-AGGREGATE-0000000003
      <-- KSTREAM-TRANSFORMVALUES-0000000001
    Processor: KSTREAM-AGGREGATE-0000000003 (stores: [Aggregate-Aggregate-Materialize])
      --> Aggregate-Aggregate-ToOutputSchema
      <-- Aggregate-Prepare
    Processor: Aggregate-Aggregate-ToOutputSchema (stores: [])
      --> Aggregate-Project
      <-- KSTREAM-AGGREGATE-0000000003
    Processor: Aggregate-Project (stores: [])
      --> KTABLE-TOSTREAM-0000000006
      <-- Aggregate-Aggregate-ToOutputSchema
    Processor: KTABLE-TOSTREAM-0000000006 (stores: [])
      --> KSTREAM-SINK-0000000007
      <-- Aggregate-Project
    Sink: KSTREAM-SINK-0000000007 (topic: LNDCDCADSPRPSL_PRPSLOTLT_RECORD_COUNTER_TBL)
      <-- KTABLE-TOSTREAM-0000000006



Overridden Properties
---------------------
 Property          | Value    
------------------------------
 auto.offset.reset | earliest 
------------------------------
```