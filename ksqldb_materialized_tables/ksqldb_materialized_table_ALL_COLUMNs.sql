-- VERY Important to ensure the table is recorded from beginning
SET 'auto.offset.reset' = 'earliest';

-- MATERIALIZED table
CREATE OR REPLACE TABLE lndcdcadsprpsl_prpslotlt_mat_tbl
AS
SELECT
    _ID as KAFKA_KEY_AS_TBL_PK,
    latest_by_offset(PRPSL_OTLT_ID) PRPSL_OTLT_ID,
    latest_by_offset(LAST_MODIFIED_BY) LAST_MODIFIED_BY,
    latest_by_offset(LAST_MODIFIED_DT) LAST_MODIFIED_DT,
    latest_by_offset(PROPOSAL_ID) PROPOSAL_ID,
    latest_by_offset(OUTLET_EXTENSION_ID) OUTLET_EXTENSION_ID,
    latest_by_offset(PRIM_INVNTRY_TYP_ID) PRIM_INVNTRY_TYP_ID,
    latest_by_offset(PRIM_UNIT_LGTH_ID) PRIM_UNIT_LGTH_ID,
    latest_by_offset(PRIM_UNIT_QTY) PRIM_UNIT_QTY,
    latest_by_offset(SEC_INVNTRY_TYP_ID) SEC_INVNTRY_TYP_ID,
    latest_by_offset(SEC_UNIT_LGTH_ID) SEC_UNIT_LGTH_ID,
    latest_by_offset(SEC_UNIT_QTY) SEC_UNIT_QTY,
    latest_by_offset(SRC_SYS_APP_RGSTRY_ID) SRC_SYS_APP_RGSTRY_ID,
    latest_by_offset(CNCRNCY_VRSN) CNCRNCY_VRSN,
    latest_by_offset(SRC_CDC_OPER_NM) SRC_CDC_OPER_NM,
    latest_by_offset(SRC_COMMIT_DT_UTC) SRC_COMMIT_DT_UTC,
    latest_by_offset(TRG_CRT_DT_PART_UTC) TRG_CRT_DT_PART_UTC
FROM LNDCDCADSPRPSL_PRPSLOTLT_STREAM
GROUP BY _ID;

SHOW TABLES;

DESCRIBE EXTENDED lndcdcadsprpsl_prpslotlt_mat_tbl;

```
Name                 : LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL
Type                 : TABLE
Timestamp field      : Not set - using <ROWTIME>
Key format           : KAFKA
Value format         : AVRO
Kafka topic          : LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL (partitions: 8, replication: 3)
Statement            : CREATE OR REPLACE TABLE LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL WITH (KAFKA_TOPIC='LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL', PARTITIONS=8, REPLICAS=3) AS SELECT
  LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID KAFKA_KEY_AS_TBL_PK,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRPSL_OTLT_ID) PRPSL_OTLT_ID,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.LAST_MODIFIED_BY) LAST_MODIFIED_BY,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.LAST_MODIFIED_DT) LAST_MODIFIED_DT,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PROPOSAL_ID) PROPOSAL_ID,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.OUTLET_EXTENSION_ID) OUTLET_EXTENSION_ID,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_INVNTRY_TYP_ID) PRIM_INVNTRY_TYP_ID,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_UNIT_LGTH_ID) PRIM_UNIT_LGTH_ID,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_UNIT_QTY) PRIM_UNIT_QTY,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_INVNTRY_TYP_ID) SEC_INVNTRY_TYP_ID,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_UNIT_LGTH_ID) SEC_UNIT_LGTH_ID,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_UNIT_QTY) SEC_UNIT_QTY,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_SYS_APP_RGSTRY_ID) SRC_SYS_APP_RGSTRY_ID,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN) CNCRNCY_VRSN,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) SRC_CDC_OPER_NM,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_COMMIT_DT_UTC) SRC_COMMIT_DT_UTC,
  LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.TRG_CRT_DT_PART_UTC) TRG_CRT_DT_PART_UTC
FROM LNDCDCADSPRPSL_PRPSLOTLT_STREAM LNDCDCADSPRPSL_PRPSLOTLT_STREAM
GROUP BY LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID
EMIT CHANGES;

 Field                 | Type                           
--------------------------------------------------------
 KAFKA_KEY_AS_TBL_PK   | VARCHAR(STRING)  (primary key) 
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
 SRC_CDC_OPER_NM       | VARCHAR(STRING)                
 SRC_COMMIT_DT_UTC     | VARCHAR(STRING)                
 TRG_CRT_DT_PART_UTC   | VARCHAR(STRING)                
--------------------------------------------------------

Queries that write from this TABLE
-----------------------------------
CTAS_LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL_237 (RUNNING) : CREATE OR REPLACE TABLE LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL WITH (KAFKA_TOPIC='LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL', PARTITIONS=8, REPLICAS=3) AS SELECT   LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID KAFKA_KEY_AS_TBL_PK,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRPSL_OTLT_ID) PRPSL_OTLT_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.LAST_MODIFIED_BY) LAST_MODIFIED_BY,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.LAST_MODIFIED_DT) LAST_MODIFIED_DT,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PROPOSAL_ID) PROPOSAL_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.OUTLET_EXTENSION_ID) OUTLET_EXTENSION_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_INVNTRY_TYP_ID) PRIM_INVNTRY_TYP_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_UNIT_LGTH_ID) PRIM_UNIT_LGTH_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.PRIM_UNIT_QTY) PRIM_UNIT_QTY,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_INVNTRY_TYP_ID) SEC_INVNTRY_TYP_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_UNIT_LGTH_ID) SEC_UNIT_LGTH_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SEC_UNIT_QTY) SEC_UNIT_QTY,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_SYS_APP_RGSTRY_ID) SRC_SYS_APP_RGSTRY_ID,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.CNCRNCY_VRSN) CNCRNCY_VRSN,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_CDC_OPER_NM) SRC_CDC_OPER_NM,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.SRC_COMMIT_DT_UTC) SRC_COMMIT_DT_UTC,   LATEST_BY_OFFSET(LNDCDCADSPRPSL_PRPSLOTLT_STREAM.TRG_CRT_DT_PART_UTC) TRG_CRT_DT_PART_UTC FROM LNDCDCADSPRPSL_PRPSLOTLT_STREAM LNDCDCADSPRPSL_PRPSLOTLT_STREAM GROUP BY LNDCDCADSPRPSL_PRPSLOTLT_STREAM._ID EMIT CHANGES;

For query topology and execution plan please run: EXPLAIN <QueryId>

Local runtime statistics
------------------------
messages-per-sec:      0.32   total-messages:   1210797     last-message: 2021-09-27T20:29:49.831Z

(Statistics of the local KSQL server interaction with the Kafka topic LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL)

Consumer Groups summary:

Consumer Group       : _confluent-ksql-default_query_CTAS_LNDCDCADSPRPSL_PRPSLOTLT_MAT_TBL_237

Kafka topic          : lndcdcadsprpsl_prpslotlt
Max lag              : 0

 Partition | Start Offset | End Offset | Offset | Lag 
------------------------------------------------------
 0         | 0            | 155722     | 155722 | 0   
 1         | 0            | 150976     | 150976 | 0   
 2         | 0            | 151341     | 151341 | 0   
 3         | 0            | 152318     | 152318 | 0   
 4         | 0            | 152113     | 152113 | 0   
 5         | 0            | 151286     | 151286 | 0   
 6         | 0            | 151954     | 151954 | 0   
 7         | 0            | 152152     | 152152 | 0   
------------------------------------------------------
```

-- BAD BOYS
select * from lndcdcadsprpsl_prpslotlt_mat_tbl 
where KAFKA_KEY_AS_TBL_PK in ('1295237', '1296923', '1296924', '1295936');
```
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|KAFKA_KE|PRPSL_OT|LAST_MOD|LAST_MOD|PROPOSAL|OUTLET_E|PRIM_INV|PRIM_UNI|PRIM_UNI|SEC_INVN|SEC_UNIT|SEC_UNIT|SRC_SYS_|CNCRNCY_|SRC_CDC_|SRC_COMM|TRG_CRT_|
|Y_AS_TBL|LT_ID   |IFIED_BY|IFIED_DT|_ID     |XTENSION|NTRY_TYP|T_LGTH_I|T_QTY   |TRY_TYP_|_LGTH_ID|_QTY    |APP_RGST|VRSN    |OPER_NM |IT_DT_UT|DT_PART_|
|_PK     |        |        |        |        |_ID     |_ID     |D       |        |ID      |        |        |RY_ID   |        |        |C       |UTC     |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|1295237 |1295237 |1000f97a|2021-09-|754221  |958     |null    |null    |null    |null    |null    |null    |3       |1       |INSERT  |2021-09-|2021-09-|
|        |        |-755c-4e|06 13:55|        |        |        |        |        |        |        |        |        |        |        |06 17:55|06 17:55|
|        |        |c9-9d7a-|:38.5485|        |        |        |        |        |        |        |        |        |        |        |:46.4684|:46.4684|
|        |        |b0d06d43|792     |        |        |        |        |        |        |        |        |        |        |        |49      |49      |
|        |        |2acc    |        |        |        |        |        |        |        |        |        |        |        |        |        |        |
|1296923 |1296923 |5698b66f|2021-09-|755050  |950     |null    |null    |null    |null    |null    |null    |3       |1       |INSERT  |2021-09-|2021-09-|
|        |        |-058f-43|17 18:42|        |        |        |        |        |        |        |        |        |        |        |17 22:42|17 22:42|
|        |        |95-9ffc-|:30.1795|        |        |        |        |        |        |        |        |        |        |        |:39.5042|:39.5042|
|        |        |9452cbf3|995     |        |        |        |        |        |        |        |        |        |        |        |4       |4       |
|        |        |8953    |        |        |        |        |        |        |        |        |        |        |        |        |        |        |
|1296924 |1296924 |5698b66f|2021-09-|755050  |958     |null    |null    |null    |null    |null    |null    |3       |1       |INSERT  |2021-09-|2021-09-|
|        |        |-058f-43|17 18:42|        |        |        |        |        |        |        |        |        |        |        |17 22:42|17 22:42|
|        |        |95-9ffc-|:30.1795|        |        |        |        |        |        |        |        |        |        |        |:39.5250|:39.5250|
|        |        |9452cbf3|995     |        |        |        |        |        |        |        |        |        |        |        |7       |7       |
|        |        |8953    |        |        |        |        |        |        |        |        |        |        |        |        |        |        |
|1295936 |1295936 |1000f97a|2021-09-|754529  |958     |null    |null    |null    |null    |null    |null    |3       |1       |INSERT  |2021-09-|2021-09-|
|        |        |-755c-4e|11 18:12|        |        |        |        |        |        |        |        |        |        |        |11 22:12|11 22:12|
|        |        |c9-9d7a-|:50.4231|        |        |        |        |        |        |        |        |        |        |        |:58.8317|:58.8317|
|        |        |b0d06d43|734     |        |        |        |        |        |        |        |        |        |        |        |24      |24      |
|        |        |2acc    |        |        |        |        |        |        |        |        |        |        |        |        |        |        |
```

-- GOOD GIRLS
select * from lndcdcadsprpsl_prpslotlt_mat_tbl 
where KAFKA_KEY_AS_TBL_PK in ('1296206', '1296590');
```
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|KAFKA_KE|PRPSL_OT|LAST_MOD|LAST_MOD|PROPOSAL|OUTLET_E|PRIM_INV|PRIM_UNI|PRIM_UNI|SEC_INVN|SEC_UNIT|SEC_UNIT|SRC_SYS_|CNCRNCY_|SRC_CDC_|SRC_COMM|TRG_CRT_|
|Y_AS_TBL|LT_ID   |IFIED_BY|IFIED_DT|_ID     |XTENSION|NTRY_TYP|T_LGTH_I|T_QTY   |TRY_TYP_|_LGTH_ID|_QTY    |APP_RGST|VRSN    |OPER_NM |IT_DT_UT|DT_PART_|
|_PK     |        |        |        |        |_ID     |_ID     |D       |        |ID      |        |        |RY_ID   |        |        |C       |UTC     |
+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
|1296206 |1296206 |ADS_PRPS|2021-09-|754666  |4       |5       |1       |null    |1       |16      |null    |3       |2       |DELETE  |2021-09-|2021-09-|
|        |        |L_USER  |14 11:07|        |        |        |        |        |        |        |        |        |        |        |14 17:01|14 17:01|
|        |        |        |:51.0910|        |        |        |        |        |        |        |        |        |        |        |:33.4352|:33.4352|
|        |        |        |77      |        |        |        |        |        |        |        |        |        |        |        |83      |83      |
|1296590 |1296590 |ADS_PRPS|2021-09-|754859  |1       |5       |1       |null    |1       |16      |null    |3       |3       |DELETE  |2021-09-|2021-09-|
|        |        |L_USER  |16 10:37|        |        |        |        |        |        |        |        |        |        |        |16 14:42|16 14:42|
|        |        |        |:08.3616|        |        |        |        |        |        |        |        |        |        |        |:59.4256|:59.4256|
|        |        |        |64      |        |        |        |        |        |        |        |        |        |        |        |43      |43      |
```