# Realtime Streaming POC with Kafka/kSQL with Oracle CDC 

## Architecture for Realtime Streaming using Kafka Streams, KSQL, Kafka Connecct (for Snowflake)
![Realtime Streaming architecture](https://github.com/productiveAnalytics/kafka-ksql-oracle-realtime/blob/main/realtime_streaming_kafka_ksqlDB_Snowflake.png?raw=true)

## Installations

Install the brew dependencies for Kafka Connect
* `brew tap confluentinc/homebrew-confluent-hub-client`
* `brew cask install confluent-hub-client`
* Test by running `confluent-hub` in bash

### Confluent Hub and CLI Install
* https://docs.confluent.io/home/connect/confluent-hub/client.html
* https://hub.docker.com/u/confluentinc/
* `confluent-hub install confluentinc/kafka-connect-jdbc:10.0.1`
Grabbed Confluent Cloud CLI cloud
URL for confluent CLI setup https://docs.confluent.io/confluent-cli/current/installing.html 
Check confluent available versions
* curl -sL https://cnfl.io/cli | sh -s -- -l
Install the latest version of confluent cli
cd /usr/local/bin
* curl -sL https://cnfl.io/cli | sh -s -- latest

### Miscellaneous Installs
* Had to install brew jq and rlwrap

### JDBC Driver - Oracle & Kafka Connect
* https://docs.oracle.com/en/cloud/paas/event-hub-cloud/admin-guide/configuring-kafka-connect.html
* https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc
* https://www.oracle.com/database/technologies/appdev/jdbc-ucp-19-8-c-downloads.html

## Building Local Kafka Connnect 
* Important! Make sure local Docker has configuration setup for 8-10 GB of memory 
and 2 GB of memory swap. This can be set in preferences by clicking on the gear icon on 
Docker in the top right corner then going to resources.
* Running locally with Docker. Refer: https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html. Either using the docker-compose.yml file in this repo or better check latest docker-compose.yml from https://github.com/confluentinc/cp-all-in-one/tree/6.0.1-post/cp-all-in-one-community
    * Build the cluster and make compose. `docker-compose up -d`
    * Bring the cluster down with compose. `docker-compose down`
* Optional add - Docker Confluent Platform Images
    * `docker pull confluentinc/cp-kafka`

### Local Kafka Connect
* Open the Confluent Kafka Command Center GUI website
http://localhost:9021/clusters/
* Check running connector Check running connector status
```bash
curl -s "http://localhost:8083/connectors?expand=info&expand=status" | \
      jq '. | to_entries[] | [ .value.info.type, .key, .value.status.connector.state,.value.status.tasks[].state,.value.info.config."connector.class"]|join(":|:")' | \
      column -s : -t| sed 's/\"//g'| sort
```
* curl -s "http://localhost:8083/connectors"
* Check running connector status
    * `curl -s "http://localhost:8083/connectors/<connector_x>/status"|jq '.'`
* Delete a connector 
    * `curl -X DELETE http://localhost:8083/connectors/<connector_x>`

* Example connector configuration JSON
```bash
{"name": "<name_of_the_connector>",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": <number_of_tasks>,
    "connection.url": "jdbc:oracle:thin:@<host_name>:<host_port>/<service_name>",
    "connection.user": "user_name",
    "connection.password": "<password>",
    "table.whitelist": "<list_of_tables_to_be_polled>",
    "mode": "<poll_type>",
    "incrementing.column.name": "<col_name_to_monitor>",
    "topic.prefix": "<topic_prefix_for_each_table>"
  }
}
```
 
* Connection URL examples
    * General - jdbc:oracle:thin:@<host>:<port>:<SID> 
    * EC2 - "connection.url": "jdbc:oracle:thin:@exacc.dataplatforms.dtci.technology:8685/edwdev"
    * LOCAL - "connection.url": "jdbc:oracle:thin:@brscc02-scan1.us1.ocm.s7358785.oraclecloudatcustomer.com:8685/edwdev",
        
* Add a connector to a local docker Kafka Connect Container
```json
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
        "name": "jdbc_oracle_01",
        "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "connection.url": "jdbc:oracle:thin:@brscc02-scan1.us1.ocm.s7358785.oraclecloudatcustomer.com:8685/edwdev",
                "connection.user": "LNDCDC_MASTER",
                "connection.password": "<password>",
                "poll.interval.ms" : 6000,
                "table.whitelist" : "CLOUD_MASTER_CDC",
                "topic.prefix": "oracle-01-",
                "timestamp.column.name": "TRG_CRT_DT_PART",
                "validate.non.null": false,
                "catalog.pattern" : "LNDCDC_MASTER",
                "mode": "timestamp",
                "numeric.mapping": "best_fit",
                }
        }'
```

* Properties to test
"key.converter": "org.apache.kafka.connect.json.JsonConverter",
"key.converter.schemas.enable": "true",
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "true",

* Using AWS Connection URL
```json
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
        "name": "oracle_01",
        "config": {
                "tasks.max": "1",
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "connection.url": "jdbc:oracle:thin:@exacc.dataplatforms.dtci.technology:8685/edwdev",
                "dialect.name" : "OracleDatabaseDialect",
                "connection.user": "QABI_USER",
                "connection.password": "<password>",
                "topic.prefix": "oracle-01-",
                "table.whitelist" : "cloud_master_cdc",
                "mode":"timestamp+incrementing",
                "poll.interval.ms" : 3600000,
                "topic.prefix": "oracle-01-",
                "incrementing.column.name": "CLOUD_MASTER_CDC_ID",
                "timestamp.column.name": "TGT_CRT_DT_PART",
                "validate.non.null": false,
                "transforms" : "createKey",
                "transforms.createKey.type" : "org.apache.kafka.connect.transforms.ValueToKey",
                "transforms.createKey.fields" : "ID"
                }
        }'
```
  
### Attach to ksql docker container, to check existing topics
docker exec -it ksqldb-server ksql http://ksqldb-server:8088
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
docker exec -it ksqldb ksql http://ksqldb:8088
      
Used EC2 instance i-0bf73f3e10f0ed91f

### Optional - Oracle Docker Image
https://github.com/oracle/docker-images/blob/master/OracleDatabase/SingleInstance/README.md
* Running locally with local oracle
* Run the oracle docker DB locally  
```
docker run --name oracle_db \
-p <host port>:1521 -p <host port>:5500 \
-e ORACLE_SID=<your SID> \
-e ORACLE_PDB=<your PDB name> \
-e ORACLE_PWD=<your database passwords> \
-e INIT_SGA_SIZE=<your database SGA memory in MB> \
-e INIT_PGA_SIZE=<your database PGA memory in MB> \
-e ORACLE_EDITION=<your database edition> \
-e ORACLE_CHARACTERSET=<your character set> \
-v [<host mount point>:]/opt/oracle/oradata \
oracle/database:19.3.0-ee
```

```
docker run --name oracle_db \
-p 1521:1521 -p 5500:5500 \
-e ORACLE_SID=ORCLCDB \
-e ORACLE_PDB=ORCLPDB1 \
-e ORACLE_PWD=dolphin \
-e ORACLE_EDITION=19.3.0-ee \
-e ORACLE_CHARACTERSET=AL32UTF8 \
oracle/database:19.3.0-ee
```

* Adding connector to local Oracle DB
```json
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
                "name": "jdbc_source_oracle_01",
                "config": {
                        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                        "connection.url": "jdbc:oracle:thin:@oracle:1521/ORCLPDB1",
                        "connection.user": "connect_user",
                        "connection.password": "asgard",
                        "topic.prefix": "oracle-01-",
                        "table.whitelist" : "NUM_TEST",
                        "mode":"bulk",
                        "poll.interval.ms" : 3600000
                        }
                }'
```
