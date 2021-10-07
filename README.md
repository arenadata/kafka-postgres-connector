# kafka-postgres-connector
An obligatory supporting service to the [Prostore main service](https://github.com/arenadata/prostore) `dtm-query-execution-core` that
communicates with the respective DBMS.

## Useful links
[Documentation (Rus)](https://arenadata.github.io/docs_prostore/getting_started/getting_started.html)

## Local deployment

### The cloning and building of kafka-postgres-connector
```shell script
#clone
git clone https://github.com/arenadata/kafka-postgres-connector
# build without any tests 
cd ~/kafka-postgres-connector
mvn clean
mvn install -DskipTests=true
```

### Connectors configuration
The connector configuration files `application.yml` are located in the respective folders
`kafka-postgres-writer/src/main/resources/` and kafka-postgres-reader/src/main/resources/`.

To run connectors correctly one has to adjust respective configuration files for `kafka-postgres-writer` and `kafka-postgres-reader` to match the key values with the Prostore configuration, namely:
-    `datasource: postgres: database ~ env: name`,
-    `datasource: postgres: user     ~ adp: datasource: user`,
-    `datasource: postgres: password ~ adp: datasource: password`,
-    `datasource: postgres: hosts    ~ adp: datasource: host, adp: datasource: port`.

The connector services look for the configuration in the same subfolders (target) where `kafka-postgres-writer-<version>.jar` and `kafka-postgres-reader-<version>.jar` are executed.
So we create the respective symbolic links
```shell script
sudo ln -s ~/kafka-postgres-connector/kafka-postrges-writer/src/main/resources/application.yml ~/kafka-postgres-connector/kafka-postrges-writer/target/application.yml
sudo ln -s ~/kafka-postgres-connector/kafka-postrges-reader/src/main/resources/application.yml ~/kafka-postgres-connector/kafka-postrges-reader/target/application.yml
```

### Run services
#### Run the kafka-postgres-writer connector as a single jar
```shell script
cd ~/kafka-postgres-connector/kafka-postgres-writer/target
java -Dspring.profiles.active=default -jar kafka-postgres-writer-<version>.jar
```
#### Run the kafka-postgres-reader connector as a single jar
```shell script
cd ~/kafka-postgres-connector/kafka-postgres-reader/target
java -Dspring.profiles.active=default -jar kafka-postgres-reader-<version>.jar
```

### Available endpoints
#### kafka-postgres-writer
```shell script
http://kafka-postgres-writer-host:8096/newdata/start [POST]
http://kafka-postgres-writer-host:8096/newdata/stop [POST]
http://kafka-postgres-writer-host:8096/version [GET]
```

#### kafka-postgres-reader
```shell script
http://kafka-postgres-reader-host:8094/query [POST]
http://kafka-postgres-reader-host:8094/versions [GET]
```
