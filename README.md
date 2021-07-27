# kafka-postgres-connector

## How to build

```shell script
mvn clean package
```

## How to run

Default configuration can be found in each project src/main/resources/application.yml it requires PostgreSQL connection.
Kafka configuration can be left as is (broker configuration coming with the request).

## Running kafka-postgres-writer:

```shell script
java -jar kafka-postgres-writer/target/kafka-postgres-writer-<version>.jar -Dspring.config.location=<path to config>/application-default.yml
# or
mvn spring-boot:run
```

After starting connector, available endpoints are:

```shell script
http://kafka-postgres-writer-host:8096/newdata/start [POST]
http://kafka-postgres-writer-host:8096/newdata/stop [POST]
http://kafka-postgres-writer-host:8096/version [GET]
```

## Running kafka-postgres-reader:

```shell script
java -jar kafka-postgres-reader/target/kafka-postgres-reader-<version>.jar -Dspring.config.location=<path to config>/application-default.yml
# or
mvn spring-boot:run
```

After starting connector, available endpoints are:

```shell script
http://kafka-postgres-reader-host:8094/query [POST]
http://kafka-postgres-reader-host:8094/versions [GET]
```