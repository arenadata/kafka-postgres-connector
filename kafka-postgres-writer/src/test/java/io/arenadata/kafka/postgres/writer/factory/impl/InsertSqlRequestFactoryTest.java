/*
 * Copyright © 2021 Arenadata Software LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.kafka.postgres.writer.factory.impl;

import io.arenadata.kafka.postgres.avro.codec.type.LocalDateLogicalType;
import io.arenadata.kafka.postgres.avro.codec.type.LocalDateTimeLogicalType;
import io.arenadata.kafka.postgres.avro.codec.type.LocalTimeLogicalType;
import io.arenadata.kafka.postgres.util.DateTimeUtils;
import io.arenadata.kafka.postgres.writer.converter.DateConverter;
import io.arenadata.kafka.postgres.writer.converter.TimeConverter;
import io.arenadata.kafka.postgres.writer.converter.TimestampConverter;
import io.arenadata.kafka.postgres.writer.converter.ToSqlConverterService;
import io.arenadata.kafka.postgres.writer.factory.InsertRequestFactory;
import io.arenadata.kafka.postgres.writer.model.InsertDataContext;
import io.arenadata.kafka.postgres.writer.model.InsertDataRequest;
import io.arenadata.kafka.postgres.writer.model.sql.PostgresInsertSqlRequest;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Tuple;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InsertSqlRequestFactoryTest {
    private static final String EXPECTED_SQL = "insert into test_datamart.test_table (id,name,date,time,timestamp) values ($1,$2,$3,$4,$5)";
    private final RoutingContext routingContext = mock(RoutingContext.class);
    private final ToSqlConverterService toSqlConverterService = new ToSqlConverterService(Arrays.asList(new TimeConverter(), new DateConverter(), new TimestampConverter()));
    private InsertRequestFactory factory;
    private InsertDataContext context;

    @BeforeEach
    public void before() {
        factory = new InsertRequestFactory(toSqlConverterService);

        HttpServerRequest httpServerRequest = mock(HttpServerRequest.class);
        when(routingContext.request()).thenReturn(httpServerRequest);

        val request = new InsertDataRequest();
        request.setDatamart("test_datamart");
        request.setKafkaTopic("kafka_topic");
        request.setTableName("test_table");

        Schema timestampType = LocalDateTimeLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));
        Schema dateType = LocalDateLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.INT));
        Schema timeType = LocalTimeLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));

        request.setSchema(SchemaBuilder.record("test").fields()
                .optionalInt("id")

                .name("name")
                .type()
                .nullable()
                .stringBuilder()
                .prop("avro.java.string", "String")
                .endString()
                .noDefault()

                .name("date")
                .type()
                .unionOf()
                .nullType()
                .and()
                .type(dateType)
                .endUnion()
                .noDefault()

                .name("time")
                .type()
                .unionOf()
                .nullType()
                .and()
                .type(timeType)
                .endUnion()
                .noDefault()

                .name("timestamp")
                .type()
                .unionOf()
                .nullType()
                .and()
                .type(timestampType)
                .endUnion()
                .noDefault()

                .endRecord());
        context = new InsertDataContext(request, routingContext);
        context.setKeyColumns(Arrays.asList("id", "name", "date", "timestamp", "timestamp_col"));
    }

    @Test
    void createInsertRequest() {
        // arrange
        context.setInsertSql(factory.getSql(context));

        // act
        PostgresInsertSqlRequest request = factory.create(context, getRows());

        // assert
        assertEquals(EXPECTED_SQL, request.getSql());
        assertEquals(10, request.getParams().size());
        Tuple tuple = request.getParams().get(0);
        assertEquals((Integer) 5, tuple.size());
        assertEquals(DateTimeUtils.toLocalDate(123), tuple.getValue(2));
        assertEquals(DateTimeUtils.toLocalTime(123L), tuple.getValue(3));
        assertEquals(DateTimeUtils.toLocalDateTime(123L), tuple.getValue(4));
    }

    private List<GenericRecord> getRows() {
        return IntStream.range(0, 10)
                .mapToObj(it -> getRow(it, "name_" + it))
                .collect(Collectors.toList());
    }

    private GenericData.Record getRow(int expectedId, String expectedName) {
        val schema = SchemaBuilder.record("test").fields()
                .optionalString("id")
                .optionalString("name")
                .optionalInt("date")
                .optionalLong("time")
                .optionalLong("timestamp")
                .endRecord();
        return new GenericRecordBuilder(schema)
                .set("id", expectedId)
                .set("name", expectedName)
                .set("date", 123)
                .set("time", 123L)
                .set("timestamp", 123L)
                .build();
    }
}
