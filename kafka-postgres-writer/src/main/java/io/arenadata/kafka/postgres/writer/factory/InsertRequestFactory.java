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
package io.arenadata.kafka.postgres.writer.factory;

import io.arenadata.kafka.postgres.writer.converter.ToSqlConverterService;
import io.arenadata.kafka.postgres.writer.model.InsertDataContext;
import io.arenadata.kafka.postgres.writer.model.sql.PostgresInsertSqlRequest;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.ArrayTuple;
import lombok.val;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
public class InsertRequestFactory {
    private static final String TEMPLATE_SQL = "insert into %s.%s (%s) values (%s)";
    private final ToSqlConverterService toSqlConverterService;

    public InsertRequestFactory(ToSqlConverterService toSqlConverterService) {
        this.toSqlConverterService = toSqlConverterService;
    }

    public PostgresInsertSqlRequest create(InsertDataContext context, List<GenericRecord> rows) {
        if (!rows.isEmpty()) {
            return createSqlRequest(context, rows);
        } else return new PostgresInsertSqlRequest(context.getInsertSql(), new ArrayList<>());
    }

    private PostgresInsertSqlRequest createSqlRequest(InsertDataContext context, List<GenericRecord> rows) {
        val fields = context.getRequest().getSchema().getFields();
        val batch = getParams(rows, fields);
        return new PostgresInsertSqlRequest(context.getInsertSql(), batch);
    }

    private List<Tuple> getParams(List<GenericRecord> rows, List<Schema.Field> fields) {
        return rows.stream()
                .map(row -> getTupleParam(fields, row))
                .collect(Collectors.toList());
    }

    private Tuple getTupleParam(List<Schema.Field> fields, GenericRecord row) {
        val params = new ArrayList<>();

        for (int i = 0; i < fields.size(); i++) {
            Object item = row.get(i);
            Schema.Field field = fields.get(i);
            params.add(toSqlConverterService.convert(item, field));
        }

        return new ArrayTuple(params);
    }

    public String getSql(InsertDataContext context) {
        val fields = context.getRequest().getSchema().getFields();
        return getInsertSql(context, fields);
    }

    private String getInsertSql(InsertDataContext context, List<Schema.Field> fields) {
        val insertColumns = fields.stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());
        val insertValues = IntStream.range(1, fields.size() + 1)
                .mapToObj(pos -> "$" + pos)
                .collect(Collectors.toList());
        val request = context.getRequest();
        return String.format(TEMPLATE_SQL, request.getDatamart(), request.getTableName(),
                String.join(",", insertColumns), String.join(",", insertValues));
    }
}
