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
package io.arenadata.kafka.postgres.writer.converter;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

@Service
public class ToSqlConverterService {
    private final Map<String, Converter> converters;

    public ToSqlConverterService(List<Converter> converters) {
        this.converters = converters.stream()
                .collect(Collectors.toMap(Converter::getLogicalType, o -> o));
    }

    public Object convert(Object value, Schema.Field field) {
        if (field == null) {
            return value;
        }

        List<Schema> types = getTypes(field.schema());
        Optional<LogicalType> logicalType = types.stream()
                .map(Schema::getLogicalType)
                .filter(Objects::nonNull)
                .findFirst();
        if (logicalType.isPresent()) {
            String logicalTypeName = logicalType.get().getName().toLowerCase(Locale.ROOT);
            if (converters.containsKey(logicalTypeName)) {
                return converters.get(logicalTypeName).convert(value);
            }
        }

        return value;
    }


    private List<Schema> getTypes(Schema schema) {
        if (schema.isUnion()) {
            return schema.getTypes();
        }

        return singletonList(schema);
    }
}
