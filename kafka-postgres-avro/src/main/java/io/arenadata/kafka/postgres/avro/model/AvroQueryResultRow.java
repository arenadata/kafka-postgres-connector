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
package io.arenadata.kafka.postgres.avro.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;

import java.util.ArrayList;
import java.util.List;

public class AvroQueryResultRow implements IndexedRecord, SpecificData.SchemaConstructable {

    private final Schema schema;
    private final List<?> inputRow;
    private final List<Object> outputRow = new ArrayList<>();

    public AvroQueryResultRow(Schema schema, List<?> inputRow) {
        this.schema = schema;
        this.inputRow = inputRow;
    }

    @Override
    public void put(int i, Object v) {
        if (outputRow.size() == i) {
            outputRow.add(v);
        } else {
            throw new IllegalArgumentException(String.format("Incorrect order: %d, %s", i, v));
        }
    }

    @Override
    public Object get(int i) {
        return inputRow.get(i);
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
