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
package io.arenadata.kafka.postgres.avro.codec.type;

import org.apache.avro.LogicalType;

public class LocalDateTimeLogicalType extends LogicalType {
    public static final LocalDateTimeLogicalType INSTANCE = new LocalDateTimeLogicalType();
    private static final String TYPE_NAME = "timestamp-micros";

    public LocalDateTimeLogicalType() {
        super(TYPE_NAME);
    }

}