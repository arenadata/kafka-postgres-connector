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
package io.arenadata.kafka.postgres.avro.codec.conversion;

import io.arenadata.kafka.postgres.avro.codec.type.LocalDateTimeLogicalType;
import org.apache.avro.*;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.regex.Pattern;

public class LocalDateTimeConversion extends Conversion<LocalDateTime> {
    private static final Pattern TIME_ZONE_PATTERN
            = Pattern.compile("(?:Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9]|\\w+/\\w+)$");

    private LocalDateTimeConversion() {
        super();
    }

    public static LocalDateTimeConversion getInstance() {
        return LocalDateTimeConversionHolder.INSTANCE;
    }

    @Override
    public Class<LocalDateTime> getConvertedType() {
        return LocalDateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
        return LocalDateTimeLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return LocalDateTimeLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));
    }

    @Override
    public Long toLong(LocalDateTime value, Schema schema, LogicalType type) {
        long millisDelta = value.getNano() / 1000 / 1000;
        return value.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() * 1000 + millisDelta;
    }

    @Override
    public LocalDateTime fromLong(Long valueInMicros, Schema schema, LogicalType type) {
        long millis = valueInMicros / 1000;
        long deltaInMicros = valueInMicros - millis * 1000;
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()).plus(deltaInMicros, ChronoUnit.MICROS);
    }

    @Override
    public LocalDateTime fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return TIME_ZONE_PATTERN.matcher(value).find() ? ZonedDateTime.parse(value).toLocalDateTime() : LocalDateTime.parse(value);
    }

    @Override
    public CharSequence toCharSequence(LocalDateTime value, Schema schema, LogicalType type) {
        return value.toString();
    }

    private static class LocalDateTimeConversionHolder {
        private static final LocalDateTimeConversion INSTANCE = new LocalDateTimeConversion();
    }
}
