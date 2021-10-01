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

import org.junit.jupiter.api.Test;

import java.time.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocalDateTimeConversionTest {

    private final LocalDateTimeConversion localDateTimeConversion = LocalDateTimeConversion.getInstance();

    private final LocalDateTime time = LocalDateTime.of(1970, 1, 1, 23, 59, 59, 999999 * 1000);
    private final Long ALMOST_2ND_OF_JAN_1970 = 86399999999L; // 1970-01-01T23:59:59:999 UTC

    @Test
    public void microsToLocalDateTime() {
        LocalDateTime dateTime = localDateTimeConversion.fromLong(ALMOST_2ND_OF_JAN_1970, null, null);
        assertEquals(dateTime, time.atOffset(ZoneOffset.of("Z")).atZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime());
    }

    @Test
    public void localDateTimeToMicros() {
        Long micros = localDateTimeConversion.toLong(time.atOffset(ZoneOffset.of("Z")).atZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime(), null, null);
        assertEquals(micros, ALMOST_2ND_OF_JAN_1970);
    }

    @Test
    public void testConversionBackAndForth() {
        Long micros = 356832360060020L; // 1981-04-23T05:06:00.060020

        LocalDateTime dateTime = localDateTimeConversion.fromLong(micros, null, null);
        Long microsFromDateTime = localDateTimeConversion.toLong(dateTime, null, null);

        assertEquals(micros, microsFromDateTime);
    }

}
