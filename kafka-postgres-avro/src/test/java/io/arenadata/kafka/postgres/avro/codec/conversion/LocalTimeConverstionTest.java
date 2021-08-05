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

import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocalTimeConverstionTest {

    private final LocalTimeConversion localTimeConversion = LocalTimeConversion.getInstance();

    private final Long TEN_MINUTES_AS_MICROS = 600000001L; // 00:10:00.000001
    private final LocalTime TEN_MINUTES_AS_LOCAL_TIME = LocalTime.of(0, 10, 0, 1000);

    @Test
    public void microsToLocalTime() {
        LocalTime time = localTimeConversion.fromLong(TEN_MINUTES_AS_MICROS, null, null);
        assertEquals(time, TEN_MINUTES_AS_LOCAL_TIME);
    }

    @Test
    public void localTimeToMicros() {
        Long millis = localTimeConversion.toLong(TEN_MINUTES_AS_LOCAL_TIME, null, null);
        assertEquals(millis, TEN_MINUTES_AS_MICROS);
    }

}
