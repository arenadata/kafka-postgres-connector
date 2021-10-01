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
package io.arenadata.kafka.postgres.util;

import lombok.val;

import java.time.*;
import java.util.TimeZone;

public final class DateTimeUtils {
    private static final TimeZone TIME_ZONE = TimeZone.getTimeZone("UTC");
    private static final ZoneId ZONE_ID = TIME_ZONE.toZoneId();

    private DateTimeUtils() {
    }

    public static Long toMicros(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }

        val dateTime = localDateTime.atZone(ZONE_ID);
        return dateTime.toEpochSecond() * 1000_000L + dateTime.getNano() / 1000L;
    }

    public static LocalDateTime toLocalDateTime(Long micros) {
        if (micros == null) {
            return null;
        }

        long seconds = micros / 1000_000L;
        int nanos = (int) (micros % 1000_000L) * 1000;
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZONE_ID);
    }

    public static Long toMicros(LocalTime localTime) {
        if (localTime == null) {
            return null;
        }

        return localTime.toNanoOfDay() / 1000L;
    }

    public static LocalTime toLocalTime(Long micros) {
        if (micros == null) {
            return null;
        }

        return LocalTime.ofNanoOfDay(micros * 1000L);
    }

    public static Long toEpochDay(LocalDate localDate) {
        if (localDate == null) {
            return null;
        }

        return localDate.toEpochDay();
    }

    public static LocalDate toLocalDate(Number epochDay) {
        if (epochDay == null) {
            return null;
        }

        return LocalDate.ofEpochDay(epochDay.longValue());
    }
}
