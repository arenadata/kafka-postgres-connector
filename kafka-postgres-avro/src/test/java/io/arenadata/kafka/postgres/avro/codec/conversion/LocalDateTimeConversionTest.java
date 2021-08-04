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
        assertEquals(dateTime, time.atOffset(ZoneOffset.of("Z")).atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime());
    }

    @Test
    public void localDateTimeToMicros() {
        Long micros = localDateTimeConversion.toLong(time.atOffset(ZoneOffset.of("Z")).atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime(), null, null);
        assertEquals(micros, ALMOST_2ND_OF_JAN_1970);
    }

}
