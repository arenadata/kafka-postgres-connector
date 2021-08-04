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
