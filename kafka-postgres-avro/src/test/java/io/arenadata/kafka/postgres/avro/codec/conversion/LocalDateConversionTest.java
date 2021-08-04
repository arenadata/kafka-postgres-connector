package io.arenadata.kafka.postgres.avro.codec.conversion;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocalDateConversionTest {

    LocalDateConversion localDateConversion = LocalDateConversion.getInstance();

    private final LocalDate FIRST_DAY_OF_EPOCH = LocalDate.of(1970, 1, 1);

    @Test
    public void daysToLocalDate() {
        LocalDate date = localDateConversion.fromLong(0L, null, null);
        assertEquals(date, FIRST_DAY_OF_EPOCH);
    }

    @Test
    public void localDateToDays() {
        Long day = localDateConversion.toLong(FIRST_DAY_OF_EPOCH, null, null);
        assertEquals(0L, day);
    }

}
