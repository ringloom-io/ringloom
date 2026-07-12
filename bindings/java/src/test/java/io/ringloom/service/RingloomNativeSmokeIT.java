package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

final class RingloomNativeSmokeIT {
    @Test
    void loadsNativeLibraryAndExposesAbiBasics() {
        assertEquals(5, RingloomNative.abiVersion());
        assertFalse(RingloomNative.statusName(RingloomStatus.OK).isEmpty());
    }

    @Test
    void invalidStartArgumentsReturnInvalidArgument() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment config = arena.allocate(RingloomNative.SERVICE_CONFIG_SIZE, 8);
            MemorySegment outService = arena.allocate(RingloomNative.ADDRESS);
            outService.set(RingloomNative.ADDRESS, 0, MemorySegment.NULL);

            int status = RingloomNative.serviceStart(config, outService);
            assertEquals(RingloomStatus.INVALID_ARGUMENT, status);
        }
    }
}
