// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** Unit tests for topic value types and layout constants. No native library required. */
final class TopicBindingTest {

    // ── TopicConfig ──────────────────────────────────────────────────────

    @Test
    void topicConfigDefault() {
        assertEquals("FAST_DAILY", TopicConfig.DEFAULT.rollScheme());
        assertEquals(0, TopicConfig.DEFAULT.retentionCycles());
        assertEquals(0, TopicConfig.DEFAULT.flags());
    }

    @Test
    void topicConfigValidatesRollScheme() {
        // Given: a valid roll scheme
        TopicConfig cfg = new TopicConfig("DAILY", 1, 0);
        assertEquals("DAILY", cfg.rollScheme());
        assertEquals(1, cfg.retentionCycles());
        assertEquals(0, cfg.flags());
    }

    @Test
    void topicConfigRejectsNullRollScheme() {
        assertThrows(NullPointerException.class, () ->
            new TopicConfig(null, 0, 0)
        );
    }

    @Test
    void topicConfigRejectsEmptyRollScheme() {
        assertThrows(IllegalArgumentException.class, () ->
            new TopicConfig("", 0, 0)
        );
    }

    @Test
    void topicConfigRejectsLongRollScheme() {
        assertThrows(IllegalArgumentException.class, () ->
            new TopicConfig("12345678901234567", 0, 0)
        );
    }

    @Test
    void topicConfigAcceptsMaxLengthRollScheme() {
        TopicConfig cfg = new TopicConfig("1234567890123456", 0, 0);
        assertEquals(16, cfg.rollScheme().length());
    }

    @Test
    void topicConfigRejectsNegativeRetention() {
        assertThrows(IllegalArgumentException.class, () ->
            new TopicConfig("FAST_DAILY", -1, 0)
        );
    }

    // ── TopicStart ───────────────────────────────────────────────────────

    @Test
    void topicStartNativeValues() {
        assertEquals(0, TopicStart.EARLIEST.nativeValue());
        assertEquals(1, TopicStart.LATEST.nativeValue());
    }

    // ── TopicAckMode ─────────────────────────────────────────────────────

    @Test
    void topicAckModeNativeValues() {
        assertEquals(0, TopicAckMode.FIRE_AND_FORGET.nativeValue());
        assertEquals(1, TopicAckMode.REPLICATE_ONCE.nativeValue());
    }

    // ── TopicPublishResult ──────────────────────────────────────────────

    @Test
    void topicPublishResultOk() {
        assertTrue(new TopicPublishResult(RingloomStatus.OK, 42L).ok());
        assertFalse(
            new TopicPublishResult(RingloomStatus.INVALID_ARGUMENT, 0L).ok()
        );
    }

    @Test
    void topicPublishResultFields() {
        TopicPublishResult result = new TopicPublishResult(
            RingloomStatus.OK,
            99L
        );
        assertEquals(RingloomStatus.OK, result.status());
        assertEquals(99L, result.publishIndex());
    }

    // ── TopicPollResult ──────────────────────────────────────────────────

    @Test
    void topicPollResultInitialState() {
        TopicPollResult out = new TopicPollResult();
        assertEquals(0L, out.index());
        assertEquals(MemorySegment.NULL, out.payloadSegment());
    }

    @Test
    void topicPollResultRefreshFromNative() {
        TopicPollResult out = new TopicPollResult();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment fakePayload = arena.allocateFrom(
                ValueLayout.JAVA_BYTE,
                new byte[] { 1, 2, 3 }
            );
            out.refreshFromNative(fakePayload, 3L, 42L);
            assertEquals(42L, out.index());
            MemorySegment seg = out.payloadSegment();
            assertEquals(3L, seg.byteSize());
            assertEquals((byte) 1, seg.get(ValueLayout.JAVA_BYTE, 0));
            assertEquals((byte) 2, seg.get(ValueLayout.JAVA_BYTE, 1));
            assertEquals((byte) 3, seg.get(ValueLayout.JAVA_BYTE, 2));
        }
    }

    @Test
    void topicPollResultNullPayload() {
        TopicPollResult out = new TopicPollResult();
        out.refreshFromNative(MemorySegment.NULL, 0L, 0L);
        assertEquals(0L, out.index());
        assertEquals(MemorySegment.NULL, out.payloadSegment());
    }

    // ── RingloomStatus ───────────────────────────────────────────────────

    @Test
    void notReadyStatus() {
        assertEquals(11, RingloomStatus.NOT_READY);
        assertFalse(RingloomStatus.isOk(RingloomStatus.NOT_READY));
    }

    // ── Layout constants ─────────────────────────────────────────────────

    @Test
    void topicConfigSizeMatchesNativeStruct() {
        // ringloom_topic_config_t: u32 size + char[16] roll_scheme + u32 retention_cycles + u32 flags
        assertEquals(28L, RingloomNative.TOPIC_CONFIG_SIZE);
    }

    @Test
    void topicConfigLayoutEncodeDecodeRoundtrip() {
        // Given: a TopicConfig with known values
        TopicConfig cfg = new TopicConfig("TEST_DAILY", 7, 0);

        // When: we encode it into a native layout
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeCfg = arena.allocate(
                RingloomNative.TOPIC_CONFIG_SIZE,
                8
            );
            byte[] rollBytes = cfg
                .rollScheme()
                .getBytes(StandardCharsets.UTF_8);

            // Encode
            nativeCfg.set(
                ValueLayout.JAVA_INT,
                RingloomNative.TOPIC_CONFIG_SIZE_OFFSET,
                (int) RingloomNative.TOPIC_CONFIG_SIZE
            );
            for (int i = 0; i < rollBytes.length && i < 16; i++) {
                nativeCfg.set(
                    ValueLayout.JAVA_BYTE,
                    RingloomNative.TOPIC_CONFIG_ROLL_SCHEME_OFFSET + i,
                    rollBytes[i]
                );
            }
            nativeCfg.set(
                ValueLayout.JAVA_INT,
                RingloomNative.TOPIC_CONFIG_RETENTION_OFFSET,
                cfg.retentionCycles()
            );
            nativeCfg.set(
                ValueLayout.JAVA_INT,
                RingloomNative.TOPIC_CONFIG_FLAGS_OFFSET,
                cfg.flags()
            );

            // Then: we can decode the same values back
            assertEquals(
                (int) RingloomNative.TOPIC_CONFIG_SIZE,
                nativeCfg.get(
                    ValueLayout.JAVA_INT,
                    RingloomNative.TOPIC_CONFIG_SIZE_OFFSET
                )
            );
            // Read roll scheme back
            byte[] decodedRoll = new byte[16];
            for (int i = 0; i < 16; i++) {
                decodedRoll[i] = nativeCfg.get(
                    ValueLayout.JAVA_BYTE,
                    RingloomNative.TOPIC_CONFIG_ROLL_SCHEME_OFFSET + i
                );
            }
            // Find the null terminator or end of used bytes
            int nameEnd = 0;
            while (nameEnd < 16 && decodedRoll[nameEnd] != 0) {
                nameEnd++;
            }
            String decodedName = new String(
                decodedRoll,
                0,
                nameEnd,
                StandardCharsets.UTF_8
            );
            assertEquals("TEST_DAILY", decodedName);

            assertEquals(
                7,
                nativeCfg.get(
                    ValueLayout.JAVA_INT,
                    RingloomNative.TOPIC_CONFIG_RETENTION_OFFSET
                )
            );
            assertEquals(
                0,
                nativeCfg.get(
                    ValueLayout.JAVA_INT,
                    RingloomNative.TOPIC_CONFIG_FLAGS_OFFSET
                )
            );
        }
    }

    @Test
    void topicConfigLayoutOffsets() {
        assertEquals(0L, RingloomNative.TOPIC_CONFIG_SIZE_OFFSET);
        assertEquals(4L, RingloomNative.TOPIC_CONFIG_ROLL_SCHEME_OFFSET);
        assertEquals(20L, RingloomNative.TOPIC_CONFIG_RETENTION_OFFSET);
        assertEquals(24L, RingloomNative.TOPIC_CONFIG_FLAGS_OFFSET);
    }
}
