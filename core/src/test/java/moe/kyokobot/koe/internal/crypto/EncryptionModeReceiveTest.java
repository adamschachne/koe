package moe.kyokobot.koe.internal.crypto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class EncryptionModeReceiveTest {
    private static final byte[] KEY = new byte[32];
    private static final byte[] OPUS = new byte[]{(byte) 0xf8, (byte) 0xff, (byte) 0xfe};

    static {
        for (int index = 0; index < KEY.length; index++) {
            KEY[index] = (byte) (index * 7 + 3);
        }
    }

    @Test
    void aesGcmRoundTripAuthenticatesRtpHeader() {
        assertRoundTrip(new AEADAES256GCMRTPSizeEncryptionMode());
    }

    @Test
    void xChaChaRoundTripAuthenticatesRtpHeader() {
        assertRoundTrip(new AEADXChaCha20Poly1305RTPSizeEncryptionMode());
    }

    @Test
    void aesGcmMatchesIndependentKnownAnswerVector() {
        assertKnownAnswer(
                new AEADAES256GCMRTPSizeEncryptionMode(),
                "807812340102030411223344d9aa235c1592dcfc5f441631a9a7ebaa57b2bb12d98101020304");
    }

    @Test
    void xChaChaMatchesIndependentKnownAnswerVector() {
        assertKnownAnswer(
                new AEADXChaCha20Poly1305RTPSizeEncryptionMode(),
                "8078123401020304112233448e97ee4d9c69d39235d4c2a35e24a73bd963e568af4f01020304");
    }

    @Test
    void tamperedHeaderIsRejected() {
        EncryptionMode mode = new AEADXChaCha20Poly1305RTPSizeEncryptionMode();
        ByteBuf packet = packetWithHeader();
        ByteBuf plain = Unpooled.wrappedBuffer(OPUS);
        ByteBuf output = Unpooled.buffer();
        try {
            assertTrue(mode.box(plain, OPUS.length, packet, KEY));
            packet.setByte(2, packet.getByte(2) ^ 1);
            assertFalse(mode.unbox(packet, 12, output, KEY));
        } finally {
            packet.release();
            plain.release();
            output.release();
        }
    }

    @Test
    void extensionPreambleIsAssociatedDataAndExtensionBodyIsEncrypted() {
        EncryptionMode mode = new AEADAES256GCMRTPSizeEncryptionMode();
        ByteBuf packet = packetWithHeader();
        packet.setByte(0, 0x90);
        packet.writeShort(0xbede);
        packet.writeShort(1);
        byte[] clear = new byte[]{1, 2, 3, 4, OPUS[0], OPUS[1], OPUS[2]};
        ByteBuf plain = Unpooled.wrappedBuffer(clear);
        ByteBuf output = Unpooled.buffer();
        try {
            assertTrue(mode.box(plain, clear.length, packet, KEY));
            assertTrue(mode.unbox(packet, 16, output, KEY));
            byte[] actual = new byte[output.readableBytes()];
            output.readBytes(actual);
            assertArrayEquals(clear, actual);
        } finally {
            packet.release();
            plain.release();
            output.release();
        }
    }

    private static void assertRoundTrip(EncryptionMode mode) {
        ByteBuf packet = packetWithHeader();
        ByteBuf plain = Unpooled.wrappedBuffer(Arrays.copyOf(OPUS, OPUS.length));
        ByteBuf output = Unpooled.buffer();
        try {
            assertTrue(mode.box(plain, OPUS.length, packet, KEY));
            assertTrue(mode.unbox(packet, 12, output, KEY));
            byte[] actual = new byte[output.readableBytes()];
            output.readBytes(actual);
            assertArrayEquals(OPUS, actual);
        } finally {
            packet.release();
            plain.release();
            output.release();
        }
    }

    private static void assertKnownAnswer(EncryptionMode mode, String packetHex) {
        byte[] key = new byte[32];
        for (int index = 0; index < key.length; index++) {
            key[index] = (byte) index;
        }
        byte[] expected = fromHex("f8fffe010203");
        ByteBuf packet = Unpooled.wrappedBuffer(fromHex(packetHex));
        ByteBuf output = Unpooled.buffer();
        try {
            assertTrue(mode.unbox(packet, 12, output, key));
            byte[] actual = new byte[output.readableBytes()];
            output.readBytes(actual);
            assertArrayEquals(expected, actual);
        } finally {
            packet.release();
            output.release();
        }
    }

    private static byte[] fromHex(String value) {
        if ((value.length() & 1) != 0) {
            throw new IllegalArgumentException("hex input must contain whole bytes");
        }
        byte[] result = new byte[value.length() / 2];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) Integer.parseInt(value.substring(index * 2, index * 2 + 2), 16);
        }
        return result;
    }

    private static ByteBuf packetWithHeader() {
        ByteBuf packet = Unpooled.buffer();
        packet.writeByte(0x80);
        packet.writeByte(0x78);
        packet.writeShort(42);
        packet.writeInt(960);
        packet.writeInt(1234);
        return packet;
    }
}
