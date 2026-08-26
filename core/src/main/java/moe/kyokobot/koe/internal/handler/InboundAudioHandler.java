package moe.kyokobot.koe.internal.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import moe.kyokobot.koe.internal.MediaConnectionImpl;
import moe.kyokobot.libdave.DecryptorResultCode;
import moe.kyokobot.libdave.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Defensive RTP receive path executed entirely on Koe's UDP event loop. */
final class InboundAudioHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private static final Logger logger = LoggerFactory.getLogger(InboundAudioHandler.class);
    private static final int RTP_VERSION = 2;
    private static final int DISCORD_OPUS_PAYLOAD = 0x78;
    private static final int MAX_PACKET_BYTES = 8192;
    private static final int MAX_PENDING_UNKNOWN = 256;
    private static final long UNKNOWN_SSRC_WAIT_MILLIS = 250;

    private final DiscordUDPConnection udp;
    private final MediaConnectionImpl connection;
    private final AtomicInteger pendingUnknown = new AtomicInteger();

    InboundAudioHandler(DiscordUDPConnection udp, MediaConnectionImpl connection) {
        this.udp = udp;
        this.connection = connection;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket datagram) {
        var packet = datagram.content();
        if (!isPlausibleRtp(packet)) {
            // Preserve Koe's existing downstream RTCP path. This handler owns
            // the original datagram through SimpleChannelInboundHandler, so a
            // retained reference is required when forwarding it.
            ctx.fireChannelRead(datagram.retain());
            return;
        }
        int ssrc = packet.getInt(packet.readerIndex() + 8);
        if (ssrc == udp.getSsrc()) {
            return;
        }
        var userId = connection.getUserIdForAudioSsrc(ssrc);
        if (userId == null) {
            if (pendingUnknown.incrementAndGet() > MAX_PENDING_UNKNOWN) {
                pendingUnknown.decrementAndGet();
                return;
            }
            var retained = packet.retainedDuplicate();
            var receivedNanos = System.nanoTime();
            ctx.executor().schedule(() -> {
                try {
                    var mapped = connection.getUserIdForAudioSsrc(ssrc);
                    if (mapped != null) {
                        process(retained, mapped, receivedNanos);
                    }
                } finally {
                    retained.release();
                    pendingUnknown.decrementAndGet();
                }
            }, UNKNOWN_SSRC_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            return;
        }
        process(packet, userId, System.nanoTime());
    }

    private boolean isPlausibleRtp(ByteBuf packet) {
        int length = packet.readableBytes();
        if (length < 12 || length > MAX_PACKET_BYTES) {
            return false;
        }
        int offset = packet.readerIndex();
        int first = packet.getUnsignedByte(offset);
        int payload = packet.getUnsignedByte(offset + 1);
        return (first >>> 6) == RTP_VERSION && (payload & 0x7f) == DISCORD_OPUS_PAYLOAD;
    }

    private void process(ByteBuf packet, String userId, long receivedNanos) {
        int offset = packet.readerIndex();
        int first = packet.getUnsignedByte(offset);
        int payload = packet.getUnsignedByte(offset + 1);
        int csrcCount = first & 0x0f;
        boolean hasExtension = (first & 0x10) != 0;
        int headerLength = 12 + csrcCount * 4;
        if (packet.readableBytes() < headerLength) {
            return;
        }
        int[] csrcs = new int[csrcCount];
        for (int index = 0; index < csrcCount; index++) {
            csrcs[index] = packet.getInt(offset + 12 + index * 4);
        }
        int extensionProfile = 0;
        int extensionWords = 0;
        if (hasExtension) {
            if (packet.readableBytes() < headerLength + 4) {
                return;
            }
            extensionProfile = packet.getUnsignedShort(offset + headerLength);
            extensionWords = packet.getUnsignedShort(offset + headerLength + 2);
            headerLength += 4;
        }

        var transportClear = udp.allocator().directBuffer();
        var opus = udp.allocator().directBuffer();
        try {
            var mode = udp.getEncryptionMode();
            var secretKey = udp.getSecretKey();
            if (mode == null || secretKey == null
                    || !mode.unbox(packet, headerLength, transportClear, secretKey)) {
                return;
            }
            int extensionBytes = extensionWords * 4;
            if (extensionBytes > transportClear.readableBytes()) {
                return;
            }
            transportClear.skipBytes(extensionBytes);

            var dave = connection.getDAVEManager();
            boolean daveEnabled = dave != null && dave.getCurrentProtocolVersion() > 0;
            if (dave != null) {
                int result = dave.decrypt(userId, MediaType.AUDIO, opus, transportClear);
                if (result != DecryptorResultCode.SUCCESS.getValue()) {
                    logger.debug("DAVE receive failed for guild={} user={} ssrc={} code={}",
                            connection.getGuildId(), userId,
                            Integer.toUnsignedLong(packet.getInt(offset + 8)), result);
                    return;
                }
            } else {
                opus.writeBytes(transportClear, transportClear.readerIndex(),
                        transportClear.readableBytes());
            }
            if (!opus.isReadable()) {
                return;
            }
            byte[] opusBytes = new byte[opus.readableBytes()];
            opus.getBytes(opus.readerIndex(), opusBytes);
            connection.dispatchInboundAudio(
                    packet.getInt(offset + 8),
                    packet.getUnsignedShort(offset + 2),
                    packet.getUnsignedInt(offset + 4),
                    receivedNanos,
                    (payload & 0x80) != 0,
                    daveEnabled,
                    csrcs,
                    extensionProfile,
                    extensionWords,
                    opusBytes);
        } catch (RuntimeException exception) {
            logger.debug("Discarding malformed inbound Discord RTP packet", exception);
        } finally {
            transportClear.release();
            opus.release();
        }
    }
}
