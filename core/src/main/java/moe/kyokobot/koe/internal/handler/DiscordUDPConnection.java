package moe.kyokobot.koe.internal.handler;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.DatagramChannel;
import moe.kyokobot.koe.codec.CodecInfo;
import moe.kyokobot.koe.codec.CodecInstance;
import moe.kyokobot.koe.codec.CodecRegistry;
import moe.kyokobot.koe.codec.CodecType;
import moe.kyokobot.koe.handler.ConnectionHandler;
import moe.kyokobot.koe.internal.MediaConnectionImpl;
import moe.kyokobot.koe.internal.NettyBootstrapFactory;
import moe.kyokobot.koe.internal.crypto.EncryptionMode;
import moe.kyokobot.koe.internal.json.JsonObject;
import moe.kyokobot.koe.internal.util.RTPHeaderWriter;
import moe.kyokobot.libdave.EncryptorResultCode;
import moe.kyokobot.libdave.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;

public class DiscordUDPConnection implements Closeable, ConnectionHandler<InetSocketAddress> {
    private static final Logger logger = LoggerFactory.getLogger(DiscordUDPConnection.class);

    private final MediaConnectionImpl connection;
    private final ByteBufAllocator allocator;
    private final SocketAddress serverAddress;
    private final Bootstrap bootstrap;
    private final int ssrc;
    private final long channelId;
    private final long transportGeneration;

    private volatile EncryptionMode encryptionMode;
    private volatile DatagramChannel channel;
    private volatile byte[] secretKey;

    private final AtomicLong udpPackets = new AtomicLong();
    private final AtomicLong plausibleAudioPackets = new AtomicLong();
    private final AtomicLong outboundSsrcPackets = new AtomicLong();
    private final AtomicLong unknownSsrcPackets = new AtomicLong();
    private final AtomicLong transportDecryptFailures = new AtomicLong();
    private final AtomicLong daveDecryptFailures = new AtomicLong();
    private final AtomicLong dispatchedFrames = new AtomicLong();
    private final AtomicLong outboundFrameAttempts = new AtomicLong();
    private final AtomicLong outboundDaveFailures = new AtomicLong();
    private final AtomicLong outboundTransportFailures = new AtomicLong();
    private final AtomicLong outboundPacketBuildFailures = new AtomicLong();
    private final AtomicLong outboundPackets = new AtomicLong();
    private final AtomicLong lastUdpPacketNanos = new AtomicLong();
    private final AtomicLong lastDispatchedFrameNanos = new AtomicLong();

    private char seq;

    public DiscordUDPConnection(MediaConnectionImpl voiceConnection,
                                SocketAddress serverAddress,
                                int ssrc, long channelId, long transportGeneration) {
        this.connection = voiceConnection;
        this.allocator = voiceConnection.getOptions().getByteBufAllocator();
        this.serverAddress = Objects.requireNonNull(serverAddress);
        this.bootstrap = NettyBootstrapFactory.datagram(voiceConnection.getOptions());
        this.ssrc = ssrc;
        this.channelId = channelId;
        this.transportGeneration = transportGeneration;
        // should be a random value https://tools.ietf.org/html/rfc1889#section-5.1
        this.seq = (char) (ThreadLocalRandom.current().nextInt() & 0xffff);
    }

    @Override
    public CompletionStage<InetSocketAddress> connect() {
        logger.debug("Connecting to {}...", serverAddress);

        var future = new CompletableFuture<InetSocketAddress>();
        bootstrap.handler(new Initializer(this, future))
                .connect(serverAddress)
                .addListener(res -> {
                    if (!res.isSuccess()) {
                        future.completeExceptionally(res.cause());
                    }
                });
        return future;
    }

    @Override
    public void close() {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    @Override
    public void handleSessionDescription(JsonObject object) {
        var mode = object.getString("mode");
        var audioCodecName = object.getString("audio_codec");

        encryptionMode = EncryptionMode.get(mode);
        CodecInstance audioCodec = null;
        if (audioCodecName != null) {
            CodecRegistry registry = connection.getOptions().getCodecRegistry();
            CodecInfo audioCodecInfo = registry.getByName(audioCodecName);
            if (audioCodecInfo != null) {
                audioCodec = audioCodecInfo.instantiate();
            } else {
                logger.warn("Unsupported audio codec type: {}, no audio data will be polled", audioCodecName);
            }
        }

        if (encryptionMode == null) {
            throw new IllegalStateException("Encryption mode selected by Discord is not supported by Koe or the " +
                    "protocol changed! Open an issue at https://github.com/KyokoBot/koe");
        }

        var keyArray = object.getArray("secret_key");
        this.secretKey = new byte[keyArray.size()];

        for (int i = 0; i < secretKey.length; i++) {
            this.secretKey[i] = (byte) (keyArray.getInt(i) & 0xff);
        }

        connection.startAudioFramePolling();
        connection.startVideoFramePolling();
    }

    @Override
    public void sendFrame(CodecType codecType, byte payloadType, int timestamp, ByteBuf data, int len, boolean extension) {
        outboundFrameAttempts.incrementAndGet();
        var buf = createPacket(codecType, payloadType, timestamp, data, len, extension);
        if (buf != null) {
            outboundPackets.incrementAndGet();
            channel.writeAndFlush(buf);
        }
    }

    public ByteBuf createPacket(CodecType codecType, byte payloadType, int timestamp, ByteBuf data, int len, boolean extension) {
        if (secretKey == null) {
            return null;
        }

        var mediaType = codecType == CodecType.AUDIO ? MediaType.AUDIO : MediaType.VIDEO;

        ByteBuf buf = null;
        var inputBuffer = data;
        var inputLen = len;
        var inputBufferIsOwned = false; // true if we allocated inputBuffer (DAVE path)

        try {
            buf = allocator.directBuffer();
            buf.clear();
            var dave = connection.getDAVEManager();
            if (dave != null) {
                inputBuffer = allocator.directBuffer();
                inputBufferIsOwned = true;
                var result = dave.encrypt(mediaType, ssrc, inputBuffer, data, len);
                inputLen = inputBuffer.readableBytes();

                if (result < 0) {
                    logOutboundDrop(outboundDaveFailures.incrementAndGet(),
                            "DAVE encrypt", result);
                    return null;
                }
            } else {
                inputBuffer.retain();
                inputBufferIsOwned = true; // we retained, so we must release
            }

            RTPHeaderWriter.writeV2(buf, payloadType, nextSeq(), timestamp, ssrc, extension);
            if (encryptionMode.box(inputBuffer, inputLen, buf, secretKey)) {
                inputBuffer.release();
                inputBufferIsOwned = false;

                var result = buf;
                buf = null; // do not release in finally — caller owns it
                return result;
            }

            logger.debug("Encryption failed!");
            logOutboundDrop(outboundTransportFailures.incrementAndGet(),
                    "transport encrypt", 0);
            return null;
        } catch (Exception e) {
            long count = outboundPacketBuildFailures.incrementAndGet();
            if (count == 1 || count == 10 || count == 100 || count % 1000 == 0) {
                logger.warn("Outbound audio packet build failed guild={} count={}",
                        connection.getGuildId(), count, e);
            }
            return null;
        } finally {
            if (buf != null && buf.refCnt() > 0) {
                buf.release();
            }

            if (inputBufferIsOwned && inputBuffer != null && inputBuffer.refCnt() > 0) {
                inputBuffer.release();
            }
        }
    }

    public char nextSeq() {
        if ((seq + 1) > 0xffff) {
            seq = 0;
        } else {
            seq++;
        }

        return seq;
    }

    public byte[] getSecretKey() {
        return secretKey;
    }

    public int getSsrc() {
        return ssrc;
    }

    public long getChannelId() {
        return channelId;
    }

    public long getTransportGeneration() {
        return transportGeneration;
    }

    public EncryptionMode getEncryptionMode() {
        return encryptionMode;
    }

    public SocketAddress getServerAddress() {
        return serverAddress;
    }

    ByteBufAllocator allocator() {
        return allocator;
    }

    public void executeOnEventLoop(Runnable operation) {
        var current = channel;
        if (current == null || !current.isActive()) {
            throw new IllegalStateException("Discord UDP transport is not active");
        }
        current.eventLoop().execute(operation);
    }

    void noteUdpPacket() {
        udpPackets.incrementAndGet();
        lastUdpPacketNanos.set(System.nanoTime());
    }

    void notePlausibleAudioPacket() {
        plausibleAudioPackets.incrementAndGet();
    }

    void noteOutboundSsrcPacket() {
        outboundSsrcPackets.incrementAndGet();
    }

    long noteUnknownSsrcPacket() {
        return unknownSsrcPackets.incrementAndGet();
    }

    long noteTransportDecryptFailure() {
        return transportDecryptFailures.incrementAndGet();
    }

    long noteDaveDecryptFailure() {
        return daveDecryptFailures.incrementAndGet();
    }

    void noteDispatchedFrame() {
        dispatchedFrames.incrementAndGet();
        lastDispatchedFrameNanos.set(System.nanoTime());
    }

    public void appendReceiveDiagnostics(Map<String, Long> values) {
        long now = System.nanoTime();
        values.put("udpPackets", udpPackets.get());
        values.put("plausibleAudioPackets", plausibleAudioPackets.get());
        values.put("outboundSsrcPackets", outboundSsrcPackets.get());
        values.put("unknownSsrcPackets", unknownSsrcPackets.get());
        values.put("transportDecryptFailures", transportDecryptFailures.get());
        values.put("daveDecryptFailures", daveDecryptFailures.get());
        values.put("dispatchedFrames", dispatchedFrames.get());
        values.put("outboundFrameAttempts", outboundFrameAttempts.get());
        values.put("outboundDaveFailures", outboundDaveFailures.get());
        values.put("outboundTransportFailures", outboundTransportFailures.get());
        values.put("outboundPacketBuildFailures", outboundPacketBuildFailures.get());
        values.put("outboundPackets", outboundPackets.get());
        values.put("lastUdpPacketAgeMs", ageMillis(now, lastUdpPacketNanos.get()));
        values.put("lastDispatchedFrameAgeMs", ageMillis(now, lastDispatchedFrameNanos.get()));
    }

    private static long ageMillis(long now, long then) {
        return then == 0 ? -1 : Math.max(0, (now - then) / 1_000_000);
    }

    private void logOutboundDrop(long count, String stage, int code) {
        if (count == 1 || count == 10 || count == 100 || count % 1000 == 0) {
            logger.warn("Outbound audio dropped guild={} stage={} code={} count={}",
                    connection.getGuildId(), stage, code, count);
        }
    }

    private static class Initializer extends ChannelInitializer<DatagramChannel> {
        private final DiscordUDPConnection connection;
        private final CompletableFuture<InetSocketAddress> future;

        private Initializer(DiscordUDPConnection connection, CompletableFuture<InetSocketAddress> future) {
            this.connection = connection;
            this.future = future;
        }

        @Override
        protected void initChannel(DatagramChannel datagramChannel) {
            connection.channel = datagramChannel;

            var handler = new HolepunchHandler(future, connection.ssrc);
            var pipeline = datagramChannel.pipeline();
            pipeline.addFirst("handler", handler);
            pipeline.addLast("audio-receive", new InboundAudioHandler(connection, connection.connection));
            pipeline.addLast("rtcp", new RTCPHandler());
        }
    }
}
