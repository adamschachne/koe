package moe.kyokobot.koe.internal;

import moe.kyokobot.koe.*;
import moe.kyokobot.koe.codec.CodecInstance;
import moe.kyokobot.koe.codec.CodecType;
import moe.kyokobot.koe.codec.OpusCodecInfo;
import moe.kyokobot.koe.experimental.MediaConnectionExperimental;
import moe.kyokobot.koe.experimental.media.VideoFrameProvider;
import moe.kyokobot.koe.gateway.MediaGatewayConnection;
import moe.kyokobot.koe.gateway.MediaValve;
import moe.kyokobot.koe.handler.ConnectionHandler;
import moe.kyokobot.koe.media.AudioFrameProvider;
import moe.kyokobot.koe.poller.AbstractFramePoller;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import moe.kyokobot.koe.internal.handler.DiscordUDPConnection;

public class MediaConnectionImpl implements MediaConnection, MediaConnectionExperimental {
    private static final Logger logger = LoggerFactory.getLogger(MediaConnectionImpl.class);

    private final KoeClientImpl client;
    private final long guildId;
    private final EventDispatcher dispatcher;

    private volatile MediaGatewayConnection gatewayConnection;
    private volatile ConnectionHandler<?> connectionHandler;
    private volatile VoiceServerInfo info;
    private CodecInstance audioCodec;
    private CodecInstance videoCodec;
    private AbstractFramePoller audioPoller;
    private AbstractFramePoller videoPoller;
    private AudioFrameProvider audioSender;
    private VideoFrameProvider videoSender;
    private volatile DAVEManager daveManager;
    private final Map<Integer, String> audioSsrcUsers = new ConcurrentHashMap<>();
    private final Map<String, Integer> userAudioSsrcs = new ConcurrentHashMap<>();
    private final AtomicLong transportGeneration = new AtomicLong();
    private final AtomicBoolean receiveEnabled = new AtomicBoolean();
    private final AtomicInteger lastSpeakingMask = new AtomicInteger();

    public MediaConnectionImpl(@NotNull KoeClientImpl client, long guildId) {
        this.client = Objects.requireNonNull(client);
        this.guildId = guildId;
        this.dispatcher = new EventDispatcher();
        this.audioCodec = OpusCodecInfo.INSTANCE.instantiate();
        this.audioPoller = client.getOptions().getFramePollerFactory().createFramePoller(this.audioCodec, this);
        this.videoCodec = null;
        this.videoPoller = null;
    }

    @Override
    public CompletionStage<Void> connect(VoiceServerInfo info) {
        this.disconnect();
        this.createDAVEManager();
        audioSsrcUsers.clear();
        userAudioSsrcs.clear();
        var generation = transportGeneration.incrementAndGet();

        var gatewayFactory = client.getGatewayVersion().getFactory();
        var conn = gatewayFactory.create(this, info);

        return conn.start().thenAccept(nothing -> {
            MediaConnectionImpl.this.info = info;
            MediaConnectionImpl.this.gatewayConnection = conn;
            MediaValve valve = conn.getValve();
            if (valve != null && getOptions().isDeafened()) {
                valve.setDeafen(true);
                valve.sendToGateway();
            }
            dispatcher.transportGenerationChanged(guildId, info.getChannelId(), generation);
        });
    }

    @Override
    public void disconnect() {
        logger.debug("Disconnecting...");
        stopAudioFramePolling();
        stopVideoFramePolling();

        if (gatewayConnection != null && gatewayConnection.isOpen()) {
            gatewayConnection.close(1000, null);
            gatewayConnection = null;
        }

        if (connectionHandler != null) {
            connectionHandler.close();
            connectionHandler = null;
        }

        info = null;
        receiveEnabled.set(false);
        this.destroyDAVEManager();
    }

    @Override
    public void reconnect() {
        logger.debug("Reconnecting...");

        if (gatewayConnection != null) {
            gatewayConnection.reconnect();
        }
    }

    @Override
    @NotNull
    public KoeClient getClient() {
        return client;
    }

    @Override
    @NotNull
    public KoeOptions getOptions() {
        return client.getOptions();
    }

    @Override
    @Nullable
    public AudioFrameProvider getAudioSender() {
        return audioSender;
    }

    @Override
    @Nullable
    public VideoFrameProvider getVideoSender() {
        return videoSender;
    }

    @Override
    public long getGuildId() {
        return guildId;
    }

    @Override
    @Nullable
    public MediaGatewayConnection getGatewayConnection() {
        return gatewayConnection;
    }

    @Override
    @Nullable
    public VoiceServerInfo getVoiceServerInfo() {
        return info;
    }

    @Override
    public ConnectionHandler<?> getConnectionHandler() {
        return connectionHandler;
    }

    @Override
    public void setAudioSender(@Nullable AudioFrameProvider sender) {
        if (this.audioSender != null) {
            this.audioSender.dispose();
        }
        this.audioSender = sender;
        if (sender != null && this.audioCodec != null) {
            sender.onCodecChanged(this.audioCodec);
        }
    }

    @Override
    public void setAudioCodec(@NotNull CodecInstance audioCodec) {
        if (Objects.requireNonNull(audioCodec).getType() != CodecType.AUDIO) {
            throw new IllegalArgumentException("Specified codec must be an audio codec!");
        }

        boolean wasPolling = this.audioPoller != null && this.audioPoller.isPolling();
        this.stopAudioFramePolling();

        this.audioCodec = audioCodec;
        this.audioPoller = client.getOptions().getFramePollerFactory().createFramePoller(audioCodec, this);
        if (this.audioSender != null) {
            this.audioSender.onCodecChanged(audioCodec);
        }

        if (wasPolling) {
            this.startAudioFramePolling();
        }
    }

    @Override
    public void startAudioFramePolling() {
        if (this.audioPoller == null || this.audioPoller.isPolling()) {
            return;
        }

        this.audioPoller.start();
    }

    @Override
    public void stopAudioFramePolling() {
        if (this.audioPoller == null || !this.audioPoller.isPolling()) {
            return;
        }

        this.audioPoller.stop();
    }

    @Override
    public void setVideoSender(@Nullable VideoFrameProvider sender) {
        if (this.videoSender != null) {
            this.videoSender.dispose();
        }
        this.videoSender = sender;
        if (sender != null && this.videoCodec != null) {
            sender.onCodecChanged(this.videoCodec);
        }
    }

    @Override
    public void setVideoCodec(@Nullable CodecInstance videoCodec) {
        if (videoCodec == null) {
            this.stopVideoFramePolling();
            this.videoCodec = null;
            this.videoPoller = null;
            return;
        }

        if (videoCodec.getType() != CodecType.VIDEO) {
            throw new IllegalArgumentException("Specified codec must be a video codec!");
        }

        boolean wasPolling = videoPoller != null && videoPoller.isPolling();
        this.stopVideoFramePolling();

        this.videoCodec = videoCodec;
        this.videoPoller = client.getOptions().getFramePollerFactory().createFramePoller(videoCodec, this);
        if (this.videoSender != null) {
            this.videoSender.onCodecChanged(videoCodec);
        }

        if (wasPolling) {
            this.startVideoFramePolling();
        }
    }

    @Override
    public void startVideoFramePolling() {
        if (this.videoPoller == null || this.videoPoller.isPolling()) {
            return;
        }

        this.videoPoller.start();
    }

    @Override
    public void stopVideoFramePolling() {
        if (this.videoPoller == null || !this.videoPoller.isPolling()) {
            return;
        }

        this.videoPoller.stop();
    }

    @Override
    public void registerListener(KoeEventListener listener) {
        dispatcher.register(listener);
    }

    @Override
    public void unregisterListener(KoeEventListener listener) {
        dispatcher.unregister(listener);
    }

    @Override
    public void executeOnTransport(Runnable operation) {
        Objects.requireNonNull(operation);
        var handler = connectionHandler;
        if (!(handler instanceof DiscordUDPConnection)) {
            throw new IllegalStateException("No active Discord UDP transport");
        }
        ((DiscordUDPConnection) handler).executeOnEventLoop(operation);
    }

    @Override
    public long getTransportGeneration() {
        return transportGeneration.get();
    }

    @Override
    public void setReceiveEnabled(boolean enabled) {
        var gateway = gatewayConnection;
        if (gateway == null) {
            if (enabled) {
                throw new IllegalStateException("Voice gateway is not active");
            }
            return;
        }
        var valve = gateway.getValve();
        if (valve != null) {
            valve.setDeafen(!enabled);
            valve.sendToGateway();
            receiveEnabled.set(enabled);
        } else if (enabled) {
            throw new IllegalStateException("Voice media sink is not available");
        }
    }

    @Override
    public Map<String, Long> getReceiveDiagnostics() {
        var values = new LinkedHashMap<String, Long>();
        var gateway = gatewayConnection;
        var handler = connectionHandler;
        var manager = daveManager;
        values.put("transportGeneration", transportGeneration.get());
        values.put("voiceGatewayOpen", gateway != null && gateway.isOpen() ? 1L : 0L);
        values.put("udpTransportActive", handler instanceof DiscordUDPConnection ? 1L : 0L);
        values.put("receiveEnabled", receiveEnabled.get() ? 1L : 0L);
        values.put("mediaSinkWantsAny", receiveEnabled.get() ? 100L : 0L);
        values.put("mappedAudioSsrcs", (long) audioSsrcUsers.size());
        values.put("daveProtocolVersion",
                manager == null ? 0L : (long) manager.getCurrentProtocolVersion());
        values.put("audioPolling", audioPoller != null && audioPoller.isPolling() ? 1L : 0L);
        values.put("lastSpeakingMask", (long) lastSpeakingMask.get());
        if (handler instanceof DiscordUDPConnection) {
            ((DiscordUDPConnection) handler).appendReceiveDiagnostics(values);
        }
        return Collections.unmodifiableMap(values);
    }

    @Override
    public void close() {
        if (this.audioSender != null) {
            this.audioSender.dispose();
            this.audioSender = null;
        }

        if (this.videoSender != null) {
            this.videoSender.dispose();
            this.videoSender = null;
        }

        disconnect();
        client.removeClosedConnection(this);
    }

    @Override
    public void updateSpeakingState(int mask) {
        lastSpeakingMask.set(mask);
        if (this.gatewayConnection != null) {
            this.gatewayConnection.updateSpeaking(mask);
        }
    }

    public EventDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setConnectionHandler(ConnectionHandler<?> connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    public void updateUserStreams(String userId, int audioSsrc) {
        var manager = daveManager;
        if (manager != null) {
            manager.addUser(userId);
        }
        var previous = userAudioSsrcs.remove(userId);
        if (previous != null) {
            audioSsrcUsers.remove(previous, userId);
        }
        if (audioSsrc == 0) {
            return;
        }
        userAudioSsrcs.put(userId, audioSsrc);
        audioSsrcUsers.put(audioSsrc, userId);
    }

    public void removeUserStreams(String userId) {
        var previous = userAudioSsrcs.remove(userId);
        if (previous != null) {
            audioSsrcUsers.remove(previous, userId);
        }
    }

    @Nullable
    public String getUserIdForAudioSsrc(int audioSsrc) {
        return audioSsrcUsers.get(audioSsrc);
    }

    public boolean dispatchInboundAudio(DiscordUDPConnection source, int ssrc,
                                     int sequence, long timestamp,
                                     long receivedNanos, boolean marker, boolean dave,
                                     int[] csrcs, int extensionProfile, int extensionWords,
                                     byte[] opus) {
        if (connectionHandler != source
                || source.getTransportGeneration() != transportGeneration.get()) {
            return false;
        }
        var user = audioSsrcUsers.get(ssrc);
        if (user == null) {
            return false;
        }
        long userId;
        try {
            userId = Long.parseUnsignedLong(user);
        } catch (NumberFormatException ignored) {
            logger.warn("Ignoring invalid Discord voice user ID {}", user);
            return false;
        }
        if (userId == client.getClientId() || ssrc == getOutboundSsrc()) {
            return false;
        }
        dispatcher.audioFrameReceived(new ReceivedAudioFrame(
                guildId, source.getChannelId(), userId, ssrc, sequence, timestamp,
                source.getTransportGeneration(), receivedNanos, marker, dave, csrcs,
                extensionProfile, extensionWords, opus));
        return true;
    }

    private int getOutboundSsrc() {
        var handler = connectionHandler;
        return handler instanceof DiscordUDPConnection
                ? ((DiscordUDPConnection) handler).getSsrc() : 0;
    }

    public DAVEManager getDAVEManager() {
        return daveManager;
    }

    public void createDAVEManager() {
        this.destroyDAVEManager();

        var daveFactory = client.getDaveFactory();
        if (daveFactory != null) {
            daveManager = new DAVEManager(this, daveFactory);
        }
    }

    public void destroyDAVEManager() {
        if (this.daveManager != null) {
            try {
                this.daveManager.close();
            } catch (Exception e) {
                logger.error("Error closing old DAVE manager", e);
            }
        }
    }
}
