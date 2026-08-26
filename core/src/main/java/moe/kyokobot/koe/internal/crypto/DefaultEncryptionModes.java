package moe.kyokobot.koe.internal.crypto;

import java.security.Security;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

class DefaultEncryptionModes {

    private static final String AES_GCM_NO_PADDING = "AES_256/GCM/NOPADDING";

    private DefaultEncryptionModes() {
        //
    }

    static final Map<String, Supplier<EncryptionMode>> encryptionModes;

    static {
        // sorted by priority
        var modes = new HashMap<String, Supplier<EncryptionMode>>();

        // the jvm may not support this algorithm, so we need to check first if it is available
        if (Security.getAlgorithms("Cipher").contains(AES_GCM_NO_PADDING)) {
            modes.put("aead_aes256_gcm_rtpsize", AEADAES256GCMRTPSizeEncryptionMode::new); // recommended by Discord when available)
        }

        modes.put("aead_xchacha20_poly1305_rtpsize", AEADXChaCha20Poly1305RTPSizeEncryptionMode::new); // required by Discord
        // Discord discontinued the XSalsa modes on 18 November 2024. Do not
        // negotiate a transport mode for which inbound authentication cannot be
        // implemented with the modern RTP-size contract.
        modes.put("plain", PlainEncryptionMode::new); // not supported by Discord anymore, implemented for testing.

        encryptionModes = Collections.unmodifiableMap(modes);
    }
}
