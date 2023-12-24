package org.apache.spark.network.crypto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.crypto.tink.subtle.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.spark.network.crypto.AuthMessage;
import org.apache.spark.network.crypto.TransportCipher;
import org.apache.spark.network.util.TransportConf;

import javax.crypto.spec.SecretKeySpec;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Properties;

public class AuthEngine implements Closeable {
    public static final byte[] INPUT_IV_INFO="inputIv".getBytes(StandardCharsets.UTF_8);
    public static final byte[] OUTPUT_IV_INFO="outputIv".getBytes(StandardCharsets.UTF_8);
    private static final String MAC_ALGORITHM="HMACSHA256";
    private static final int AES_GCM_KEY_SIZE_BYTES=16;
    private static final byte[] EMPTY_TRANSCRIPT=new byte[0];
    private final String appId;
    private final byte[] preSharedSecret;
    private final TransportConf conf;
    private final Properties cryptoConf;
    private byte[] clientPrivateKey;
    private TransportCipher sessionCipher;
    public AuthEngine(String appId,String preSharedSecret,TransportConf conf){
        Preconditions.checkNotNull(appId);
        Preconditions.checkNotNull(preSharedSecret);
        this.appId=appId;
        this.preSharedSecret=preSharedSecret.getBytes(StandardCharsets.UTF_8);
        this.conf=conf;
        this.cryptoConf=conf.cryptoConf();
    }
    @VisibleForTesting
    void setClientPrivateKey(byte[] privateKey){
        this.clientPrivateKey=privateKey;
    }
    private AuthMessage encryptEphemeralPublicKey(byte[] ephemeralX25519PublicKey,
                                                  byte[] transcript)throws GeneralSecurityException{
        byte[] nonSecretSalt = Random.randBytes(AES_GCM_KEY_SIZE_BYTES);
        byte[] addState= Bytes.concat(appId.getBytes(StandardCharsets.UTF_8),nonSecretSalt,transcript);
        byte[] derivedKeyEncryptingKey= Hkdf.computeHkdf(
                MAC_ALGORITHM,
                preSharedSecret,
                nonSecretSalt,
                addState,
                AES_GCM_KEY_SIZE_BYTES
        );
        byte[] aesGcmCiphertext=new AesGcmJce(derivedKeyEncryptingKey).encrypt(ephemeralX25519PublicKey,addState);
        return new AuthMessage(appId,nonSecretSalt,aesGcmCiphertext);
    }

    private byte[] decryptEphemeralPublicKey(AuthMessage encryptedPubliKey,byte[] transcript)throws GeneralSecurityException{
        Preconditions.checkArgument(appId.equals(encryptedPubliKey.appId));
        byte[] aadState=Bytes.concat(appId.getBytes(StandardCharsets.UTF_8),encryptedPubliKey.salt,transcript);
        byte[] deriveKeyEncryptingKey=Hkdf.computeHkdf(
                MAC_ALGORITHM,
                preSharedSecret,
                encryptedPubliKey.salt,
                aadState,
                AES_GCM_KEY_SIZE_BYTES
        );
        return new AesGcmJce(deriveKeyEncryptingKey)
                .decrypt(encryptedPubliKey.ciphertext,aadState);
    }
    AuthMessage challenge()throws GeneralSecurityException{
        setClientPrivateKey(X25519.generatePrivateKey());
        return encryptEphemeralPublicKey(X25519.publicFromPrivate(clientPrivateKey),EMPTY_TRANSCRIPT);
    }
    AuthMessage response(AuthMessage encryptedClientPublicKey)throws GeneralSecurityException{
        Preconditions.checkArgument(appId.equals(encryptedClientPublicKey.appId));
        byte[] clientPublicKey=
                decryptEphemeralPublicKey(encryptedClientPublicKey,EMPTY_TRANSCRIPT);
        byte[] serverEphemeralPrivateKey=X25519.generatePrivateKey();
        AuthMessage ephemeralServerPublicKey=encryptEphemeralPublicKey(
                X25519.publicFromPrivate(serverEphemeralPrivateKey),
                getTranscript(encryptedClientPublicKey)
        );
        byte[] sharedSecret=X25519.computeSharedSecret(serverEphemeralPrivateKey,clientPublicKey);
        byte[] challengeResponseTranscript=
                getTranscript(encryptedClientPublicKey,ephemeralServerPublicKey);
        this.sessionCipher=
                generateTransportCipher(sharedSecret,false,challengeResponseTranscript);
        return ephemeralServerPublicKey;
    }
    void deriveSessionCipher(AuthMessage encryptedClientPublicKey,
                             AuthMessage encryptedServerPublicKey)throws GeneralSecurityException{
        Preconditions.checkArgument(appId.equals(encryptedClientPublicKey.appId));
        Preconditions.checkArgument(appId.equals(encryptedServerPublicKey.appId));
        byte[] serverPublicKey=decryptEphemeralPublicKey(encryptedServerPublicKey,getTranscript(encryptedClientPublicKey));
        byte[] sharedSecret=X25519.computeSharedSecret(clientPrivateKey,serverPublicKey);
        byte[] challengeResponseTranscript=getTranscript(encryptedClientPublicKey,encryptedServerPublicKey);
        this.sessionCipher=generateTransportCipher(sharedSecret,true,challengeResponseTranscript);
    }
    private TransportCipher generateTransportCipher(
            byte[] sharedSecret,
            boolean isClient,
            byte[] transcript
    )throws GeneralSecurityException{
        byte[] clientIv=Hkdf.computeHkdf(
                MAC_ALGORITHM,
                sharedSecret,
                transcript,
                INPUT_IV_INFO,
                AES_GCM_KEY_SIZE_BYTES
        );
        byte[] serverIv=Hkdf.computeHkdf(
                MAC_ALGORITHM,
                sharedSecret,
                transcript,
                OUTPUT_IV_INFO,
                AES_GCM_KEY_SIZE_BYTES
        );
        SecretKeySpec sessionKey=new SecretKeySpec(sharedSecret,"AES");
        return new TransportCipher(cryptoConf,conf.cipherTransformation(),sessionKey,
                isClient?clientIv:serverIv,
                isClient?clientIv:serverIv);
    }
    private byte[] getTranscript(AuthMessage... encryptedPublicKeys){
        ByteBuf transcript = Unpooled.buffer(Arrays.stream(encryptedPublicKeys).mapToInt(k -> k.encodedLength()).sum());
        Arrays.stream(encryptedPublicKeys).forEachOrdered(k->k.encode(transcript));
        return transcript.array();
    }
    TransportCipher sessionCipher(){
        Preconditions.checkState(sessionCipher!=null);
        return sessionCipher;
    }

    @Override
    public void close() throws IOException {

    }
}
