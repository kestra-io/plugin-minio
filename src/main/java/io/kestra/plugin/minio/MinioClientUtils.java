package io.kestra.plugin.minio;

import okhttp3.OkHttpClient;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;


public class MinioClientUtils {
    public static OkHttpClient withPemCertificate(InputStream clientPemIs, InputStream caPem) throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException, UnrecoverableKeyException {
        PrivateKey privateKey = null;
        Certificate clientCertificate = null;

        try (PEMParser pemParser = new PEMParser(new InputStreamReader(clientPemIs))) {
            JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter();
            JcaX509CertificateConverter certConverter = new JcaX509CertificateConverter();
            Object object;
            while ((object = pemParser.readObject()) != null) {
                if (object instanceof PrivateKeyInfo privateKeyInfo) {
                    privateKey = keyConverter.getPrivateKey(privateKeyInfo);
                } else if (object instanceof X509CertificateHolder certHolder) {
                    clientCertificate = certConverter.getCertificate(certHolder);
                }
            }
        }

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);

        Certificate[] privateKeyChain = new Certificate[]{clientCertificate};

        if (caPem != null) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            keyStore.setCertificateEntry("ca", cf.generateCertificate(caPem));
        }

        keyStore.setKeyEntry("client-key", privateKey, "".toCharArray(), privateKeyChain);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "".toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());

        X509TrustManager trustManager = getTrustManager(tmf);

        return new OkHttpClient.Builder()
            .sslSocketFactory(sslContext.getSocketFactory(), trustManager)
            .build();
    }

    private static X509TrustManager getTrustManager(TrustManagerFactory trustManagerFactory) {
        for (var trustManager : trustManagerFactory.getTrustManagers()) {
            if (trustManager instanceof X509TrustManager) {
                return (X509TrustManager) trustManager;
            }
        }
        throw new IllegalStateException("No X509TrustManager found");
    }
}
