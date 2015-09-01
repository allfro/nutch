package org.apache.nutch.util;


import org.apache.commons.httpclient.HttpsURL;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * This class provide various static methods that relax X509 certificate and
 * hostname verification while using the SSL over the HTTP protocol.
 *
 * @author    Francis Labrie
 */
public final class SSLUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(SSLUtilities.class);
    private static final String[] protocols = new String[] {
            "SSL",
            "SSLv2",
            "SSLv3",
            "TLS",
            "TLSv1",
            "TLSv1.1",
            "TLSv1.2"
    };

    public static void trustAll() {
        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager(){
            public X509Certificate[] getAcceptedIssuers(){return null;}
            public void checkClientTrusted(X509Certificate[] certs, String authType){}
            public void checkServerTrusted(X509Certificate[] certs, String authType){}
        }};

        // Install the all-trusting trust managers
//        for (String protocol: protocols) {
        try {
            SSLContext sc = SSLContext.getInstance(protocols[2]);
            sc.init(null, trustAllCerts, new SecureRandom());
            SSLContext.setDefault(sc);
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (Exception ignored) {
            LOG.error("Error while installing \"trust all\" trust manager for protocol {}", protocols[2]);
        }
//        }

        HttpsURLConnection.setDefaultHostnameVerifier(new AllowAllHostnameVerifier());
    }

} // SSLUtilities