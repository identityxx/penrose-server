/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.module;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import java.security.Provider;
import java.security.Security;
import java.security.MessageDigest;
import java.util.StringTokenizer;

/**
 * @author Endi S. Dewata
 */
public class EncryptionModule extends GenericModule {

    public boolean verbose;

    public void init() throws Exception {

        Security.addProvider(new BouncyCastleProvider());

        verbose = new Boolean(getParameter("verbose")).booleanValue();

        if (verbose) {
            Provider[] providers = Security.getProviders();

            for (int i = 0; i < providers.length; i++) {
                Provider provider = providers[i];
                System.out.println("[EncryptionModule] "+provider.getName()+" "+provider.getVersion()+" security provider available.");
            }
        }

        String ciphers = getParameter("ciphers");
        if (ciphers != null) {
            StringTokenizer st = new StringTokenizer(ciphers, ",");
            while (st.hasMoreTokens()) {
                String name = st.nextToken().trim();
                checkCipher(name);
            }
        }

        String messageDigests = getParameter("messageDigests");
        if (messageDigests != null) {
            StringTokenizer st = new StringTokenizer(messageDigests, ",");
            while (st.hasMoreTokens()) {
                String name = st.nextToken().trim();
                checkMessageDigest(name);
            }
        }
    }

    public void checkCipher(String cipherName) throws Exception {
        Cipher.getInstance(cipherName);
        if (verbose) System.out.println("[EncryptionModule] "+cipherName+" cipher available.");
    }

    public void checkMessageDigest(String messageDigestName) throws Exception {
        MessageDigest.getInstance(messageDigestName);
        if (verbose) System.out.println("[EncryptionModule] "+messageDigestName+" message digest available.");
    }
}
