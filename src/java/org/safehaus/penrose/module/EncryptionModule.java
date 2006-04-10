/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
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
public class EncryptionModule extends Module {

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
