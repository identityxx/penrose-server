/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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
package org.safehaus.penrose.util;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.Cipher;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.security.MessageDigest;
import java.security.Security;
import java.security.Provider;
import java.math.BigInteger;

import grid.security.MD5Crypt;


/**
 * @author Endi S. Dewata
 */
public class PasswordUtil {

    public static Logger log = LoggerFactory.getLogger(PasswordUtil.class);

    protected final static boolean DEBUG = true;

    public static Provider SECURITY_PROVIDER = new BouncyCastleProvider();
    public static String SALT_CHARACTERS =
            "abcdefghijklmnopqrstuvwxyz"+
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"+
            "0123456789./";

    static {
        try {
            Provider[] providers = Security.getProviders();
            //log.debug("Providers:");
            for (int i=0; i<providers.length; i++) {
                //log.debug(" - "+providers[i].getName()+" "+providers[i].getVersion());
            }

            Provider provider = Security.getProvider("BC");
            //log.debug("BouncyCastle: "+provider);
            if (provider == null) {
                provider = SECURITY_PROVIDER;
                //log.debug("Registering "+provider);
                Security.addProvider(provider);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static byte[] encrypt(String method, byte[] bytes) throws Exception {
        return encrypt(method, null, bytes);
    }

    public static byte[] encrypt(String method, byte[] salt, byte[] bytes) throws Exception {
        if (method == null) return bytes;
        if (bytes == null) return null;

        if ("crypt".equalsIgnoreCase(method)) {
            salt = salt == null ? generateSalt() : salt;
            return MD5Crypt.crypt(new String(bytes), new String(salt)).getBytes();
        }

        MessageDigest md = MessageDigest.getInstance(method);
        md.update(bytes);

        return md.digest();
    }

    public static byte[] generateSalt() {
        byte[] salt = new byte[2];
        for (int i=0; i<salt.length; i++) {
            int r = (int)(Math.random() * salt.length);
            salt[0] = (byte)SALT_CHARACTERS.charAt(r);
        }
        return salt;
    }

    public static byte[] encryptNTPassword(Object password) throws Exception {
        if (password == null) return null;

        String s;
        if (password instanceof byte[]) {
            s = new String((byte[])password);
        } else {
            s = password.toString();
        }

        byte[] bytes = s.getBytes("UTF-16LE");
        MessageDigest md = MessageDigest.getInstance("MD4");
        md.update(bytes);
        return md.digest();
    }

    public static byte[] convert(byte[] str, int i) {
        byte[] key = new byte[8];

        key[0] = (byte)(str[i] >> 1);
        key[1] = (byte)(( ( str[i+0] & 0x01 ) << 6 ) | ( str[i+1] >> 2 ));
        key[2] = (byte)(( ( str[i+1] & 0x03 ) << 5 ) | ( str[i+2] >> 3 ));
        key[3] = (byte)(( ( str[i+2] & 0x07 ) << 4 ) | ( str[i+3] >> 4 ));
        key[4] = (byte)(( ( str[i+3] & 0x0F ) << 3 ) | ( str[i+4] >> 5 ));
        key[5] = (byte)(( ( str[i+4] & 0x1F ) << 2 ) | ( str[i+5] >> 6 ));
        key[6] = (byte)(( ( str[i+5] & 0x3F ) << 1 ) | ( str[i+6] >> 7 ));
        key[7] = (byte)(str[i+6] & 0x7F);

        for (int j=0; j<8; j++) key[j] = (byte)( key[j] << 1 );

        return key;
    }

    public static byte[] encryptLMPassword(Object password) throws Exception {
        if (password == null) return null;

        String s;
        if (password instanceof byte[]) {
            s = new String((byte[])password);
        } else {
            s = password.toString();
        }

        byte[] bytes = s.toUpperCase().getBytes("UTF-8");

        if (bytes.length != 14) {
            byte[] b = new byte[14];
            for (int i=0; i<bytes.length && i<14; i++) b[i] = bytes[i];
            bytes = b;
        }
        //log.debug("BYTES ("+bytes.length+"):"+encode(bytes));

        byte[] key1 = convert(bytes, 0);
        byte[] key2 = convert(bytes, 7);

        //log.debug("KEY1 ("+key1.length+"): "+encode(key1));
        //log.debug("KEY2 ("+key2.length+"): "+encode(key2));

        byte[] magic = new BigInteger("4B47532140232425", 16).toByteArray();

        SecretKeySpec spec1 = new SecretKeySpec(key1, "DES");
        Cipher cipher1 = Cipher.getInstance("DES");
        cipher1.init(Cipher.ENCRYPT_MODE, spec1);
        byte[] result1 = cipher1.doFinal(magic);
        //log.debug("RESULT1 ("+result1.length+"): "+encode(result1));

        SecretKeySpec spec2 = new SecretKeySpec(key2, "DES");
        Cipher cipher2 = Cipher.getInstance("DES");
        cipher2.init(Cipher.ENCRYPT_MODE, spec2);
        byte[] result2 = cipher2.doFinal(magic);
        //log.debug("RESULT2 ("+result2.length+"): "+encode(result2));

        for (int i=0; i<8; i++) result1[i+8] = result2[i];

        return result1;
    }

    /**
     * 
     * @param password
     * @param digest
     * @return true if password matches the digest
     * @throws Exception
     */
    public static boolean comparePassword(String password, Object digest) throws Exception {
        return comparePassword(password.getBytes(), digest);
    }

    public static boolean comparePassword(byte[] password, Object digest) throws Exception {

        if (digest == null) return false;

        String encryption     = getEncryptionMethod(digest);
        String encoding       = getEncodingMethod(digest);
        String storedPassword = getEncryptedPassword(digest);

        return comparePassword(password, encryption, encoding, storedPassword);
    }

    public static boolean comparePassword(byte[] credential, String encryption, String encoding, String storedPassword) throws Exception {
        log.debug("Comparing passwords:");
        if (encryption != null) log.debug("Encryption ["+encryption+"]");
        if (encoding != null)   log.debug("Encoding   ["+encoding+"]");

        String encryptedCredential;

        if ("crypt".equals(encryption) && storedPassword.startsWith("$1$")) {

            // get the salt form the stored password
            int i = storedPassword.indexOf("$", 3);
            String salt = storedPassword.substring(3, i);
            log.debug("Salt       ["+salt+"]");

            // encrypt the new password with the same salt
            byte[] bytes = encrypt(encryption, salt.getBytes(), credential);

            // the result is already in encoded form: $1$salt$hash
            encryptedCredential = new String(bytes);

        } else {
            byte[] bytes = encrypt(encryption, credential);
            encryptedCredential = BinaryUtil.encode(encoding, bytes);
        }

        log.debug("Supplied   ["+encryptedCredential+"]");
        log.debug("Stored     ["+storedPassword+"]");

        boolean result = encryptedCredential.equals(storedPassword);

        log.debug("Result: "+result);

        return result;
    }

    /**
     * 
     * @param password
     * @return unicode password
     * @throws Exception
     */
    public static byte[] toUnicodePassword(Object password) throws Exception {
        String newPassword;
        if (password instanceof byte[]) {
            newPassword = "\""+new String((byte[])password)+ "\"";
        } else {
            newPassword = "\""+password+ "\"";
        }
        byte unicodeBytes[] = newPassword.getBytes("Unicode");
        byte bytes[]  = new byte[unicodeBytes.length-2];

        System.arraycopy(unicodeBytes, 2, bytes, 0, unicodeBytes.length-2);
        return bytes;
    }

    public static String getEncryptionMethod(Object password) {

        if (!(password instanceof String)) return null;

        String s = (String)password;

        if (s == null || !s.startsWith("{")) return null; // no encryption/encoding

        int i = s.indexOf("}");
        if (i < 0) return null; // invalid format, considered as no encryption/encoding

        s = s.substring(1, i);

        i = s.indexOf("|");
        if (i < 0) return s; // no encoding

        return s.substring(0, i);
    }

    public static String getEncodingMethod(Object password) {

        if (!(password instanceof String)) return null;

        String s = (String)password;

        if (s == null || !s.startsWith("{")) return null; // no encryption/encoding

        int i = s.indexOf("}");
        if (i < 0) return null; // invalid format, considered as no encryption/encoding

        s = s.substring(1, i);

        i = s.indexOf("|");
        if (i < 0) return "Base64"; // no encoding

        return s.substring(i+1);
    }

    public static String getEncryptedPassword(Object password) {

        if (password instanceof byte[]) return new String((byte[])password);

        String s = (String)password;

        if (s == null || !s.startsWith("{")) return s; // no encryption

        int i = s.indexOf("}");
        if (i < 0) return s; // invalid format, considered as no encryption

        return s.substring(i+1);
    }
}
