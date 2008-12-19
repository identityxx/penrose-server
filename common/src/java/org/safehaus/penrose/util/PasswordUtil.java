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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.vps.crypt.Crypt;
import org.safehaus.penrose.password.Password;

import java.security.MessageDigest;
import java.math.BigInteger;

import arlut.csd.crypto.MD5Crypt;
import arlut.csd.crypto.Sha256Crypt;
import arlut.csd.crypto.Sha512Crypt;


/**
 * @author Endi S. Dewata
 */
public class PasswordUtil {

    public static Logger log = LoggerFactory.getLogger(PasswordUtil.class);
    public static boolean debug = log.isDebugEnabled();

    protected final static boolean DEBUG = true;

    public static String SALT_CHARACTERS =
            "abcdefghijklmnopqrstuvwxyz"+
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"+
            "0123456789./";

    public static byte[] encrypt(String method, String password) throws Exception {
        return encrypt(method, null, password.getBytes());
    }

    public static byte[] encrypt(String method, byte[] bytes) throws Exception {
        return encrypt(method, null, bytes);
    }

    public static byte[] encrypt(String method, byte[] salt, byte[] bytes) throws Exception {
        if (method == null) return bytes;
        if (bytes == null) return null;

        if (debug) {
            log.debug("Encrypting "+bytes.length+" byte(s) with "+method+":");
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                String s = Integer.toHexString(0xff & b);
                if (s.length() == 1) s = "0"+s;
                sb.append(s);
            }
            log.debug(" - before: ["+sb+"]");
        }

        byte[] result;

        if ("crypt".equalsIgnoreCase(method)) {
            String password = new String(bytes);

            if (salt == null) {
                if (debug) log.debug("MD5Crypt.crypt(\""+password+"\")");
                result = MD5Crypt.crypt(password).getBytes();
                
            } else {
                String s = new String(salt);
                if (debug) log.debug("MD5Crypt.crypt(\""+password+"\", \""+s+"\")");
                result = MD5Crypt.crypt(password, new String(salt)).getBytes();
            }

        } else {
            MessageDigest md = MessageDigest.getInstance(method);
            md.update(bytes);

            result = md.digest();
        }

        if (debug) {
            StringBuilder sb = new StringBuilder();
            for (byte b : result) {
                String s = Integer.toHexString(0xff & b);
                if (s.length() == 1) s = "0"+s;
                sb.append(s);
            }
            log.debug(" - after : ["+sb+"]");
        }

        return result;
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
        key[1] = (byte)(( ( str[i  ] & 0x01 ) << 6 ) | ( str[i+1] >> 2 ));
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

        System.arraycopy(result2, 0, result1, 8, 8);

        return result1;
    }

    public static boolean validate(String password, Object digest) throws Exception {

        if (digest == null) return false;

        String encryption = getEncryptionMethod(digest);
        String hash   = getEncryptedPassword(digest);

        return Password.validate(encryption, password, hash);
        //return validate(password, encryption, encoding, hash);
    }

    public static boolean validate(String encryption, byte[] credential, String encoding, String hash) throws Exception {

        String newHash;

        if ("crypt".equalsIgnoreCase(encryption)) {

            if (hash.startsWith("$1$")) {

                newHash = MD5Crypt.crypt(new String(credential), hash);

            } else if (hash.startsWith("$5$")) {

                newHash = Sha256Crypt.Sha256_crypt(new String(credential), hash, 0);

            } else if (hash.startsWith("$6$")) {

                newHash = Sha512Crypt.Sha512_crypt(new String(credential), hash, 0);

            } else {
                String salt = hash.substring(0, 2);
                log.debug(" - salt     : ["+salt+"]");

                newHash = Crypt.crypt(salt, new String(credential));
            }

        } else {
            byte[] bytes = encrypt(encryption, credential);
            newHash = BinaryUtil.encode(encoding, bytes);
        }

        if (debug) {
            log.debug("Comparing passwords:");
            if (encryption != null) log.debug(" - encryption: ["+encryption+"]");
            if (encoding != null)   log.debug(" - encoding  : ["+encoding+"]");

            log.debug(" - supplied  : ["+newHash+"]");
            log.debug(" - stored    : ["+hash+"]");
        }

        boolean result = newHash.equals(hash);

        if (debug) log.debug(" - result    : "+result);

        return result;
    }

    public static String getEncryptionMethod(Object password) {

        if (!(password instanceof String)) return null;

        String s = (String)password;

        if (!s.startsWith("{")) return null; // no encryption/encoding

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

        if (!s.startsWith("{")) return null; // no encryption/encoding

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
