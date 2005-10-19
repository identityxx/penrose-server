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
package org.safehaus.penrose.util;

import sun.misc.BASE64Encoder;
import sun.misc.BASE64Decoder;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.Cipher;

import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.math.BigInteger;


/**
 * @author Endi S. Dewata
 */
public class PasswordUtil {

    public static Logger log = Logger.getLogger(PasswordUtil.class);

	protected final static boolean DEBUG = true;

    public static String encrypt(String method, String encoding, String password) throws Exception {
        byte[] bytes = encrypt(method, password);
        return encode(encoding, bytes);
    }

    public static byte[] encrypt(String method, String password) throws Exception {
        if (password == null) return null;

        byte[] bytes = password.getBytes();
        if (method == null) return password.getBytes();

        MessageDigest md = MessageDigest.getInstance(method);
        md.update(bytes);

        return md.digest();
    }

    public static String encode(byte[] bytes) throws Exception {
        return encode(null, bytes);
    }

    public static String encode(String encoding, byte[] bytes) throws Exception {
        if (bytes == null) return null;

        String string;

        if ("Base64".equals(encoding)) {
            BASE64Encoder enc = new BASE64Encoder();
            string = enc.encode(bytes);

        } else if ("BigInteger".equals(encoding)) {
            string = new BigInteger(1, bytes).toString(16);
            while (string.length() < bytes.length*2) string = "0"+string;

        } else {
            string = new String(bytes);
        }

        return string;
    }

    public static byte[] decode(String string) throws Exception {
        return decode(null, string);
    }

    public static byte[] decode(String encoding, String string) throws Exception {
        if (string == null) return null;

        byte[] bytes;

        if ("Base64".equals(encoding)) {
            BASE64Decoder decoder = new BASE64Decoder();
            bytes = decoder.decodeBuffer(string);

        } else if ("BigInteger".equals(encoding)) {
            BigInteger bigInteger = new BigInteger(string, 16);
            bytes = bigInteger.toByteArray();

        } else {
            bytes = string.getBytes();
        }

        return bytes;
    }

    public static byte[] encryptNTPassword(String password) throws Exception {
        if (password == null) return null;

        byte[] bytes = password.getBytes("UTF-16LE");
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

    public static byte[] encryptLMPassword(String password) throws Exception {
        if (password == null) return null;

        byte[] bytes = password.toUpperCase().getBytes("UTF-8");
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
     * @param credential
     * @param digest
     * @return true if password matches the digest
     * @throws Exception
     */
    public static boolean comparePassword(String credential, String digest) throws Exception {

        if (digest == null) return false;

        String encryption     = getEncryptionMethod(digest);
        String encoding       = getEncodingMethod(digest);
        String storedPassword = getEncryptedPassword(digest);

        return comparePassword(credential, encryption, encoding, storedPassword);
    }

    public static boolean comparePassword(String credential, String encryption, String encoding, String storedPassword) throws Exception {
        log.debug("COMPARING PASSWORDS:");
        log.debug("  encryption      : ["+encryption+"]");
        log.debug("  encoding        : ["+encoding+"]");
        log.debug("  digest          : ["+storedPassword+"]");

        byte[] bytes = encrypt(encryption, credential);
        String encryptedCredential = encode(encoding, bytes);

        //log.debug("  credential      : ["+credential+"]");
        log.debug("  enc credential  : ["+encryptedCredential+"]");

        boolean result = encryptedCredential.equals(storedPassword);

        log.debug("  result          : ["+result+"]");

        return result;
    }

    /**
     * 
     * @param password
     * @return unicode password
     * @throws Exception
     */
    public static byte[] toUnicodePassword(String password) throws Exception {
        String newPassword = "\""+password+ "\"";
        byte unicodeBytes[] = newPassword.getBytes("Unicode");
        byte bytes[]  = new byte[unicodeBytes.length-2];

        System.arraycopy(unicodeBytes, 2, bytes, 0, unicodeBytes.length-2);
        return bytes;
    }

    public static String getEncryptionMethod(String password) {

        if (password == null || !password.startsWith("{")) return null; // no encryption/encoding

        int i = password.indexOf("}");
        if (i < 0) return null; // invalid format, considered as no encryption/encoding

        String s = password.substring(1, i);

        i = s.indexOf("|");
        if (i < 0) return s; // no encoding

        return s.substring(0, i);
    }

    public static String getEncodingMethod(String password) {

        if (password == null || !password.startsWith("{")) return null; // no encryption/encoding

        int i = password.indexOf("}");
        if (i < 0) return null; // invalid format, considered as no encryption/encoding

        String s = password.substring(1, i);

        i = s.indexOf("|");
        if (i < 0) return "Base64"; // no encoding

        return s.substring(i+1);
    }

    public static String getEncryptedPassword(String password) {

        if (password == null || !password.startsWith("{")) return password; // no encryption

        int i = password.indexOf("}");
        if (i < 0) return password; // invalid format, considered as no encryption

        return password.substring(i+1);
    }
}
