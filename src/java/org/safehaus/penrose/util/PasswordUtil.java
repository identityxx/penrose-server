/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.util;

import sun.misc.BASE64Encoder;

import javax.crypto.spec.SecretKeySpec;
import javax.crypto.Cipher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.Penrose;

import java.security.MessageDigest;
import java.math.BigInteger;


/**
 * @author Endi S. Dewata
 */
public class PasswordUtil {

    public static Logger log = LoggerFactory.getLogger(PasswordUtil.class);

	protected final static boolean DEBUG = true;

    public static String encrypt(String method, String encoding, String password) throws Exception {
        if (password == null) return null;
        
        if (method == null || method.trim().equals("")) {
            return password;
        }
        
        byte[] bytes = encrypt(method, password);

        if ("Base64".equals(encoding)) {
            BASE64Encoder enc = new BASE64Encoder();
            return enc.encode(bytes);

        } else {
            return toString(bytes);
        }

    }

    public static byte[] encrypt(String method, String password) throws Exception {
        if (password == null) return null;

        byte[] bytes = password.getBytes();

        MessageDigest md = MessageDigest.getInstance(method);
        md.update(bytes);

        return md.digest();
    }

    public static String toString(byte[] b) {
        if (b == null) return null;
        String s = new BigInteger(1, b).toString(16);
        while (s.length() < b.length*2) s = "0"+s;
        return s;
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
        log.debug("BYTES ("+bytes.length+"):"+toString(bytes));

        byte[] key1 = convert(bytes, 0);
        byte[] key2 = convert(bytes, 7);

        log.debug("KEY1 ("+key1.length+"): "+toString(key1));
        log.debug("KEY2 ("+key2.length+"): "+toString(key2));

        byte[] magic = new BigInteger("4B47532140232425", 16).toByteArray();

        SecretKeySpec spec1 = new SecretKeySpec(key1, "DES");
        Cipher cipher1 = Cipher.getInstance("DES");
        cipher1.init(Cipher.ENCRYPT_MODE, spec1);
        byte[] result1 = cipher1.doFinal(magic);
        log.debug("RESULT1 ("+result1.length+"): "+toString(result1));

        SecretKeySpec spec2 = new SecretKeySpec(key2, "DES");
        Cipher cipher2 = Cipher.getInstance("DES");
        cipher2.init(Cipher.ENCRYPT_MODE, spec2);
        byte[] result2 = cipher2.doFinal(magic);
        log.debug("RESULT2 ("+result2.length+"): "+toString(result2));

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

        String encryption = getEncryptionMethod(digest);

        int i = digest.indexOf("}");
        String storedPassword = digest.substring(i+1);

        String encoding = "Base64";

        return comparePassword(credential, encryption, encoding, storedPassword);
    }

    public static boolean comparePassword(String credential, String encryption, String encoding, String storedPassword) throws Exception {
        encryption = getEncryptionMethod(storedPassword);
        encoding = getEncodingMethod(storedPassword);
        storedPassword = getEncryptedPassword(storedPassword);

        log.debug("COMPARING PASSWORDS:");
        log.debug("  encryption      : ["+encryption+"]");
        log.debug("  encoding        : ["+encoding+"]");
        log.debug("  digest          : ["+storedPassword+"]");

        String encryptedCredential = encrypt(encryption, encoding, credential);

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
        if (i < 0) return null; // no encoding

        return s.substring(i+1);
    }

    public static String getEncryptedPassword(String password) {

        if (password == null || !password.startsWith("{")) return password; // no encryption

        int i = password.indexOf("}");
        if (i < 0) return password; // invalid format, considered as no encryption

        return password.substring(i+1);
    }
}
