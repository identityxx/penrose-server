package org.safehaus.penrose.util;

import sun.misc.BASE64Encoder;
import sun.misc.BASE64Decoder;

import java.math.BigInteger;

import org.apache.log4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class BinaryUtil {

    public static Logger log = Logger.getLogger(BinaryUtil.class);

    public static String BASE64      = "Base64";
    public static String BIG_INTEGER = "BigInteger";

    public static String encode(byte[] bytes) throws Exception {
        return encode(null, bytes);
    }

    public static String encode(String encoding, byte[] bytes) throws Exception {
        if (bytes == null) return null;

        String string;

        if (BASE64.equals(encoding)) {
            BASE64Encoder enc = new BASE64Encoder();
            string = enc.encode(bytes);

        } else if (BIG_INTEGER.equals(encoding)) {
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

        if (BASE64.equals(encoding)) {
            BASE64Decoder decoder = new BASE64Decoder();
            bytes = decoder.decodeBuffer(string);

        } else if (BIG_INTEGER.equals(encoding)) {
            BigInteger bigInteger = new BigInteger(string, 16);
            int length = (string.length()+1) / 2;
            bytes = bigInteger.toByteArray();
            if (bytes.length > length) {
                byte[] b = new byte[length];
                for (int i=0; i<length; i++) {
                    b[i] = bytes[bytes.length-length+i];
                }
                bytes = b;
            }

        } else {
            bytes = string.getBytes();
        }

        return bytes;
    }
}
