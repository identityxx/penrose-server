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

import net.iharder.base64.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

/**
 * @author Endi S. Dewata
 */
public class BinaryUtil {

    public static Logger log = LoggerFactory.getLogger(BinaryUtil.class);
    public static boolean debug = log.isDebugEnabled();

    public static String BASE64      = "Base64";
    public static String BIG_INTEGER = "BigInteger";

    public static String encode(byte[] bytes) throws Exception {
        return encode(null, bytes);
    }

    public static String encode(String encoding, byte[] bytes) {
        return encode(encoding, bytes, 0, bytes.length);
    }

    public static String encode(String encoding, byte[] bytes, int offset, int length) {

        if (bytes == null) return null;

        if (offset + length > bytes.length) length = bytes.length - offset;

        if (debug) {
            log.debug("Encoding "+length+" byte(s) with "+encoding+":");
            StringBuilder sb = new StringBuilder();
            for (int i=0; i<length; i++) {
                byte b = bytes[offset+i];
                String s = Integer.toHexString(0xff & b);
                if (s.length() == 1) s = "0"+s;
                sb.append(s);
            }
            log.debug(" - before: ["+sb+"]");
        }

        String string;

        if (BASE64.equals(encoding)) {
            string = Base64.encodeBytes(bytes, offset, length, Base64.DONT_BREAK_LINES);

        } else if (BIG_INTEGER.equals(encoding)) {

            if (offset != 0 || length != bytes.length) {
                byte[] b = new byte[length];
                System.arraycopy(bytes, offset, b, 0, length);
                bytes = b;
            }

            string = new BigInteger(1, bytes).toString(16);
            while (string.length() < bytes.length*2) string = "0"+string;

        } else {
            string = new String(bytes, offset, length);
        }

        if (debug) log.debug(" - after : ["+string+"]");

        return string;
    }

    public static byte[] decode(String string) throws Exception {
        return decode(null, string);
    }

    public static byte[] decode(String encoding, String string) throws Exception {
        if (string == null) return null;

        if (debug) {
            log.debug("Encoding "+string.length()+" char(s) with "+encoding+":");
            log.debug(" - before: ["+string+"]");
        }
        byte[] bytes;

        if (BASE64.equals(encoding)) {
            bytes = Base64.decode(string);

        } else if (BIG_INTEGER.equals(encoding)) {
            BigInteger bigInteger = new BigInteger(string, 16);
            int length = (string.length()+1) / 2;
            bytes = bigInteger.toByteArray();

            if (bytes.length > length) {
                byte[] b = new byte[length];
                System.arraycopy(bytes, bytes.length - length, b, 0, length);
                bytes = b;

            } else if (bytes.length < length) {
                byte[] b = new byte[length];
                System.arraycopy(bytes, 0, b, length - bytes.length, bytes.length);
                bytes = b;
            }

        } else {
            bytes = string.getBytes();
        }

        if (debug) {
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) {
                String s = Integer.toHexString(0xff & b);
                if (s.length() == 1) s = "0"+s;
                sb.append(s);
            }
            log.debug(" - after : ["+sb+"]");
        }

        return bytes;
    }
}
