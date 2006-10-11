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

/**
 * @author Endi S. Dewata
 */
public class ActiveDirectoryUtil {      

    public static String getGUID(byte[] guid) {
        String strGUID = "{";
        strGUID = strGUID + byte2hex(guid[3]);
        strGUID = strGUID + byte2hex(guid[2]);
        strGUID = strGUID + byte2hex(guid[1]);
        strGUID = strGUID + byte2hex(guid[0]);
        strGUID = strGUID + "-";
        strGUID = strGUID + byte2hex(guid[5]);
        strGUID = strGUID + byte2hex(guid[4]);
        strGUID = strGUID + "-";
        strGUID = strGUID + byte2hex(guid[7]);
        strGUID = strGUID + byte2hex(guid[6]);
        strGUID = strGUID + "-";
        strGUID = strGUID + byte2hex(guid[8]);
        strGUID = strGUID + byte2hex(guid[9]);
        strGUID = strGUID + "-";
        strGUID = strGUID + byte2hex(guid[10]);
        strGUID = strGUID + byte2hex(guid[11]);
        strGUID = strGUID + byte2hex(guid[12]);
        strGUID = strGUID + byte2hex(guid[13]);
        strGUID = strGUID + byte2hex(guid[14]);
        strGUID = strGUID + byte2hex(guid[15]);
        strGUID = strGUID + "}";

        return strGUID;
    }

    public static String getSID(byte[] sid) {
        String strSID = "";
        int version;
        long authority;
        int count;
        String rid = "";
        strSID = "S";

         // get version
        version = sid[0];
        strSID = strSID + "-" + Integer.toString(version);
        for (int i=6; i>0; i--) {
            rid += byte2hex(sid[i]);
        }

        // get authority
        authority = Long.parseLong(rid);
        strSID = strSID + "-" + Long.toString(authority);

        //next byte is the count of sub-authorities
        count = sid[7]&0xFF;

        //iterate all the sub-auths
        for (int i=0;i<count;i++) {
            rid = "";
            for (int j=11; j>7; j--) {
                rid += byte2hex(sid[j+(i*4)]);
            }
            strSID = strSID + "-" + Long.parseLong(rid,16);
        }
        return strSID;
    }

    public static String byte2hex(byte b) {
        int i = (int)b & 0xFF;
        return (i <= 0x0F) ? "0" + Integer.toHexString(i) : Integer.toHexString(i);
    }

}
