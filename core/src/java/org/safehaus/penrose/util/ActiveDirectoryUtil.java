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
