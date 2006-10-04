package org.safehaus.penrose.operationalAttribute;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.Date;

/**
 * @author Endi S. Dewata
 */
public class OperationalAttribute {

    public static DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+00"));
    }

    public static Date parseDate(String date) throws Exception {
        if (!date.endsWith("Z")) return null;
        return dateFormat.parse(date.substring(0, date.length()-1));
    }

    public static String formatDate(Date date) throws Exception {
        return dateFormat.format(date)+"Z";
    }
}
