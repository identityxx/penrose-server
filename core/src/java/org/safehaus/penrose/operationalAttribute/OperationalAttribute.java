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
