/**
 * Copyright 2009 Red Hat, Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class TextUtil {

    public static Logger log = LoggerFactory.getLogger(TextUtil.class);

    public static Collection<String> split(String s, int length) throws Exception {
        Collection<String> list = new ArrayList<String>();

        BufferedReader in = new BufferedReader(new StringReader(s));
        String line;
        
        while ((line = in.readLine()) != null) {
            while (line.length() > length-4) {
                list.add(line.substring(0, length-4));
                line = line.substring(length-4);
            }
            list.add(line);
        }

        return list;
    }

    public static String displaySeparator(int length) {
        return "+"+repeat("-", length-2)+"+";
    }

    public static String displayLine(String string, int length) {
        return "| "+rightPad(string, length-4)+" |";
    }

    public static String leftPad(String s, int length) {
        if (s == null) s = "";
        if (s.length() > length) return s.substring(0, length);
        return repeat(" ", length-s.length())+s;
    }

    public static String rightPad(String s, int length) {
        if (s == null) s = "";
        if (s.length() > length) return s.substring(0, length);
        return s+repeat(" ", length-s.length());
    }

    public static String repeat(String s, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<length; i++) sb.append(s);
        return sb.toString();
    }
}
