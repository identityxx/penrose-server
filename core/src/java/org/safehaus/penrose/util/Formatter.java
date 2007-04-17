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

import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class Formatter {

    public static Logger log = LoggerFactory.getLogger(Formatter.class);

    public static Collection split(String s, int length) {
        Collection list = new ArrayList();
        while (s.length() > length-4) {
            list.add(s.substring(0, length-4));
            s = s.substring(length-4);
        }
        list.add(s);
        return list;
    }

    public static String displaySeparator(int length) {
        return "+"+repeat("-", length-2)+"+";
    }

    public static String displayLine(String string, int length) {
        return "| "+rightPad(string, length-4)+" |";
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

    public static void printEntry(Entry entry) throws Exception {

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("Entry: "+entry.getDn(), 80));

        Attributes attributes = entry.getAttributes();
        if (attributes != null) {
            for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
                Attribute attribute = (Attribute)i.next();
                String name = attribute.getName();

                for (Iterator k=attribute.getValues().iterator(); k.hasNext(); ) {
                    Object value = k.next();
                    String className = value.getClass().getName();
                    className = className.substring(className.lastIndexOf(".")+1);

                    log.debug(Formatter.displayLine(" - "+name+": "+value+" ("+className+")", 80));
                }
            }
        }

        for (Iterator i=entry.getSourceNames().iterator(); i.hasNext(); ) {
            String sourceName = (String)i.next();
            Attributes sourceValues = entry.getSourceValues(sourceName);

            for (Iterator j=sourceValues.getAll().iterator(); j.hasNext(); ) {
                Attribute attribute = (Attribute)j.next();
                String fieldName = sourceName+"."+attribute.getName();

                for (Iterator k=attribute.getValues().iterator(); k.hasNext(); ) {
                    Object value = k.next();
                    String className = value.getClass().getName();
                    className = className.substring(className.lastIndexOf(".")+1);

                    log.debug(Formatter.displayLine(" - "+fieldName+": "+value+" ("+className+")", 80));
                }
            }
        }

        log.debug(Formatter.displaySeparator(80));
    }
}
