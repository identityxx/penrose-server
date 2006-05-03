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

import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.apache.log4j.Logger;

import javax.naming.directory.*;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryUtil {

    static Logger log = Logger.getLogger(EntryUtil.class);

    /**
     * Compare dn1 and dn2
     * @param dn1
     * @param dn2
     * @return true if dn1 == dn2
     * @throws Exception
     */
    public static boolean match(String dn1, String dn2) throws Exception {

        //log.debug("Matching ["+dn1+"] with ["+dn2+"]");
        Row rdn1 = getRdn(dn1);
        Row rdn2 = getRdn(dn2);

        // if attribute types don't match => false
        //log.debug(" - Comparing attribute types ["+attr+"] with ["+attr2+"]");
        if (!rdn1.getNames().equals(rdn2.getNames())) return false;

        // if values are not dynamic and they don't match => false
        for (Iterator i=rdn1.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = (String)rdn1.get(name);
            String value2 = (String)rdn2.get(name);
            //log.debug(" - Comparing attribute values ["+value+"] with ["+value2+"]");
            if (!"...".equals(value) && !"...".equals(value2) && !value.equals(value2)) return false;
        }

        String parentDn1 = getParentDn(dn1);
        String parentDn2 = getParentDn(dn2);

        // if parents matches => true
        //log.debug(" - Comparing parents ["+parentDn1+"] with ["+parentDn2+"]");
        if (parentDn1 != null && parentDn2 != null && match(parentDn1, parentDn2)) return true;

        // if neither has parents => true
        return parentDn1 == null && parentDn2 == null;
    }

    public static String append(String dn, String suffix) {
        if (dn == null || "".equals(dn)) return suffix == null ? "" : suffix;
        if (suffix == null || "".equals(suffix)) return dn == null ? "" : dn;

        return dn+","+suffix;
    }

    public static Row getRdn(String dn) {

        Row rdn = new Row();
        if (dn == null || "".equals(dn)) return rdn;

        int index = dn.indexOf(",");
        String s = index < 0 ? dn : dn.substring(0, index);

        StringTokenizer st = new StringTokenizer(s, "+");

        while (st.hasMoreTokens()) {
            s = st.nextToken();
            index = s.indexOf("=");
            rdn.set(s.substring(0, index), s.substring(index+1));
        }

        return rdn;
    }

    public static String getParentDn(String dn) {
        if (dn == null || "".equals(dn)) return null;
        int index = dn.indexOf(",");
        return index < 0 ? null : dn.substring(index+1);
    }

    public static SearchResult toSearchResult(Entry entry) {
        return new SearchResult(entry.getDn(), entry, getAttributes(entry));
    }

    public static Attributes getAttributes(Entry entry) {

        AttributeValues attributeValues = entry.getAttributeValues();
        Attributes attributes = new BasicAttributes();

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            Attribute attribute = new BasicAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                attribute.add(value);
            }

            attributes.put(attribute);
        }

        EntryMapping entryMapping = entry.getEntryMapping();
        if (entryMapping != null) {
            Collection objectClasses = entryMapping.getObjectClasses();

            if (!objectClasses.isEmpty()) {
                Attribute objectClass = new BasicAttribute("objectClass");
                for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
                    String oc = (String)i.next();
                    objectClass.add(oc);
                }

                attributes.put(objectClass);
            }
        }

        return attributes;
    }
}
