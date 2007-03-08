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
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.mapping.EntryMapping;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryUtil {

    static Logger log = LoggerFactory.getLogger(EntryUtil.class);

    public static SearchResult toSearchResult(LDAPEntry entry) {
        return new SearchResult(entry.getDN(), entry, getAttributes(entry));
    }

    public static SearchResult toSearchResult(Entry entry) {
        return new SearchResult(entry.getDn().toString(), entry, getAttributes(entry));
    }

    public static Attributes getAttributes(LDAPEntry entry) {

        //log.debug("Converting attributes for "+entry.getDN());

        LDAPAttributeSet attributeSet = entry.getAttributeSet();
        Attributes attributes = new BasicAttributes();

        for (Iterator i=attributeSet.iterator(); i.hasNext(); ) {
            LDAPAttribute ldapAttribute = (LDAPAttribute)i.next();
            //log.debug(" - "+ldapAttribute.getName()+": "+Arrays.asList(ldapAttribute.getSubtypes()));
            Attribute attribute = new BasicAttribute(ldapAttribute.getName());

            for (Enumeration j=ldapAttribute.getStringValues(); j.hasMoreElements(); ) {
                String value = (String)j.nextElement();
                //log.debug("   - "+value);
                attribute.add(value);
            }

            attributes.put(attribute);
        }

        return attributes;
    }

    public static Attributes getAttributes(Entry entry) {

        //log.debug("Converting "+entry.getDn());

        AttributeValues attributeValues = entry.getAttributeValues();

        Attributes attributes = new BasicAttributes();
        if (attributeValues == null) return attributes;

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            Attribute attribute = new BasicAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();

                //String className = value.getClass().getName();
                //className = className.substring(className.lastIndexOf(".")+1);
                //log.debug(" - "+name+": "+value+" ("+className+")");

                if (value instanceof byte[]) {
                    attribute.add(value);

                } else {
                    attribute.add(value.toString());
                }
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

    public static String toString(SearchResult searchResult) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("dn: ");
        sb.append(searchResult.getName());
        sb.append("\n");

        sb.append(toString(searchResult.getAttributes()));

        return sb.toString();
    }

    public static String toString(Entry entry) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append("dn: ");
        sb.append(entry.getDn());
        sb.append("\n");

        sb.append(toString(getAttributes(entry)));

        return sb.toString();
    }

    public static String toString(Attributes attributes) throws Exception {

        StringBuilder sb = new StringBuilder();

        for (NamingEnumeration i = attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();

            for (NamingEnumeration j = attribute.getAll(); j.hasMore(); ) {
                Object value = j.next();

                String className = value.getClass().getName();
                className = className.substring(className.lastIndexOf(".")+1);

                sb.append(name);
                sb.append(": ");
                sb.append(value);
                sb.append(" (");
                sb.append(className);
                sb.append(")");
                sb.append("\n");
            }
        }

        return sb.toString();
    }
}
