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
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;

import javax.naming.directory.*;
import javax.naming.NamingEnumeration;
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

    public static LDAPEntry convert(String dn, Attributes attributes) throws Exception {

        LDAPAttributeSet attributeSet = new LDAPAttributeSet();

        log.debug("Converting:");
        for (Enumeration en = attributes.getAll(); en.hasMoreElements(); ) {
            Attribute attribute = (Attribute)en.nextElement();

            boolean binary = false;
            try {
                DirContext ctx = attribute.getAttributeSyntaxDefinition();
            } catch (Exception e) {
                binary = "SyntaxDefinition/1.3.6.1.4.1.1466.115.121.1.40".equals(e.getMessage());
            }
            log.debug(" - "+attribute.getID()+": "+(binary ? "(binary)" : "(not binary)"));

            LDAPAttribute attr = new LDAPAttribute(attribute.getID());

            for (Enumeration values = attribute.getAll(); values.hasMoreElements(); ) {
                Object value = values.nextElement();
                log.debug("   "+value.getClass().getName());
                if (value instanceof String) {
                    attr.addValue((String)value);
                } else {
                    attr.addValue(value.toString());
                }
            }

            attributeSet.add(attr);
        }

        return new LDAPEntry(dn, attributeSet);
    }

    public static Attributes convert(LDAPEntry entry) throws Exception {
        Attributes attributes = new BasicAttributes();
        if (entry == null) return attributes;
        
        LDAPAttributeSet attributeSet = entry.getAttributeSet();
        for (Iterator j=attributeSet.iterator(); j.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)j.next();
            String name = attribute.getName();

            String values[] = attribute.getStringValueArray();
            if (values == null || values.length == 0) continue;

            Attribute attr = new BasicAttribute(name);
            for (int k = 0; k<values.length; k++) {
                String value = values[k];
                attr.add(value.toString());
            }
            attributes.put(attr);
        }

        return attributes;
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

    public static String toString(LDAPEntry entry) throws Exception {

        StringBuffer sb = new StringBuffer();
        sb.append("dn: " + entry.getDN() + "\n");

        LDAPAttributeSet attributeSet = entry.getAttributeSet();
        for (Iterator i = attributeSet.iterator(); i.hasNext();) {
            LDAPAttribute attribute = (LDAPAttribute) i.next();

            String name = attribute.getName();
            String values[] = attribute.getStringValueArray();

            for (int j = 0; j < values.length; j++) {
                sb.append(name + ": " + values[j] + "\n");
            }
        }

        return sb.toString();
    }

    public static void filterAttributes(
            LDAPEntry ldapEntry,
            Collection attributeNames)
            throws Exception {

        if (attributeNames == null || attributeNames.contains("*")) return;

        LDAPAttributeSet attributeSet = ldapEntry.getAttributeSet();
        Collection list = new ArrayList();

        for (Iterator i=attributeSet.iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();
            String name = attribute.getName().toLowerCase();

            if (attributeNames.contains(name)) list.add(attribute);
        }

        attributeSet.retainAll(list);
    }
}
