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

import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.mapping.EntryMapping;
import org.ietf.ldap.LDAPEntry;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;
import org.ietf.ldap.LDAPDN;
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

    /**
     * Compare dn1 and dn2
     * @param dn1
     * @param dn2
     * @return true if dn1 == dn2
     * @throws Exception
     */
    public static boolean match(String dn1, String dn2) throws Exception {

        //log.debug("Matching ["+dn1+"] with ["+dn2+"]");
        RDN rdn1 = getRdn(dn1);
        RDN rdn2 = getRdn(dn2);

        Iterator i = rdn1.getNames().iterator();
        Iterator j = rdn2.getNames().iterator();

        while (i.hasNext() && j.hasNext()) {
            String name1 = (String)i.next();
            String name2 = (String)j.next();

            if (!name1.equalsIgnoreCase(name2)) {
                return false;
            }

            String value = (String)rdn1.get(name1);
            String value2 = (String)rdn2.get(name2);

            //log.debug(" - Comparing attribute values ["+value+"] with ["+value2+"]");
            if (!"...".equals(value) && !"...".equals(value2) && !value.equalsIgnoreCase(value2)) {
                return false;
            }
        }

        String parentDn1 = getParentDn(dn1);
        String parentDn2 = getParentDn(dn2);

        // if parents matches => true
        //log.debug(" - Comparing parents ["+parentDn1+"] with ["+parentDn2+"]");
        if (parentDn1 != null && parentDn2 != null && match(parentDn1, parentDn2)) {
            return true;
        }

        // if neither has parents => true
        return parentDn1 == null && parentDn2 == null;
    }

    public static String append(String dn, String suffix) {
        if (dn == null || "".equals(dn)) return suffix == null ? "" : suffix;
        if (suffix == null || "".equals(suffix)) return dn == null ? "" : dn;

        return dn+","+suffix;
    }

    public static String append(String dn, RDN rdn) {
        return append(dn, toString(rdn));
    }

    public static String append(RDN rdn, String dn) {
        return append(toString(rdn), dn);
    }

    public static String append(RDN rdn1, RDN rdn2) {
        return append(toString(rdn1), toString(rdn2));
    }

    public static RDN getRdn(String dn) {
        RDNBuilder rb = new RDNBuilder();
        if (dn == null || "".equals(dn)) return rb.toRdn();

        try {
            //log.debug("###### Getting RDN from "+dn);

            String rdns[] = LDAPDN.explodeDN(dn, false);
            String r = rdns[0];

            StringTokenizer st = new StringTokenizer(r, "+");

            while (st.hasMoreTokens()) {
                String s = LDAPDN.unescapeRDN(st.nextToken());
                int index = s.indexOf("=");

                String attribute = s.substring(0, index);
                if (attribute.startsWith("null")) attribute = attribute.substring(4);

                String value =  s.substring(index+1);

                //log.debug(" - "+attribute+": "+value);
                rb.set(attribute, value);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return rb.toRdn();
    }

    public static String getParentDn(String dn) {
        if (dn == null || "".equals(dn)) return null;

        try {
            //log.debug("###### Getting Parent DN from "+dn);

            String rdns[] = LDAPDN.explodeDN(dn, false);
            if (rdns.length == 1) return null;

            StringBuffer sb = new StringBuffer();
            for (int i=1; i<rdns.length; i++) {
                String r = rdns[i];

                StringTokenizer st = new StringTokenizer(r, "+");
                StringBuffer sb2 = new StringBuffer();

                while (st.hasMoreTokens()) {
                    String s = LDAPDN.unescapeRDN(st.nextToken());

                    int index = s.indexOf("=");

                    String attribute = s.substring(0, index);
                    if (attribute.startsWith("null")) attribute = attribute.substring(4);

                    String value =  s.substring(index+1);

                    String rdn = attribute+"="+value;
                    //log.debug("Processing "+rdn);

                    if (sb2.length() > 0) sb2.append("+");
                    sb2.append(LDAPDN.escapeRDN(rdn));
                }

                if (sb.length() > 0) sb.append(",");
                sb.append(sb2);
            }

            return sb.toString();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static String getSuffix(String dn) {
        if (dn == null || "".equals(dn)) return null;

        try {
            //log.debug("###### Getting suffix from "+dn);

            String rdns[] = LDAPDN.explodeDN(dn, false);
            if (rdns.length < 2) return dn;

            return LDAPDN.escapeRDN(rdns[rdns.length-1]);

            //int i = dn.lastIndexOf(",");
            //if (i<0) return dn;

            //String suffix = dn.substring(i+1);
            //log.debug(" - "+suffix);

            //return suffix.toString();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public static String getPrefix(String dn) {
        if (dn == null || "".equals(dn)) return null;

        try {
            //log.debug("###### Getting prefix from "+dn);

            String rdns[] = LDAPDN.explodeDN(dn, false);
            if (rdns.length < 2) return null;

            StringBuffer sb = new StringBuffer();
            for (int i=0; i<rdns.length-1; i++) {
                if (sb.length() > 0) sb.append(",");
                sb.append(LDAPDN.escapeRDN(rdns[i]));
            }

            return sb.toString();

            //int i = dn.lastIndexOf(",");
            //if (i<0) return null;

            //String prefix = dn.substring(0, i);
            //log.debug(" - "+prefix);

            //return prefix;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

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

                String className = value.getClass().getName();
                className = className.substring(className.lastIndexOf(".")+1);
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

        StringBuffer sb = new StringBuffer();
        sb.append("dn: ");
        sb.append(searchResult.getName());
        sb.append("\n");

        sb.append(toString(searchResult.getAttributes()));

        return sb.toString();
    }

    public static String toString(Entry entry) throws Exception {

        StringBuffer sb = new StringBuffer();
        sb.append("dn: ");
        sb.append(entry.getDn());
        sb.append("\n");

        sb.append(toString(getAttributes(entry)));

        return sb.toString();
    }

    public static String toString(Attributes attributes) throws Exception {

        StringBuffer sb = new StringBuffer();

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

    public static List parseDn(String dn) {
        List list = new ArrayList();

        RDNBuilder rb = new RDNBuilder();
        String compositeRdns[] = LDAPDN.explodeDN(dn, false);
        for (int i=0; i<compositeRdns.length; i++) {
            String compositeRdn = compositeRdns[i];

            StringTokenizer st = new StringTokenizer(compositeRdn, "+");

            rb.clear();
            while (st.hasMoreTokens()) {
                String s = LDAPDN.unescapeRDN(st.nextToken());

                int index = s.indexOf("=");

                String name = s.substring(0, index);
                if (name.startsWith("null")) name = name.substring(4);

                String value = s.substring(index+1);

                rb.set(name, value);
            }

            RDN rdn = rb.toRdn();
            list.add(rdn);
        }

        return list;
    }

    public static String getSubDn(List dn, int start, int length) {
        String newDn = null;

        for (int i=0; i<length; i++) {
            int p = i+start;
            if (p >= dn.size()) break;
            RDN rdn = (RDN)dn.get(p);
            append(newDn, toString(rdn));
        }

        return newDn;
    }

    public static String toString(RDN rdn) {
        StringBuffer sb = new StringBuffer();
        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);
            String r = name+"="+value;

            if (sb.length() > 0) sb.append("+");
            sb.append(LDAPDN.escapeRDN(r));
        }

        return sb.toString();
    }
}
