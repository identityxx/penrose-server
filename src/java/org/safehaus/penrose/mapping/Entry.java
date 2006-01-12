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
package org.safehaus.penrose.mapping;


import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;
import org.ietf.ldap.LDAPEntry;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Entry {

    private String dn;
    private String parentDn;
    private EntryMapping entryMapping;

    private AttributeValues sourceValues;
    private AttributeValues attributeValues;

    public Entry(String dn, EntryMapping entryMapping) {
        this.dn = dn;
        this.parentDn = getParentDn(dn);
        this.entryMapping = entryMapping;
        this.sourceValues = new AttributeValues();
        this.attributeValues = new AttributeValues();
    }

    public Entry(String dn, EntryMapping entryMapping, AttributeValues attributes) {
        this.dn = dn;
        this.parentDn = getParentDn(dn);
        this.entryMapping = entryMapping;
        this.sourceValues = new AttributeValues();
        this.attributeValues = attributes;
    }

    public Entry(String dn, EntryMapping entryMapping, AttributeValues sourceValues, AttributeValues attributeValues) {
        this.dn = dn;
        this.parentDn = getParentDn(dn);
        this.entryMapping = entryMapping;
        this.sourceValues = sourceValues;
        this.attributeValues = attributeValues;
    }

    public String getDn() {
        return dn;
    }

    public Row getRdn() throws Exception {

        Row rdn = new Row();

        Collection rdnAttributes = entryMapping.getRdnAttributes();

        for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
            AttributeMapping attributeMapping = (AttributeMapping) i.next();

            String name = attributeMapping.getName();
            Collection values = attributeValues.get(name);
            if (values == null) return null;

            Object value = values.iterator().next();
            rdn.set(name, value);
        }

        return rdn;
    }

    public AttributeValues getAttributeValues() {
        return attributeValues;
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public Collection getSources() {
        return entryMapping.getSourceMappings();
    }

    public Collection getRelationships() {
        return entryMapping.getRelationships();
    }

    public void setEntryMapping(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }

    public Collection getObjectClasses() {
        return entryMapping.getObjectClasses();
    }

    public LDAPEntry toLDAPEntry() {
        return new LDAPEntry(dn, getAttributeSet(attributeValues));
    }

    public static LDAPAttributeSet getAttributeSet(AttributeValues attributeValues) {
        LDAPAttributeSet set = new LDAPAttributeSet();

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            LDAPAttribute attribute = new LDAPAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                attribute.addValue(value.toString());
            }

            set.add(attribute);
        }

        return set;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("dn: "+getDn()+"\n");
/*
        for (Iterator i = entryMappingtClasses().iterator(); i.hasNext(); ) {
            String oc = (String)i.next();
            sb.append("objectClass: "+oc+"\n");
        }
*/
        for (Iterator i = attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            for (Iterator j = values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                sb.append(name+": "+value+"\n");
            }
        }

        return sb.toString();
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

    public Collection getACL() {
        return entryMapping.getACL();
    }

    public AttributeValues getSourceValues() {
        return sourceValues;
    }

    public void setSourceValues(AttributeValues sourceValues) {
        this.sourceValues = sourceValues;
    }

    public String getParentDn() {
        return parentDn;
    }

    public void setParentDn(String parentDn) {
        this.parentDn = parentDn;
    }

    public static Row getRdn(String dn) {

        int index = dn.indexOf(",");
        String s = index < 0 ? dn : dn.substring(0, index);

        StringTokenizer st = new StringTokenizer(s, "+");
        Row rdn = new Row();

        while (st.hasMoreTokens()) {
            s = st.nextToken();
            index = s.indexOf("=");
            rdn.set(s.substring(0, index), s.substring(index+1));
        }

        return rdn;
    }

    public static String getParentDn(String dn) {
        if (dn == null) return null;
        int index = dn.indexOf(",");
        return index < 0 ? null : dn.substring(index+1);
    }

}
