/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;


import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.AttributeValues;
import org.ietf.ldap.LDAPAttributeSet;
import org.ietf.ldap.LDAPAttribute;
import org.ietf.ldap.LDAPEntry;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Entry {

    private EntryDefinition entryDefinition;
    private AttributeValues attributeValues;

    public Entry(EntryDefinition entry, AttributeValues attributes) {
        this.entryDefinition = entry;
        this.attributeValues = attributes;
    }

    public String getDn() {
        return entryDefinition.getDn(attributeValues);
    }

    public String getRdn() {
        return entryDefinition.getRdn(attributeValues);
    }

    public AttributeValues getAttributeValues() {
        return attributeValues;
    }

    public EntryDefinition getEntryDefinition() {
        return entryDefinition;
    }

    public Collection getSources() {
        return entryDefinition.getSources();
    }

    public Collection getRelationships() {
        return entryDefinition.getRelationships();
    }

    public void setEntryDefinition(EntryDefinition entryDefinition) {
        this.entryDefinition = entryDefinition;
    }

    public Collection getObjectClasses() {
        return entryDefinition.getObjectClasses();
    }

    public LDAPEntry toLDAPEntry() {
        return entryDefinition.toLDAPEntry(attributeValues);
    }

    public String toString() {
        return entryDefinition.toString(attributeValues);
    }

    public static Map parseRdn(String rdn) throws Exception {
        Map map = new HashMap();
        StringTokenizer st = new StringTokenizer(rdn, "+");

        while (st.hasMoreTokens()) {
            String rdnComponent = st.nextToken();

            int i = rdnComponent.indexOf("=");
            String name = rdnComponent.substring(0, i);
            String value = rdnComponent.substring(i+1);

            map.put(name, value);
        }

        return map;
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

    public static LDAPEntry filterAttributes(
            LDAPEntry ldapEntry,
            Collection attributeNames)
            throws Exception {

        if (attributeNames == null || attributeNames.size() == 0 || attributeNames.contains("*")) return ldapEntry;

        LDAPAttributeSet attributeSet = ldapEntry.getAttributeSet();
        Collection list = new ArrayList();

        for (Iterator i=attributeSet.iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();
            String name = attribute.getName();

            if (attributeNames.contains(name)) list.add(attribute);
        }

        attributeSet.retainAll(list);

        return ldapEntry;
    }

    public boolean isDynamic() {
        return entryDefinition.isDynamic();
    }
}
