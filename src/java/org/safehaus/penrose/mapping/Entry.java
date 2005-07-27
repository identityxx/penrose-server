/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
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

    private Entry parent;
    private EntryDefinition entryDefinition;
    private AttributeValues attributeValues;

    public Entry(EntryDefinition entry, AttributeValues attributes) {
        this.entryDefinition = entry;
        this.attributeValues = attributes;
    }

    public String getDn() {
        String dn;

        if (isDynamic()) {
            Collection rdnAttributes = entryDefinition.getRdnAttributes();

            // TODO fix for multiple rdn attributes
            AttributeDefinition rdnAttribute = (AttributeDefinition)rdnAttributes.iterator().next();

            // TODO fix for multiple values
            Collection rdnValues = attributeValues.get(rdnAttribute.getName());
            Object rdnValue = rdnValues.iterator().next();

            StringBuffer sb = new StringBuffer();
            sb.append(rdnAttribute.getName());
            sb.append("=");
            sb.append(rdnValue);

            if (parent != null) {
                //System.out.println("parent: "+parent);
                sb.append(",");
                sb.append(parent.getDn());

            } else if (entryDefinition.getParent() != null) {
                //System.out.println("parent dn: "+entryDefinition.getParentDn());
                sb.append(",");
                sb.append(entryDefinition.getParentDn());

            } else {
                //System.out.println("no parent");
            }

            dn = sb.toString();

        } else {
            dn = entryDefinition.getDn();
        }

        //System.out.println("DN: "+dn);

        return dn;
    }

    public Row getRdn() throws Exception {

        Row rdn = new Row();

        Collection rdnAttributes = entryDefinition.getRdnAttributes();

        for (Iterator i = rdnAttributes.iterator(); i.hasNext();) {
            AttributeDefinition attributeDefinition = (AttributeDefinition) i.next();

            String name = attributeDefinition.getName();
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
        return entryDefinition.toLDAPEntry(getDn(), attributeValues);
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("dn: "+getDn()+"\n");

        for (Iterator i = entryDefinition.getObjectClasses().iterator(); i.hasNext(); ) {
            String oc = (String)i.next();
            sb.append("objectClass: "+oc+"\n");
        }

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
/*
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
*/
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

    public Entry getParent() {
        return parent;
    }

    public void setParent(Entry parent) {
        this.parent = parent;
    }
}
