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
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.EntryUtil;
import org.apache.log4j.Logger;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Entry {

    Logger log = Logger.getLogger(getClass());

    private String dn;
    private String parentDn;
    private EntryMapping entryMapping;

    private AttributeValues sourceValues;
    private AttributeValues attributeValues;

    public Entry(String dn, EntryMapping entryMapping) {
        this.dn = dn;
        this.parentDn = EntryUtil.getParentDn(dn);
        this.entryMapping = entryMapping;
        this.sourceValues = new AttributeValues();
        this.attributeValues = new AttributeValues();
    }

    public Entry(String dn, EntryMapping entryMapping, AttributeValues attributes) {
        this.dn = dn;
        this.parentDn = EntryUtil.getParentDn(dn);
        this.entryMapping = entryMapping;
        this.sourceValues = new AttributeValues();
        this.attributeValues = attributes;

        //attributeValues.remove("objectClass");
    }

    public Entry(String dn, EntryMapping entryMapping, AttributeValues sourceValues, AttributeValues attributeValues) {
        this.dn = dn;
        this.parentDn = EntryUtil.getParentDn(dn);
        this.entryMapping = entryMapping;
        this.sourceValues = sourceValues;
        this.attributeValues = attributeValues;

        //attributeValues.remove("objectClass");
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
        return new LDAPEntry(dn, getAttributeSet());
    }

    public LDAPAttributeSet getAttributeSet() {
        LDAPAttributeSet set = new LDAPAttributeSet();

        //log.debug("Entry "+dn);
        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            LDAPAttribute attribute = new LDAPAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                if (value instanceof byte[]) {
                    attribute.addValue((byte[])value);
                    //log.debug(" - "+name+": (binary)");
                } else {
                    attribute.addValue(value.toString());
                    //log.debug(" - "+name+": "+value);
                }
            }

            set.add(attribute);
        }

        if (entryMapping != null) {
            Collection objectClasses = entryMapping.getObjectClasses();
            if (!objectClasses.isEmpty()) {
                LDAPAttribute objectClass = new LDAPAttribute("objectClass");
                for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
                    String oc = (String)i.next();
                    objectClass.addValue(oc);
                }

                set.add(objectClass);
                //log.debug(" - objectClass: "+objectClasses);
            }
        }

        return set;
    }

    public Attributes getAttributes() throws Exception {

        Attributes attributes = new BasicAttributes();

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);

            Attribute attribute = new BasicAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                attribute.add(value.toString());
/*
                if ("unicodePwd".equals(name)) {
                    attribute.add(PasswordUtil.toUnicodePassword(value.toString()));
                } else {
                    attribute.add(value.toString());
                }
*/
            }

            attributes.put(attribute);
        }

        if (entryMapping != null) {
            Collection objectClasses = entryMapping.getObjectClasses();

            Attribute objectClass = new BasicAttribute("objectClass");
            for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
                String oc = (String)i.next();
                objectClass.add(oc);
            }

            attributes.put(objectClass);
        }

        return attributes;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("dn: "+getDn()+"\n");

        for (Iterator i = entryMapping.getObjectClasses().iterator(); i.hasNext(); ) {
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

}
