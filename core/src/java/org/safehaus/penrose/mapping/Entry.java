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
package org.safehaus.penrose.mapping;


import org.safehaus.penrose.util.EntryUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Entry {

    Logger log = LoggerFactory.getLogger(getClass());

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
    }

    public Entry(String dn, EntryMapping entryMapping, AttributeValues sourceValues, AttributeValues attributeValues) {
        this.dn = dn;
        this.parentDn = EntryUtil.getParentDn(dn);
        this.entryMapping = entryMapping;
        this.sourceValues = sourceValues;
        this.attributeValues = attributeValues;
    }

    public String getDn() {
        return dn;
    }

    public Row getRdn() throws Exception {

        Row rdn = new Row();

        Collection rdnAttributes = entryMapping.getRdnAttributeNames();

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
