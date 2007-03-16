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
package org.safehaus.penrose.entry;

import org.safehaus.penrose.mapping.EntryMapping;

/**
 * @author Endi S. Dewata
 */
public class Entry {

    protected DN dn;
    private Attributes attributes;

    protected EntryMapping entryMapping;
    protected AttributeValues sourceValues;

    public Entry(String dn, EntryMapping entryMapping) {
        this(new DN(dn), entryMapping);
    }

    public Entry(DN dn, EntryMapping entryMapping) {
        this.dn = dn;
        this.attributes = new Attributes();

        this.entryMapping = entryMapping;
        this.sourceValues = new AttributeValues();
    }

    public Entry(String dn, EntryMapping entryMapping, Attributes attributes) {
        this(new DN(dn), entryMapping, attributes);
    }

    public Entry(DN dn, EntryMapping entryMapping, Attributes attributes) {
        this.dn = dn;
        this.attributes = attributes;

        this.entryMapping = entryMapping;
        this.sourceValues = new AttributeValues();
    }

    public Entry(String dn, EntryMapping entryMapping, Attributes attributes, AttributeValues sourceValues) {
        this(new DN(dn), entryMapping, attributes, sourceValues);
    }

    public Entry(DN dn, EntryMapping entryMapping, Attributes attributes, AttributeValues sourceValues) {
        this.dn = dn;
        this.attributes = attributes;

        this.entryMapping = entryMapping;
        this.sourceValues = sourceValues;
    }

    public DN getDn() {
        return dn;
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public void setEntryMapping(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }

    public AttributeValues getSourceValues() {
        return sourceValues;
    }

    public void setSourceValues(AttributeValues sourceValues) {
        this.sourceValues = sourceValues;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public void setAttributes(Attributes attributes) {
        this.attributes = attributes;
    }
}
