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

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Entry {

    protected DN dn;
    protected EntryMapping entryMapping;
    protected Attributes attributes = new Attributes();

    public Entry(String dn) {
        this.dn = new DN(dn);
    }

    public Entry(RDN rdn) {
        dn = new DN(rdn);
    }

    public Entry(DN dn) {
        this.dn = dn;
    }

    public Entry(String dn, EntryMapping entryMapping) {
        this.dn = new DN(dn);
        this.entryMapping = entryMapping;
    }

    public Entry(RDN rdn, EntryMapping entryMapping) {
        this.dn = new DN(rdn);
        this.entryMapping = entryMapping;
    }

    public Entry(DN dn, EntryMapping entryMapping) {
        this.dn = dn;
        this.entryMapping = entryMapping;
    }

    public Entry(String dn, EntryMapping entryMapping, Attributes attributes) {
        this.dn = new DN(dn);
        this.entryMapping = entryMapping;
        setAttributes(attributes);
    }

    public Entry(RDN rdn, EntryMapping entryMapping, Attributes attributes) {
        this.dn = new DN(rdn);
        this.entryMapping = entryMapping;
        setAttributes(attributes);
    }

    public Entry(DN dn, EntryMapping entryMapping, Attributes attributes) {
        this.dn = dn;
        this.entryMapping = entryMapping;
        setAttributes(attributes);
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public void setEntryMapping(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }

    public Attributes getAttributes() {
        return attributes;
    }

    public void setAttributes(Attributes attributes) {
        setAttributes(null, attributes);
    }

    public void setAttributes(String prefix, Attributes attributes) {

        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String name = attribute.getName();
            name = prefix == null ? name : prefix+"."+name;

            Collection values = attribute.getValues();

            Attribute newAttribute = new Attribute(name, values);
            this.attributes.add(newAttribute);
        }
    }
}
