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
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.Attributes;

import java.util.Map;
import java.util.HashMap;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Entry {

    protected DN dn;
    protected EntryMapping entryMapping;
    protected Attributes attributes;

    protected Map sourceValues = new HashMap();

    public Entry() {
    }

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
        this.attributes = attributes;
    }

    public Entry(RDN rdn, EntryMapping entryMapping, Attributes attributes) {
        this.dn = new DN(rdn);
        this.entryMapping = entryMapping;
        this.attributes = attributes;
    }

    public Entry(DN dn, EntryMapping entryMapping, Attributes attributes) {
        this.dn = dn;
        this.entryMapping = entryMapping;
        this.attributes = attributes;
    }

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = new DN(dn);
    }

    public void setDn(RDN rdn) {
        this.dn = new DN(rdn);
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
        this.attributes = attributes;
    }

    public Collection getSourceNames() {
        return sourceValues.keySet();
    }
    
    public Attributes getSourceValues(String sourceName) {
        return (Attributes)sourceValues.get(sourceName);
    }

    public void setSourceValues(String sourceName, Attributes attributes) {
        sourceValues.put(sourceName, attributes);
    }
}
