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
package org.safehaus.penrose.directory;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.ldap.DN;

/**
 * @author Endi S. Dewata
 */
public class Entry {

    protected EntryMapping entryMapping;

    public Entry(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }

    public String getId() {
        return entryMapping.getId();
    }
    
    public DN getDn() {
        return entryMapping.getDn();
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }
}
