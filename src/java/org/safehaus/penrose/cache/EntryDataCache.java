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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Entry;

/**
 * @author Endi S. Dewata
 */
public abstract class EntryDataCache extends Cache {

    Entry parent;
    EntryDefinition entryDefinition;

    public EntryDefinition getEntryDefinition() {
        return entryDefinition;
    }

    public void setEntryDefinition(EntryDefinition entryDefinition) {
        this.entryDefinition = entryDefinition;

        String s = entryDefinition.getParameter(EntryDefinition.DATA_CACHE_SIZE);
        size = s == null ? EntryDefinition.DEFAULT_DATA_CACHE_SIZE : Integer.parseInt(s);

        s = entryDefinition.getParameter(EntryDefinition.DATA_CACHE_EXPIRATION);
        expiration = s == null ? EntryDefinition.DEFAULT_DATA_CACHE_EXPIRATION : Integer.parseInt(s);
    }

    public Entry getParent() {
        return parent;
    }

    public void setParent(Entry parent) {
        this.parent = parent;
    }
}
