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

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.engine.Engine;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public abstract class EntryCache extends Cache {

    String parentDn;
    EntryMapping entryMapping;

    Engine engine;

    public void init() throws Exception {
        super.init();

        String s = entryMapping.getParameter(EntryMapping.DATA_CACHE_SIZE);
        if (s != null) size = Integer.parseInt(s);

        s = entryMapping.getParameter(EntryMapping.DATA_CACHE_EXPIRATION);
        if (s != null) expiration = Integer.parseInt(s);
    }

    public EntryMapping getEntryMapping() {
        return entryMapping;
    }

    public void setEntryMapping(EntryMapping entryMapping) {
        this.entryMapping = entryMapping;
    }

    public String getParentDn() {
        return parentDn;
    }

    public void setParentDn(String parentDn) {
        this.parentDn = parentDn;
    }

    public Collection get(Filter filter) throws Exception {
        return null;
    }

    public void put(Filter filter, Collection rdns) throws Exception {
    }

    public void invalidate() throws Exception {
    }

    public void create() throws Exception {
    }

    public void load() throws Exception {
    }

    public void clean() throws Exception {
    }

    public void drop() throws Exception {
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }
}
