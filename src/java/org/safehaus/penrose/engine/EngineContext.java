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
package org.safehaus.penrose.engine;

import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.cache.EntryDataCache;
import org.safehaus.penrose.cache.EntryFilterCache;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.sync.SyncService;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Entry;
import org.safehaus.penrose.mapping.Source;

/**
 * @author Endi S. Dewata
 */
public interface EngineContext {

    public String getRootDn() throws Exception;
    public String getRootPassword() throws Exception;

    public EntryFilterCache getEntryFilterCache(String parentDn, EntryDefinition entryDefinition) throws Exception;
    public EntryDataCache getEntryDataCache(String parentDn, EntryDefinition entryDefinition) throws Exception;

    public Schema getSchema() throws Exception;
    public FilterTool getFilterTool() throws Exception;
    public Interpreter newInterpreter() throws Exception;
    public Config getConfig(Source source) throws Exception;
    public Config getConfig(String dn) throws Exception;
    public TransformEngine getTransformEngine() throws Exception;
    public SyncService getSyncService() throws Exception;
    public Connection getConnection(String connectionName) throws Exception;
}
