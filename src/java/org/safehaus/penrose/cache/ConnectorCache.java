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

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.Connection;
import org.safehaus.penrose.connector.Adapter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.SearchResults;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class ConnectorCache extends Cache {

    Connector connector;
    SourceDefinition sourceDefinition;

    public abstract int getLastChangeNumber() throws Exception;
    public abstract void setLastChangeNumber(int lastChangeNumber) throws Exception;
    public abstract Map load(Collection filters, Collection missingKeys) throws Exception;

    public SourceDefinition getSourceDefinition() {
        return sourceDefinition;
    }

    public void setSourceDefinition(SourceDefinition sourceDefinition) {
        this.sourceDefinition = sourceDefinition;
    }

    public void init() throws Exception {
        super.init();

        String s = sourceDefinition.getParameter(SourceDefinition.DATA_CACHE_SIZE);
        if (s != null) size = Integer.parseInt(s);

        s = sourceDefinition.getParameter(SourceDefinition.DATA_CACHE_EXPIRATION);
        if (s != null) expiration = Integer.parseInt(s);
    }

    public void create() throws Exception {
    }

    public void clean() throws Exception {
    }

    public void drop() throws Exception {
    }

    public Collection search(Filter filter) throws Exception {
        return null;
    }

    public void put(Filter filter, Collection pks) throws Exception {
    }

    public void invalidate() throws Exception {
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public void load() throws Exception {
        String s = sourceDefinition.getParameter(SourceDefinition.AUTO_REFRESH);
        boolean autoRefresh = s == null ? SourceDefinition.DEFAULT_AUTO_REFRESH : new Boolean(s).booleanValue();

        if (!autoRefresh) return;

        Config config = connector.getConfig(sourceDefinition);
        ConnectionConfig conCfg = config.getConnectionConfig(sourceDefinition.getConnectionName());
        Connection connection = connector.getConnection(conCfg.getConnectionName());
        Adapter adapter = connection.getAdapter();

        SearchResults sr = adapter.load(sourceDefinition, null, 100);

        //log.debug("Results:");
        while (sr.hasNext()) {
            AttributeValues sourceValues = (AttributeValues)sr.next();
            Row pk = sourceDefinition.getPrimaryKeyValues(sourceValues);
            //log.debug(" - "+pk+": "+sourceValues);

            put(pk, sourceValues);
        }

        int lastChangeNumber = adapter.getLastChangeNumber(sourceDefinition);
        setLastChangeNumber(lastChangeNumber);
    }
}
