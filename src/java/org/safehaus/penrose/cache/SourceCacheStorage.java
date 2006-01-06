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
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.ConnectionConfig;
import org.safehaus.penrose.session.PenroseSearchResults;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public abstract class SourceCacheStorage extends Cache {

    Partition partition;
    Connector connector;
    SourceConfig sourceConfig;

    public abstract int getLastChangeNumber() throws Exception;
    public abstract void setLastChangeNumber(int lastChangeNumber) throws Exception;
    public abstract Map load(Collection filters, Collection missingKeys) throws Exception;

    public SourceConfig getSourceDefinition() {
        return sourceConfig;
    }

    public void setSourceDefinition(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    public void init() throws Exception {
        super.init();

        String s = sourceConfig.getParameter(SourceConfig.DATA_CACHE_SIZE);
        if (s != null) size = Integer.parseInt(s);

        s = sourceConfig.getParameter(SourceConfig.DATA_CACHE_EXPIRATION);
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
        String s = sourceConfig.getParameter(SourceConfig.AUTO_REFRESH);
        boolean autoRefresh = s == null ? SourceConfig.DEFAULT_AUTO_REFRESH : new Boolean(s).booleanValue();

        if (!autoRefresh) return;

        log.debug("Loading cache for "+sourceConfig.getName());

        ConnectionConfig conCfg = partition.getConnectionConfig(sourceConfig.getConnectionName());
        Connection connection = connector.getConnection(conCfg.getName());

        PenroseSearchResults sr = connection.load(sourceConfig, null, 100);

        //log.debug("Results:");
        while (sr.hasNext()) {
            AttributeValues sourceValues = (AttributeValues)sr.next();
            Row pk = sourceConfig.getPrimaryKeyValues(sourceValues);
            //log.debug(" - "+pk+": "+sourceValues);

            put(pk, sourceValues);
        }

        int lastChangeNumber = connection.getLastChangeNumber(sourceConfig);
        setLastChangeNumber(lastChangeNumber);
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }
}
