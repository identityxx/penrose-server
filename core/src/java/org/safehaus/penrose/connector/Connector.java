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
package org.safehaus.penrose.connector;

import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.partition.*;
import org.safehaus.penrose.cache.SourceCacheManager;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.AttributeValues;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.BasicAttribute;
import javax.naming.directory.Attribute;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.DirContext;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Connector {

    static Logger log = LoggerFactory.getLogger(Connector.class);

    public final static String DEFAULT_CACHE_CLASS = SourceCacheManager.class.getName();

    private PenroseConfig penroseConfig;
    private ConnectorConfig connectorConfig;

    private ConnectionManager connectionManager;
    private PartitionManager partitionManager;
    private SourceCacheManager sourceCacheManager;

    private boolean stopping = false;

    public Connector() {
    }

    public void init() throws Exception {
        sourceCacheManager = new SourceCacheManager();
        sourceCacheManager.setCacheConfig(penroseConfig.getSourceCacheConfig());
        sourceCacheManager.setConnector(this);
    }

    public void start() throws Exception {
    }

    public boolean isStopping() {
        return stopping;
    }

    public void stop() throws Exception {
        if (stopping) return;
        stopping = true;
    }


    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void setPartitionManager(PartitionManager partitionManager) throws Exception {
        this.partitionManager = partitionManager;
    }

    public void addPartition(Partition partition) throws Exception {

        Collection sourceConfigs = partition.getSourceConfigs();
        for (Iterator i=sourceConfigs.iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();

            String connectorName = sourceConfig.getParameter("connectorName");
            connectorName = connectorName == null ? "DEFAULT" : connectorName;

            if (!connectorConfig.getName().equals(connectorName)) continue;

            sourceCacheManager.create(partition, sourceConfig);
        }
    }

    public Connection getConnection(Partition partition, String name) throws Exception {
        return (Connection)connectionManager.getConnection(partition, name);
    }

    public RDN normalize(RDN rdn) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        if (rdn == null) return rb.toRdn();

        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = rdn.get(name);

            if (value == null) continue;

            if (value instanceof String) {
                value = ((String)value).toLowerCase();
            }

            rb.set(name, value);
        }

        return rb.toRdn();
    }

    public void bind(Partition partition, SourceConfig sourceConfig, EntryMapping entry, RDN pk, String password) throws Exception {

        if (log.isDebugEnabled()) log.debug("Binding as entry in "+sourceConfig.getName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());
        connection.bind(sourceConfig, pk, password);
    }

    public void add(
            Partition partition,
            SourceConfig sourceConfig,
            AttributeValues sourceValues
    ) throws Exception {

    	if (log.isDebugEnabled()) {
    		log.debug("----------------------------------------------------------------");
    		log.debug("Adding entry into "+sourceConfig.getName());
    		log.debug("Values: "+sourceValues);
    	}

        Collection pks = TransformEngine.getPrimaryKeys(sourceConfig, sourceValues);

        // Add rows
        for (Iterator i = pks.iterator(); i.hasNext();) {
            RDN pk = (RDN) i.next();
            AttributeValues newEntry = (AttributeValues)sourceValues.clone();
            newEntry.set("primaryKey", pk);
            if (log.isDebugEnabled()) {
            	log.debug("Adding entry: "+pk);
            	log.debug(" - "+newEntry);
            }

            // Add row to source table in the source database/directory
            Connection connection = getConnection(partition, sourceConfig.getConnectionName());
            connection.add(sourceConfig, pk, newEntry);
        }
    }

    public void delete(Partition partition, SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {

    	if (log.isDebugEnabled()) log.debug("Deleting entry in "+sourceConfig.getName()+": "+sourceValues);

        Collection pks = TransformEngine.getPrimaryKeys(sourceConfig, sourceValues);

        // Remove rows
        for (Iterator i = pks.iterator(); i.hasNext();) {
            RDN pk = (RDN)i.next();
            AttributeValues oldEntry = (AttributeValues)sourceValues.clone();
            oldEntry.set(pk);
            if (log.isDebugEnabled()) log.debug("DELETE ("+pk+"): "+oldEntry);

            // Delete row from source table in the source database/directory
            Connection connection = getConnection(partition, sourceConfig.getConnectionName());
            connection.delete(sourceConfig, pk);
        }
    }

    public void modify(
            Partition partition,
            SourceConfig sourceConfig,
            RDN pk,
            Collection modifications,
            AttributeValues oldSourceValues,
            AttributeValues newSourceValues
    ) throws Exception {

    	if (log.isDebugEnabled()) log.debug("Modifying entry in " + sourceConfig.getName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());

        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            ModificationItem mi = (ModificationItem)i.next();
            int op = mi.getModificationOp();
            Attribute attribute = mi.getAttribute();
            String name = attribute.getID();

            if (!pk.contains(name)) continue;

            AttributeValues av = new AttributeValues();
            av.add(pk);

            switch (op) {
                case DirContext.ADD_ATTRIBUTE:
                    connection.add(sourceConfig, pk, av);

                case DirContext.REPLACE_ATTRIBUTE:
                    connection.add(sourceConfig, pk, av);

                case DirContext.REMOVE_ATTRIBUTE:
                    connection.delete(sourceConfig, pk);
            }
        }

        connection.modify(sourceConfig, pk, modifications);
    }

    public void modrdn(
            Partition partition,
            SourceConfig sourceConfig,
            RDN oldPk,
            RDN newPk,
            boolean deleteOldRdn
    ) throws Exception {

    	if (log.isDebugEnabled()) log.debug("Renaming entry in " + sourceConfig.getName());

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());
        connection.modrdn(sourceConfig, oldPk, newPk, deleteOldRdn);
    }

    public Collection createModifications(AttributeValues oldValues, AttributeValues newValues) throws Exception {

        Collection list = new ArrayList();

        Set addAttributes = new HashSet(newValues.getNames());
        addAttributes.removeAll(oldValues.getNames());

        if (log.isDebugEnabled()) log.debug("Values to add:");
        for (Iterator i=addAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Collection values = newValues.get(name);
            Attribute attribute = new BasicAttribute(name);

            for (Iterator j = values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                if (log.isDebugEnabled()) log.debug(" - "+name+": "+value);
                attribute.add(value);
            }

            list.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attribute));
        }

        Set removeAttributes = new HashSet(oldValues.getNames());
        removeAttributes.removeAll(newValues.getNames());

        if (log.isDebugEnabled()) log.debug("Values to remove:");
        for (Iterator i=removeAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Collection values = newValues.get(name);
            Attribute attribute = new BasicAttribute(name);
            for (Iterator j = values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                if (log.isDebugEnabled()) log.debug(" - "+name+": "+value);
                attribute.add(value);
            }

            list.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, attribute));
        }

        Set replaceAttributes = new HashSet(oldValues.getNames());
        replaceAttributes.retainAll(newValues.getNames());

        if (log.isDebugEnabled()) log.debug("Values to replace:");
        for (Iterator i=replaceAttributes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            if (name.startsWith("primaryKey.")) continue;
            
            Set set = (Set)newValues.get(name);
            Attribute attribute = new BasicAttribute(name);
            for (Iterator j = set.iterator(); j.hasNext(); ) {
                Object value = j.next();
                if (log.isDebugEnabled()) log.debug(" - "+name+": "+value);
                attribute.add(value);
            }

            list.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attribute));
        }

        return list;
    }

    /**
     * Search the data sources.
     * @param partition
     * @param entryMapping
     * @param sourceMapping
     * @param sourceConfig
     * @param primaryKeys
     * @param filter
     * @param searchControls
     * @param results Collection of source values. @throws Exception
     */
    public void search(
            final Partition partition,
            final EntryMapping entryMapping,
            final SourceMapping sourceMapping,
            final SourceConfig sourceConfig,
            final Collection primaryKeys,
            final Filter filter,
            final PenroseSearchControls searchControls,
            final Results results
    ) throws Exception {

        Connection connection = getConnection(partition, sourceConfig.getConnectionName());

        long sizeLimit = searchControls.getSizeLimit();
        String s = sourceConfig.getParameter(SourceConfig.SIZE_LIMIT);
        long maxSizeLimit = s == null ? SourceConfig.DEFAULT_SIZE_LIMIT : Integer.parseInt(s);

        int timeLimit = searchControls.getTimeLimit();
        s = sourceConfig.getParameter(SourceConfig.TIME_LIMIT);
        int maxTimeLimit = s == null ? SourceConfig.DEFAULT_TIME_LIMIT : Integer.parseInt(s);

        PenroseSearchControls sc = new PenroseSearchControls(searchControls);
        sc.setSizeLimit((sizeLimit > 0 && maxSizeLimit > 0) ? Math.min(sizeLimit, maxSizeLimit) : Math.max(sizeLimit, maxSizeLimit));
       	sc.setTimeLimit((timeLimit > 0 && maxTimeLimit > 0) ? Math.min(timeLimit, maxTimeLimit) : Math.max(timeLimit, maxTimeLimit));
        sc.setScope(searchControls.getScope());

        connection.search(partition, entryMapping, sourceMapping, sourceConfig, primaryKeys, filter, sc, results);
    }

    public RDN store(Partition partition, SourceConfig sourceConfig, AttributeValues sourceValues) throws Exception {
        RDN pk = sourceConfig.getPrimaryKeyValues(sourceValues);
        //RDN pk = sourceValues.getRdn();
        RDN npk = normalize(pk);

        if (log.isDebugEnabled()) log.debug("Storing source cache: "+pk);
        getSourceCacheManager().put(partition, sourceConfig, pk, sourceValues);

        Filter f = FilterTool.createFilter(npk);
        Collection c = new TreeSet();
        c.add(npk);

        if (log.isDebugEnabled()) log.debug("Storing filter cache "+f+": "+c);
        getSourceCacheManager().put(partition, sourceConfig, f, c);

        return npk;
    }

    public void store(Partition partition, SourceConfig sourceConfig, Collection values) throws Exception {

        Collection pks = new TreeSet();

        Collection uniqueFieldDefinitions = sourceConfig.getUniqueFieldConfigs();
        Collection uniqueKeys = new TreeSet();

        for (Iterator i=values.iterator(); i.hasNext(); ) {
            AttributeValues sourceValues = (AttributeValues)i.next();
            RDN npk = store(partition, sourceConfig, sourceValues);
            pks.add(npk);

            RDNBuilder rb = new RDNBuilder();
            for (Iterator j=uniqueFieldDefinitions.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                String fieldName = fieldConfig.getName();

                Object value = sourceValues.getOne(fieldName);

                rb.clear();
                rb.set(fieldName, value);

                uniqueKeys.add(rb.toRdn());
            }
        }

        if (!uniqueKeys.isEmpty()) {
            Filter f = FilterTool.createFilter(uniqueKeys);
            if (log.isDebugEnabled()) log.debug("Storing query cache "+f+": "+pks);
            getSourceCacheManager().put(partition, sourceConfig, f, pks);
        }

        if (pks.size() <= 10) {
            Filter filter = FilterTool.createFilter(pks);
            if (log.isDebugEnabled()) log.debug("Storing query cache "+filter+": "+pks);
            getSourceCacheManager().put(partition, sourceConfig, filter, pks);
        }
    }

    public ConnectorConfig getConnectorConfig() {
        return connectorConfig;
    }

    public void setConnectorConfig(ConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public PenroseConfig getServerConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public SourceCacheManager getSourceCacheManager() {
        return sourceCacheManager;
    }

    public void setSourceCacheManager(SourceCacheManager sourceCacheManager) {
        this.sourceCacheManager = sourceCacheManager;
    }
}
