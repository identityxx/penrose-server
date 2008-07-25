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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.source.SourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class PartitionConfigManager implements Serializable {

    private Map<String,PartitionConfig> partitionConfigs = new LinkedHashMap<String,PartitionConfig>();

    public PartitionConfigManager() {
    }

    public PartitionConfig removePartitionConfig(String name) throws Exception {
        return partitionConfigs.remove(name);
    }

    public void clear() throws Exception {
        partitionConfigs.clear();
    }

    public PartitionConfig getPartitionConfig(EntrySourceConfig sourceMapping) throws Exception {

        if (sourceMapping == null) return null;

        String sourceName = sourceMapping.getSourceName();
        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (partitionConfig.getSourceConfigManager().getSourceConfig(sourceName) != null) return partitionConfig;
        }
        return null;
    }

    public PartitionConfig getPartitionConfig(SourceConfig sourceConfig) throws Exception {

        if (sourceConfig == null) return null;

        String connectionName = sourceConfig.getConnectionName();
        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (partitionConfig.getConnectionConfigManager().getConnectionConfig(connectionName) != null) return partitionConfig;
        }
        return null;
    }

    public PartitionConfig getPartitionConfig(ConnectionConfig connectionConfig) throws Exception {

        if (connectionConfig == null) return null;

        String connectionName = connectionConfig.getName();
        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (partitionConfig.getConnectionConfigManager().getConnectionConfig(connectionName) != null) return partitionConfig;
        }
        return null;
    }

    public PartitionConfig getPartitionConfig(EntryConfig entryConfig) throws Exception {

        if (entryConfig == null) return null;

        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (partitionConfig.getDirectoryConfig().contains(entryConfig)) {
                return partitionConfig;
            }
        }

        return null;
    }

    public PartitionConfig getPartitionConfig(DN dn) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());
        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Finding partition for \""+dn+"\".");

        if (dn == null) {
            log.debug("DN is null.");
            return null;
        }

        PartitionConfig p = null;
        DN s = null;

        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (debug) log.debug("Checking "+partitionConfig.getName()+" partition.");

            Collection<DN> suffixes = partitionConfig.getDirectoryConfig().getSuffixes();
            for (DN suffix : suffixes) {
                if (suffix.isEmpty() && dn.isEmpty() // Root DSE
                        || dn.endsWith(suffix)) {

                    if (s == null || s.getSize() < suffix.getSize()) {
                        p = partitionConfig;
                        s = suffix;
                    }
                }
            }
        }

        if (debug) {
            if (p == null) {
                log.debug("Partition not found.");
            } else {
                log.debug("Found "+p.getName()+" partition.");
            }
        }

        return p;
    }

    public Collection<String> getPartitionNames() {
        return partitionConfigs.keySet();
    }

    public void addPartitionConfig(PartitionConfig partitionConfig) {
        partitionConfigs.put(partitionConfig.getName(), partitionConfig);
    }

    public Collection<String> getLoadOrder() throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());

        log.debug("Computing load order.");
        
        Map<String,Collection<String>> dependents = new LinkedHashMap<String,Collection<String>>();

        for (String partitionName : partitionConfigs.keySet()) {
            dependents.put(partitionName, new LinkedHashSet<String>());
        }

        for (String partitionName : partitionConfigs.keySet()) {
            PartitionConfig partitionConfig = partitionConfigs.get(partitionName);

            for (String depend : partitionConfig.getDepends()) {
                Collection<String> list = dependents.get(depend);
                if (list == null) {
                    log.error("Partition not found: "+depend);
                    throw LDAP.createException(LDAP.OPERATIONS_ERROR);
                }
                list.add(partitionName);
            }
        }

        Collection<String> results = new LinkedHashSet<String>();
        results.add("DEFAULT");

        Collection<String> visited = new LinkedHashSet<String>();
        results.add("DEFAULT");

        for (String partitionName : partitionConfigs.keySet()) {
            PartitionConfig partitionConfig = partitionConfigs.get(partitionName);

            Collection<String> list = dependents.get(partitionName);
            if (!list.isEmpty()) continue;

            getLoadOrder(partitionConfig, results, visited);
        }

        return results;
    }

    public void getLoadOrder(PartitionConfig partitionConfig, Collection<String> results, Collection<String> visited) throws Exception {

        Logger log = LoggerFactory.getLogger(getClass());

        String partitionName = partitionConfig.getName();

        if (visited.contains(partitionName)) {
            if (!results.contains(partitionName)) {
                log.error("Circular dependency.");
                throw LDAP.createException(LDAP.OPERATIONS_ERROR);
            }
            return;
        }

        visited.add(partitionName);

        for (String depend : partitionConfig.getDepends()) {
            PartitionConfig pc = partitionConfigs.get(depend);
            getLoadOrder(pc, results, visited);
        }

        results.add(partitionName);
    }

    public Collection<PartitionConfig> getPartitionConfigs() {
        return partitionConfigs.values();
    }

    public PartitionConfig getPartitionConfig(String name) {
        return partitionConfigs.get(name);
    }
}
