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
import org.safehaus.penrose.directory.EntryMapping;
import org.safehaus.penrose.directory.SourceMapping;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.LDAP;
import org.safehaus.penrose.source.SourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PartitionConfigs implements PartitionConfigsMBean {

    public Logger log = LoggerFactory.getLogger(getClass());
    public Logger errorLog = org.safehaus.penrose.log.Error.log;
    public boolean debug = log.isDebugEnabled();

    protected PartitionReader partitionReader = new PartitionReader();
    protected PartitionWriter partitionWriter = new PartitionWriter();

    private Map<String,PartitionConfig> partitionConfigs = new LinkedHashMap<String,PartitionConfig>();

    private File partitionsDir;

    public PartitionConfigs(File partitionsDir) {
        this.partitionsDir = partitionsDir;
    }

    public Collection<String> getAvailablePartitionNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        for (File partitionDir : partitionsDir.listFiles()) {
            list.add(partitionDir.getName());
        }
        return list;
    }

    public PartitionConfig load(String partitionName) throws Exception {
        File dir = new File(partitionsDir, partitionName);
        return load(dir);
    }

    public PartitionConfig load(File dir) throws Exception {
        if (debug) log.debug("Loading partition from "+dir+".");

        return partitionReader.read(dir);
    }

    public void store(File dir, PartitionConfig partitionConfig) throws Exception {

        if (debug) log.debug("Storing "+partitionConfig.getName()+" partition into "+dir+".");

        partitionWriter.write(dir, partitionConfig);
    }

    public PartitionConfig removePartitionConfig(String name) throws Exception {
        return partitionConfigs.remove(name);
    }

    public void clear() throws Exception {
        partitionConfigs.clear();
    }

    public PartitionConfig getPartitionConfig(SourceMapping sourceMapping) throws Exception {

        if (sourceMapping == null) return null;

        String sourceName = sourceMapping.getSourceName();
        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (partitionConfig.getSourceConfigs().getSourceConfig(sourceName) != null) return partitionConfig;
        }
        return null;
    }

    public PartitionConfig getPartitionConfig(SourceConfig sourceConfig) throws Exception {

        if (sourceConfig == null) return null;

        String connectionName = sourceConfig.getConnectionName();
        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (partitionConfig.getConnectionConfigs().getConnectionConfig(connectionName) != null) return partitionConfig;
        }
        return null;
    }

    public PartitionConfig getPartitionConfig(ConnectionConfig connectionConfig) throws Exception {

        if (connectionConfig == null) return null;

        String connectionName = connectionConfig.getName();
        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (partitionConfig.getConnectionConfigs().getConnectionConfig(connectionName) != null) return partitionConfig;
        }
        return null;
    }

    public PartitionConfig getPartitionConfig(EntryMapping entryMapping) throws Exception {

        if (entryMapping == null) return null;

        for (PartitionConfig partitionConfig : partitionConfigs.values()) {
            if (partitionConfig.getDirectoryConfig().contains(entryMapping)) {
                return partitionConfig;
            }
        }

        return null;
    }

    public PartitionConfig getPartitionConfig(DN dn) throws Exception {

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

    public PartitionReader getPartitionReader() {
        return partitionReader;
    }

    public void setPartitionReader(PartitionReader partitionReader) {
        this.partitionReader = partitionReader;
    }

    public File getPartitionsDir() {
        return partitionsDir;
    }

    public void setPartitionsDir(File partitionsDir) {
        this.partitionsDir = partitionsDir;
    }
}
