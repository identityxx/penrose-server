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

import java.util.*;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.safehaus.penrose.module.Modules;
import org.safehaus.penrose.mapping.Mappings;
import org.safehaus.penrose.source.Sources;
import org.safehaus.penrose.connection.Connections;
import org.safehaus.penrose.connection.ConnectionConfig;

/**
 * @author Endi S. Dewata
 */
public class Partition implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PartitionConfig partitionConfig;

    private Connections connections = new Connections();
    private Sources sources = new Sources();
    private Mappings mappings = new Mappings();
    private Modules modules = new Modules();

    private Collection<String> libraries = new ArrayList<String>();
    private ClassLoader classLoader;

    public Partition(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    public String getName() {
        return partitionConfig.getName();
    }

    public void setName(String name) {
        partitionConfig.setName(name);
    }

    public String getHandlerName() {
        return partitionConfig.getHandlerName();
    }

    public String getEngineName() {
        return partitionConfig.getEngineName();
    }

    public Modules getModules() {
        return modules;
    }

    public Connections getConnections() {
        return connections;
    }

    public Collection<ConnectionConfig> getConnectionConfigs() {
        return connections.getConnectionConfigs();
    }

    public PartitionConfig getPartitionConfig() {
        return partitionConfig;
    }

    public void setPartitionConfig(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    public String toString() {
        return partitionConfig.getName();
    }

    public Sources getSources() {
        return sources;
    }

    public void setSources(Sources sources) {
        this.sources = sources;
    }

    public Collection<String> getLibraries() {
        return libraries;
    }

    public void addLibrary(String library) {
        libraries.add(library);
    }

    public Mappings getMappings() {
        return mappings;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public Object clone() throws CloneNotSupportedException {
        Partition partition = (Partition)super.clone();

        partition.partitionConfig = (PartitionConfig)partitionConfig.clone();

        partition.connections = (Connections)connections.clone();
        partition.sources = (Sources)sources.clone();
        partition.mappings = (Mappings)mappings.clone();
        partition.modules = (Modules)modules.clone();

        partition.libraries = new ArrayList<String>();

        return partition;
    }
}
