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

import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.connection.ConnectionConfigs;
import org.safehaus.penrose.directory.DirectoryConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.module.ModuleConfigs;
import org.safehaus.penrose.scheduler.SchedulerConfig;
import org.safehaus.penrose.source.SourceConfigs;

import java.io.Serializable;
import java.net.URL;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class PartitionConfig implements Serializable, PartitionConfigMBean, Cloneable {

    public final static String DEFAULT_PARTITION_CLASS = Partition.class.getName();
    protected boolean enabled = true;

    protected String name;
    protected String description;
    protected String partitionClass = DEFAULT_PARTITION_CLASS;

    protected Collection<String> depends = new ArrayList<String>();

    protected Map<String,AdapterConfig>     adapterConfigs     = new LinkedHashMap<String,AdapterConfig>();
    protected Map<String,InterpreterConfig> interpreterConfigs = new LinkedHashMap<String,InterpreterConfig>();

    protected Map<String,String> parameters = new LinkedHashMap<String,String>();

    protected ConnectionConfigs connectionConfigs = new ConnectionConfigs();
    protected SourceConfigs     sourceConfigs     = new SourceConfigs();
    protected DirectoryConfig   directoryConfig   = new DirectoryConfig();
    protected ModuleConfigs     moduleConfigs     = new ModuleConfigs();

    protected SchedulerConfig   schedulerConfig;

    protected Collection<URL>   classPaths = new ArrayList<URL>();

    public PartitionConfig() {
    }

    public PartitionConfig(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String,String> getParameters() {
        return parameters;
    }

    public Collection<String> getParameterNames() {
        return parameters.keySet();
    }

    public String getParameter(String name) {
        return parameters.get(name);
    }

    public void setParameters(Map<String,String> parameters) {
        if (parameters == this.parameters) return;
        this.parameters.clear();
        this.parameters.putAll(parameters);
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public String removeParameter(String name) {
        return parameters.remove(name);
    }

    public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        PartitionConfig partitionConfig = (PartitionConfig)object;
        if (enabled != partitionConfig.enabled) return false;

        if (!equals(name, partitionConfig.name)) return false;
        if (!equals(description, partitionConfig.description)) return false;
        if (!equals(partitionClass, partitionConfig.partitionClass)) return false;

        if (!equals(depends, partitionConfig.depends)) return false;

        if (!equals(adapterConfigs, partitionConfig.adapterConfigs)) return false;
        if (!equals(interpreterConfigs, partitionConfig.interpreterConfigs)) return false;

        if (!equals(parameters, partitionConfig.parameters)) return false;

        if (!equals(schedulerConfig, partitionConfig.schedulerConfig)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        PartitionConfig partitionConfig = (PartitionConfig)super.clone();

        partitionConfig.enabled = enabled;

        partitionConfig.name = name;
        partitionConfig.description = description;
        partitionConfig.partitionClass = partitionClass;

        partitionConfig.depends = new ArrayList<String>();
        partitionConfig.depends.addAll(depends);

        partitionConfig.adapterConfigs = new LinkedHashMap<String,AdapterConfig>();
        for (AdapterConfig adapterConfig : adapterConfigs.values()) {
            partitionConfig.addAdapterConfig((AdapterConfig) adapterConfig.clone());
        }

        partitionConfig.interpreterConfigs = new LinkedHashMap<String,InterpreterConfig>();
        for (InterpreterConfig interpreterConfig : interpreterConfigs.values()) {
            partitionConfig.addInterpreterConfig((InterpreterConfig) interpreterConfig.clone());
        }

        partitionConfig.parameters = new LinkedHashMap<String,String>();
        partitionConfig.parameters.putAll(parameters);

        partitionConfig.connectionConfigs = (ConnectionConfigs) connectionConfigs.clone();
        partitionConfig.sourceConfigs = (SourceConfigs) sourceConfigs.clone();
        partitionConfig.directoryConfig = (DirectoryConfig) directoryConfig.clone();
        partitionConfig.moduleConfigs = (ModuleConfigs) moduleConfigs.clone();

        partitionConfig.schedulerConfig = schedulerConfig == null ? null : (SchedulerConfig)schedulerConfig.clone();

        partitionConfig.classPaths = new ArrayList<URL>();
        partitionConfig.classPaths.addAll(classPaths);

        return partitionConfig;
    }

    public ConnectionConfigs getConnectionConfigs() {
        return connectionConfigs;
    }

    public SourceConfigs getSourceConfigs() {
        return sourceConfigs;
    }

    public DirectoryConfig getDirectoryConfig() {
        return directoryConfig;
    }

    public ModuleConfigs getModuleConfigs() {
        return moduleConfigs;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Collection<URL> getClassPaths() {
        return classPaths;
    }

    public void addClassPath(URL library) {
        classPaths.add(library);
    }

    public Collection<InterpreterConfig> getInterpreterConfigs() {
        return interpreterConfigs.values();
    }

    public void addInterpreterConfig(InterpreterConfig interpreterConfig) {
        interpreterConfigs.put(interpreterConfig.getName(), interpreterConfig);
    }

    public Collection<AdapterConfig> getAdapterConfigs() {
        return adapterConfigs.values();
    }

    public AdapterConfig getAdapterConfig(String name) {
        return adapterConfigs.get(name);
    }

    public void addAdapterConfig(AdapterConfig adapter) {
        adapterConfigs.put(adapter.getName(), adapter);
    }

    public Collection<String> getAdapterNames() {
        return adapterConfigs.keySet();
    }

    public SchedulerConfig getSchedulerConfig() {
        return schedulerConfig;
    }

    public void setSchedulerConfig(SchedulerConfig schedulerConfig) {
        this.schedulerConfig = schedulerConfig;
    }

    public String getPartitionClass() {
        return partitionClass;
    }

    public void setPartitionClass(String partitionClass) {
        this.partitionClass = partitionClass;
    }

    public Collection<String> getDepends() {
        return depends;
    }

    public void setDepends(Collection<String> depends) {
        this.depends.clear();
        if (depends == null) return;
        this.depends.addAll(depends);
    }

    public void setStringDepends(String depends) {
        for (StringTokenizer st = new StringTokenizer(depends, ", "); st.hasMoreTokens(); ) {
            String depend = st.nextToken();
            addDepend(depend);
        }
    }
    public void addDepend(String depend) {
        depends.add(depend);
    }

    public void removeDepend(String depend) {
        depends.remove(depend);
    }
}
