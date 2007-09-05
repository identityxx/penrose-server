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

import org.safehaus.penrose.connection.ConnectionConfigs;
import org.safehaus.penrose.source.SourceConfigs;
import org.safehaus.penrose.directory.DirectoryConfigs;
import org.safehaus.penrose.module.ModuleConfigs;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.net.URL;
import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class PartitionConfig implements Serializable, PartitionConfigMBean, Cloneable {

    private boolean enabled = true;

    private String name;
    private String description;

    private Map<String,AdapterConfig>     adapterConfigs     = new LinkedHashMap<String,AdapterConfig>();
    private Map<String,EngineConfig>      engineConfigs      = new LinkedHashMap<String,EngineConfig>();
    private Map<String,HandlerConfig>     handlerConfigs     = new LinkedHashMap<String,HandlerConfig>();
    private Map<String,InterpreterConfig> interpreterConfigs = new LinkedHashMap<String,InterpreterConfig>();

    public Map<String,String> parameters = new LinkedHashMap<String,String>();

    private ConnectionConfigs connectionConfigs = new ConnectionConfigs();
    private SourceConfigs     sourceConfigs     = new SourceConfigs();
    private DirectoryConfigs  directoryConfigs  = new DirectoryConfigs();
    private ModuleConfigs     moduleConfigs     = new ModuleConfigs();

    private Collection<URL> classPaths = new ArrayList<URL>();

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

        if (!equals(adapterConfigs, partitionConfig.adapterConfigs)) return false;
        if (!equals(handlerConfigs, partitionConfig.handlerConfigs)) return false;
        if (!equals(engineConfigs, partitionConfig.engineConfigs)) return false;
        if (!equals(interpreterConfigs, partitionConfig.interpreterConfigs)) return false;

        if (!equals(parameters, partitionConfig.parameters)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        PartitionConfig partitionConfig = (PartitionConfig)super.clone();

        partitionConfig.name = name;
        partitionConfig.enabled = enabled;
        partitionConfig.description = description;

        partitionConfig.adapterConfigs = new LinkedHashMap<String,AdapterConfig>();
        for (AdapterConfig adapterConfig : adapterConfigs.values()) {
            partitionConfig.addAdapterConfig((AdapterConfig) adapterConfig.clone());
        }

        partitionConfig.engineConfigs = new LinkedHashMap<String,EngineConfig>();
        for (EngineConfig engineConfig : engineConfigs.values()) {
            partitionConfig.addEngineConfig((EngineConfig) engineConfig.clone());
        }

        partitionConfig.handlerConfigs = new LinkedHashMap<String,HandlerConfig>();
        for (HandlerConfig handlerConfig : handlerConfigs.values()) {
            partitionConfig.addHandlerConfig((HandlerConfig) handlerConfig.clone());
        }

        partitionConfig.interpreterConfigs = new LinkedHashMap<String,InterpreterConfig>();
        for (InterpreterConfig interpreterConfig : interpreterConfigs.values()) {
            partitionConfig.addInterpreterConfig((InterpreterConfig) interpreterConfig.clone());
        }

        partitionConfig.parameters = new LinkedHashMap<String,String>();
        partitionConfig.parameters.putAll(parameters);

        partitionConfig.connectionConfigs = (ConnectionConfigs) connectionConfigs.clone();
        partitionConfig.sourceConfigs = (SourceConfigs) sourceConfigs.clone();
        partitionConfig.directoryConfigs = (DirectoryConfigs) directoryConfigs.clone();
        partitionConfig.moduleConfigs = (ModuleConfigs) moduleConfigs.clone();

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

    public DirectoryConfigs getDirectoryConfigs() {
        return directoryConfigs;
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

    public void addEngineConfig(EngineConfig engineConfig) {
        engineConfigs.put(engineConfig.getName(), engineConfig);
    }

    public EngineConfig getEngineConfig(String name) {
        return engineConfigs.get(name);
    }

    public Collection<EngineConfig> getEngineConfigs() {
        return engineConfigs.values();
    }

    public Collection<String> getEngineNames() {
        return engineConfigs.keySet();
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

    public HandlerConfig getHandlerConfig(String name) {
        return handlerConfigs.get(name);
    }

    public Collection<HandlerConfig> getHandlerConfigs() {
        return handlerConfigs.values();
    }

    public Collection<String> getHandlerNames() {
        return handlerConfigs.keySet();
    }

    public void addHandlerConfig(HandlerConfig handlerConfig) {
        handlerConfigs.put(handlerConfig.getName(), handlerConfig);
    }

    public HandlerConfig removeHandlerConfig(String name) {
        return handlerConfigs.remove(name);
    }

}
