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

import java.util.Collection;
import java.util.ArrayList;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class PartitionConfig implements PartitionConfigMBean, Cloneable {

    private boolean enabled = true;

    private String name;
    private String description;

    private String handlerName;
    private String engineName;

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

    public String getHandlerName() {
        return handlerName;
    }

    public void setHandlerName(String handlerName) {
        this.handlerName = handlerName;
    }

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
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

        if (!equals(handlerName, partitionConfig.handlerName)) return false;
        if (!equals(engineName, partitionConfig.engineName)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        PartitionConfig partitionConfig = (PartitionConfig)super.clone();

        partitionConfig.name = name;
        partitionConfig.enabled = enabled;
        partitionConfig.description = description;

        partitionConfig.handlerName = handlerName;
        partitionConfig.engineName = engineName;

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
}
