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
import java.net.URL;
import java.net.URLClassLoader;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class Partition implements PartitionMBean, Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PartitionConfig partitionConfig;

    private ClassLoader classLoader;

    public Partition(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;

        Collection<URL> classPaths = partitionConfig.getClassPaths();
        classLoader = new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]));
    }

    public String getName() {
        return partitionConfig.getName();
    }

    public void setName(String name) {
        partitionConfig.setName(name);
    }

    public String getDescription() {
        return partitionConfig.getDescription();
    }

    public void setDescription(String description) {
        partitionConfig.setDescription(description);
    }

    public boolean isEnabled() {
        return partitionConfig.isEnabled();
    }

    public String getHandlerName() {
        return partitionConfig.getHandlerName();
    }

    public String getEngineName() {
        return partitionConfig.getEngineName();
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

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public Object clone() throws CloneNotSupportedException {
        Partition partition = (Partition)super.clone();

        partition.partitionConfig = (PartitionConfig)partitionConfig.clone();

        partition.classLoader = classLoader;

        return partition;
    }
}
