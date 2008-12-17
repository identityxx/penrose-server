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

import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.PenroseConfig;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class PartitionFactory {

    private File partitionsDir;
    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public PartitionFactory() {
    }

    public ClassLoader createClassLoader(PartitionConfig partitionConfig) throws Exception {
        Collection<URL> classPaths = partitionConfig.getClassPaths();
        return new URLClassLoader(classPaths.toArray(new URL[classPaths.size()]), getClass().getClassLoader());
    }

    public Partition createPartition(PartitionConfig partitionConfig) throws Exception {

        ClassLoader classLoader = createClassLoader(partitionConfig);

        PartitionContext partitionContext = new PartitionContext();

        if (partitionConfig instanceof DefaultPartitionConfig) {
            partitionContext.setPath(null);
        } else {
            partitionContext.setPath(partitionsDir == null ? null : new File(partitionsDir, partitionConfig.getName()));
        }

        partitionContext.setPenroseConfig(penroseConfig);
        partitionContext.setPenroseContext(penroseContext);

        partitionContext.setPartitionManager(penroseContext.getPartitionManager());
        partitionContext.setClassLoader(classLoader);

        Partition partition;

        String className = partitionConfig.getPartitionClass();
        if (className == null) {
            partition = new Partition();

        } else {
            Class clazz = classLoader.loadClass(className);
            partition = (Partition)clazz.newInstance();
        }

        partition.init(partitionConfig, partitionContext);

        return partition;
    }

    public File getPartitionsDir() {
        return partitionsDir;
    }

    public void setPartitionsDir(File partitionsDir) {
        this.partitionsDir = partitionsDir;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }
}
