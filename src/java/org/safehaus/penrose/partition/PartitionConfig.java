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
package org.safehaus.penrose.partition;

/**
 * @author Endi S. Dewata
 */
public class PartitionConfig implements Cloneable {

    private String name;
    private String path;

    public PartitionConfig() {
    }

    public PartitionConfig(String name, String path) {
        this.name = name;
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (path == null ? 0 : path.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        PartitionConfig partitionConfig = (PartitionConfig)object;
        if (!equals(name, partitionConfig.name)) return false;
        if (!equals(path, partitionConfig.path)) return false;

        return true;
    }

    public void copy(PartitionConfig partitionConfig) {
        name = partitionConfig.name;
        path = partitionConfig.path;
    }

    public Object clone() {
        PartitionConfig partitionConfig = new PartitionConfig();
        partitionConfig.copy(this);

        return partitionConfig;
    }
}
