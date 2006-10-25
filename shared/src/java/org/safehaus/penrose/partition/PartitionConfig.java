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

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class PartitionConfig implements PartitionConfigMBean, Cloneable, Serializable {

	private static final long serialVersionUID = 8442096980176568175L;

	private String name;
    private String path;
    private boolean enabled = true;

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

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (path == null ? 0 : path.hashCode()) +
                (enabled ? 0 : 1);
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if((object == null) || (object.getClass() != getClass())) return false;

        PartitionConfig partitionConfig = (PartitionConfig)object;
        if (!equals(name, partitionConfig.name)) return false;
        if (!equals(path, partitionConfig.path)) return false;
        if (enabled != partitionConfig.enabled) return false;

        return true;
    }

    public void copy(PartitionConfig partitionConfig) {
        name = partitionConfig.name;
        path = partitionConfig.path;
        enabled = partitionConfig.enabled;
    }

    public Object clone() {
        PartitionConfig partitionConfig = new PartitionConfig();
        partitionConfig.copy(this);

        return partitionConfig;
    }
}
