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
package org.safehaus.penrose.schema;

import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class SchemaConfig implements Cloneable, SchemaConfigMBean {

    private String name;
    private String path;

    public SchemaConfig() {
    }

    public SchemaConfig(String path) {
        setPath(path);
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

        File file = new File(path);
        String filename = file.getName();
        int i = filename.indexOf(".");

        this.name = filename.substring(0, i);
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

        SchemaConfig schemaConfig = (SchemaConfig)object;
        if (!equals(name, schemaConfig.name)) return false;
        if (!equals(path, schemaConfig.path)) return false;

        return true;
    }

    public void copy(SchemaConfig schemaConfig) {
        name = schemaConfig.name;
        path = schemaConfig.path;
    }

    public Object clone() throws CloneNotSupportedException {
        SchemaConfig schemaConfig = (SchemaConfig)super.clone();
        schemaConfig.copy(this);
        return schemaConfig;
    }
}
