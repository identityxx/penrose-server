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
public class TableConfig implements Comparable, Cloneable {

	private String name;
    private String type;

    private String catalog;
    private String schema;

	public TableConfig() {
	}

    public TableConfig(String name) {
        this.name = name;
    }

    public TableConfig(String name, String type, String catalog, String schema) {
        this.name = name;
        this.type = type;
        this.catalog = catalog;
        this.schema = schema;
    }

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (type == null ? 0 : type.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (object == null) return false;
        if (!(object instanceof TableConfig)) return false;

        TableConfig fieldConfig = (TableConfig)object;
        if (!equals(name, fieldConfig.name)) return false;
        if (!equals(type, fieldConfig.type)) return false;

        return true;
    }

    public int compareTo(Object object) {
        if (object == null) return 0;
        if (!(object instanceof TableConfig)) return 0;

        TableConfig fd = (TableConfig)object;
        return name.compareTo(fd.name);
    }

    public void copy(TableConfig fieldConfig) {
        name = fieldConfig.name;
        type = fieldConfig.type;
    }

    public Object clone() {
        TableConfig fieldConfig = new TableConfig();
        fieldConfig.copy(this);
        return fieldConfig;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}