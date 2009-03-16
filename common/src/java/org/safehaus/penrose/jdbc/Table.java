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
package org.safehaus.penrose.jdbc;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Table implements Serializable, Comparable, Cloneable {

    public final static long serialVersionUID = 1L;

	private String name;
    private String type;

    private String catalog;
    private String schema;

    public Table(String name) {
        this(name, null, null, null);
    }

    public Table(String name, String catalog, String schema) {
        this(name, null, catalog, schema);
    }

    public Table(String name, String type, String catalog, String schema) {
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
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Table fieldConfig = (Table)object;
        if (!equals(name, fieldConfig.name)) return false;
        if (!equals(type, fieldConfig.type)) return false;

        return true;
    }

    public int compareTo(Object object) {
        if (object == null) return 0;
        if (!(object instanceof Table)) return 0;

        Table table = (Table)object;
        return name.compareTo(table.name);
    }

    public void copy(Table table) {
        name = table.name;
        type = table.type;
    }

    public Object clone() throws CloneNotSupportedException {
        Table table = (Table)super.clone();
        table.copy(this);
        return table;
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