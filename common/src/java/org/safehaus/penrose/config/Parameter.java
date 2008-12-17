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
package org.safehaus.penrose.config;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class Parameter implements Cloneable {

    public final static int TYPE_NORMAL   = 0;
    public final static int TYPE_REQUIRED = 1;
    public final static int TYPE_READ_ONLY = 2;
    public final static int TYPE_HIDDEN   = 3;
    public final static int TYPE_PASSWORD = 4;
    public final static int TYPE_TEMP     = 5;

    private String name;
    private String description;
    private String defaultValue;
    private int type = TYPE_NORMAL;

    private Collection<String> options = new ArrayList<String>();

    public Parameter() {
    }

    public Parameter(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public Parameter(String name, String description, String defaultValue) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
    }

    public Parameter(String name, String description, int type) {
        this.name = name;
        this.description = description;
        this.type = type;
    }

    public Parameter(String name, String description, String defaultValue, int type) {
        this.name = name;
        this.description = description;
        this.defaultValue = defaultValue;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getTypeAsString() {
        switch (type) {
            case TYPE_NORMAL:
                return "NORMAL";

            case TYPE_REQUIRED:
                return "REQUIRED";

            case TYPE_READ_ONLY:
                return "READ_ONLY";

            case TYPE_HIDDEN:
                return "HIDDEN";

            case TYPE_PASSWORD:
                return "PASSWORD";

            case TYPE_TEMP:
                return "TEMP";

            default:
                return null;
        }
    }

    public void setTypeAsString(String s) {
        if ("NORMAL".equals(s)) {
            type = TYPE_NORMAL;

        } else if ("REQUIRED".equals(s)) {
            type = TYPE_REQUIRED;

        } else if ("READ_ONLY".equals(s)) {
            type = TYPE_READ_ONLY;

        } else if ("HIDDEN".equals(s)) {
            type = TYPE_HIDDEN;

        } else if ("PASSWORD".equals(s)) {
            type = TYPE_PASSWORD;

        } else if ("TEMP".equals(s)) {
            type = TYPE_TEMP;
        }
    }

    public int hashCode() {
        return name.hashCode();
    }

    boolean compare(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Parameter p = (Parameter)object;
        if (!compare(name, p.name)) return false;
        if (!compare(description, p.description)) return false;
        if (!compare(defaultValue, p.defaultValue)) return false;
        if (type != p.type) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public String toString() {
        return "Parameter "+name+"/"+ description +" ("+type+"): "+defaultValue;
    }

    public Collection<String> getOptions() {
        return options;
    }

    public void setOptions(Collection<String> options) {
        this.options = options;
    }

    public void addOption(String option) {
        options.add(option);
    }

    public void removeOption(String option) {
        options.remove(option);
    }
}
