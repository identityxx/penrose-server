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
package org.safehaus.penrose.schema.attributeSyntax;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class AttributeSyntax implements Serializable, Cloneable {

    public String oid;
    public String description;
    public boolean humanReadable;

    public AttributeSyntax(String oid, String description, boolean humanReadable) {
        this.oid = oid;
        this.description = description;
        this.humanReadable = humanReadable;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isHumanReadable() {
        return humanReadable;
    }

    public void setHumanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
    }

    public int hashCode() {
        return oid == null ? 0 : oid.hashCode();
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

        AttributeSyntax attributeSyntax = (AttributeSyntax)object;
        if (!equals(oid, attributeSyntax.oid)) return false;
        if (!equals(description, attributeSyntax.description)) return false;
        if (humanReadable != attributeSyntax.humanReadable) return false;

        return true;
    }

    public void copy(AttributeSyntax attributeSyntax) throws CloneNotSupportedException {
        oid           = attributeSyntax.oid;
        description   = attributeSyntax.description;
        humanReadable = attributeSyntax.humanReadable;
    }

    public Object clone() throws CloneNotSupportedException {
        AttributeSyntax attributeSyntax = (AttributeSyntax)super.clone();
        attributeSyntax.copy(this);
        return attributeSyntax;
    }
}
