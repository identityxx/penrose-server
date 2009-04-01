/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.filter;

import org.safehaus.penrose.ldap.Attributes;

import java.io.Serializable;
import java.util.Collection;

public abstract class Filter implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    ContainerFilter parent;

    public boolean eval(Attributes attributes) throws Exception {
        return true;
    }

    public String toString(Collection<Object> args) {
        return toString();
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public ContainerFilter getParent() {
        return parent;
    }

    public void setParent(ContainerFilter parent) {
        this.parent = parent;
    }

    public boolean matches(Filter filter) throws Exception {
        if (filter == null) return false;
        if (filter == this) return true;
        if (filter.getClass() != getClass()) return false;

        return true;
    }
}
