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
package org.safehaus.penrose.sql;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class SQLCompareExprRight {

    Collection<Object> children = new ArrayList<Object>();

    public void addChild(Object object) {
        children.add(object);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (Object child : children) {
            if (first) {
                first = false;
            } else {
                sb.append(" ");
            }
            sb.append(child);
        }
        
        return sb.toString();
    }
}
