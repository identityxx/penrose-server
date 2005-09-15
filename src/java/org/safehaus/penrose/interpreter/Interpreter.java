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
package org.safehaus.penrose.interpreter;

import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.AttributeValues;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public abstract class Interpreter {

    public void set(Row row) throws Exception {
        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);
            set(name, value);
        }
    }

    public void set(AttributeValues av) throws Exception {
        for (Iterator i=av.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection list = av.get(name);
            Object value;
            if (list.size() == 1) {
                value = list.iterator().next();
            } else {
                value = list;
            }
            set(name, value);
        }
    }

    public abstract Collection parseVariables(String script) throws Exception;

    public abstract void set(String name, Object value) throws Exception;

    public abstract Object get(String name) throws Exception;

    public abstract Object eval(String expression) throws Exception;
}
