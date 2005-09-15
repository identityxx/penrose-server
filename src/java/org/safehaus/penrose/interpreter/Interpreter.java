/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
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
