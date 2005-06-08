package org.safehaus.penrose.interpreter;

import org.safehaus.penrose.mapping.Row;

import java.util.Iterator;

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

    public abstract void set(String name, Object value) throws Exception;

    public abstract Object get(String name) throws Exception;

    public abstract Object eval(String expression) throws Exception;
}
