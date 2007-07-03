package org.safehaus.penrose.interpreter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SimpleInterpreter extends Interpreter {

    public Map variables = new HashMap();

    public SimpleInterpreter() {
    }

    public Collection parse(String script) throws Exception {
        return new ArrayList();
    }

    public Collection parseVariables(String script) throws Exception {
        return new ArrayList();
    }

    public void set(String name, Object value) throws Exception {
        variables.put(name, value);
    }

    public Object get(String name) throws Exception {
        return variables.get(name);
    }

    public Object eval(String script) throws Exception {
        return variables.get(script);
    }

    public void clear() throws Exception {
        variables.clear();
    }
}
