package org.safehaus.penrose.interpreter;

import bsh.Interpreter;

/**
 * @author Endi S. Dewata
 */
public class DefaultInterpreter extends org.safehaus.penrose.interpreter.Interpreter {

    public Interpreter interpreter;

    public DefaultInterpreter() {
        interpreter = new Interpreter();
    }

    public void set(String name, Object value) throws Exception {
        interpreter.set(name, value);
    }

    public Object get(String name) throws Exception {
        return interpreter.get(name);
    }

    public Object eval(String expression) throws Exception {
        try {
            if (expression == null) return null;
            return interpreter.eval(expression);

        } catch (Exception e) {
        	e.printStackTrace();
            return null;
        }
    }
}
