package org.safehaus.penrose.interpreter;

/**
 * @author Endi S. Dewata
 */
public abstract class Interpreter {
    
    public abstract void set(String name, Object value) throws Exception;

    public abstract Object get(String name) throws Exception;

    public abstract Object eval(String expression) throws Exception;
}
