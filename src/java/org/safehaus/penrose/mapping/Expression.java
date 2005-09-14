/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

/**
 * @author Endi S. Dewata
 */
public class Expression implements Cloneable {

    private String foreach;
    private String var;
    private String script;

    public Expression() {
    }

    public Expression(String script) {
        this.script = script;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public String getForeach() {
        return foreach;
    }

    public void setForeach(String foreach) {
        this.foreach = foreach;
    }

    public String getConstant() {
        if (script == null || "".equals(script.trim())) return null;
        if (script.length() < 2) return null;
        if (!script.startsWith("\"")) return null;
        if (!script.endsWith("\"")) return null;
        String constant = script.substring(1, script.length()-1);
        if (constant.indexOf("\"") >= 0) return null;
        return constant;
    }

    public String getVar() {
        return var;
    }

    public void setVar(String var) {
        this.var = var;
    }

    public int hashCode() {
        return (foreach == null ? 0 : foreach.hashCode()) +
                (var == null ? 0 : var.hashCode()) +
                (script == null ? 0 : script.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        Expression expression = (Expression)object;
        if (!equals(foreach, expression.foreach)) return false;
        if (!equals(var, expression.var)) return false;
        if (!equals(script, expression.script)) return false;

        return true;
    }

    public Object clone() {
        Expression expression = new Expression();
        expression.foreach = foreach;
        expression.var     = var;
        expression.script  = script;
        return expression;
    }

    public String toString() {
        return foreach == null ? script : "foreach "+foreach+" ("+var+"): "+script;
    }

}
