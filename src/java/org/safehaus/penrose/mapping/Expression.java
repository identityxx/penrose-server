/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.mapping;

/**
 * @author Endi S. Dewata
 */
public class Expression {

    private String foreach;
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

    public String toString() {
        return foreach == null ? script : "foreach "+foreach+": "+script;
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
}
